// ShardAgentTest.cpp — the shard pass drives its OWN driver + conductor; the
// loop-duration alarm measures the pass's WORK duration from the injected clock
// only (no wall time; idle parks between passes are never counted).
#include <rune/node/ShardAgent.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/ParkUntilWorkIdle.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <thread>
#include <unistd.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif

using namespace rune::node;
namespace fs = std::filesystem;

namespace {
struct Shard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    Worker worker{0, 1 << 16, 1 << 16, 16, 16};
    Shard() {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() / ("rune_shagent_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        worker.setShardMode(true);
    }
    ~Shard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
};
}

// Injected clock for the pass-end sample: the test sets what "now" is when the
// pass finishes its work.
struct TestClock final : rune::consensus::NanoClock {
    int64_t endNs = 0;
    int64_t nowNs() const override { return endNs; }
};
using TestShardAgent = BasicShardAgent<TestClock>;

TEST(ShardAgentRegression, SlowPassAlarmMeasuresPassDurationNotGaps) {
    Shard s;
    TestClock clk;
    TestShardAgent a(s.worker, *s.driver, *s.client, clk);
    clk.endNs = 1'000'000 + 50'000;
    a.doWork(1'000'000);                                   // 50 µs pass
    EXPECT_EQ(a.slowPassesForTest(), 0u);
    clk.endNs = 90'000'000 + 10'000;
    a.doWork(90'000'000);                                  // 89 ms idle park before it, 10 µs pass
    EXPECT_EQ(a.slowPassesForTest(), 0u) << "an idle gap between passes is not a slow pass";
    clk.endNs = 100'000'000 + TestShardAgent::kSlowPassNs + 1;
    a.doWork(100'000'000);                                 // the pass itself exceeds the budget
    EXPECT_EQ(a.slowPassesForTest(), 1u);
}

TEST(ShardAgentRegression, StartShardRunnersRefusesAgentCountMismatch) {
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    std::vector<std::unique_ptr<ShardAgent>> none;
    rune::consensus::MonotonicNanoClock clk;
    EXPECT_THROW(pool.startShardRunners(none, clk, NodeConfig{}), std::logic_error);
}

TEST(ShardAgent, PassDrivesOwnDriverAndConductor) {
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(s.worker, *s.driver, *s.client, clk);
    const int64_t reg = s.client->addSubscription("aeron:ipc", 9100);
    std::shared_ptr<aeron::Subscription> sub;
    int64_t now = 1'000;
    for (int i = 0; i < 100'000 && !sub; ++i) {
        a.doWork(now += 1'000);                  // the pass is the ONLY thing driving anything
        sub = s.client->findSubscription(reg);
    }
    EXPECT_TRUE(sub) << "subscription resolves only if the shard pass pumps driver + conductor";
    EXPECT_EQ(s.driver->foreignPumpAttempts(), 0u);
}

// A1: one pass drains its own driver while it has work (bounded), so a pass moves
// more than ~1 datagram per stream; an idle driver costs exactly one duty cycle.
TEST(ShardAgentRegression, PassDrainsDriverWhileItHasWorkRegression) {
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(s.worker, *s.driver, *s.client, clk);
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(39000 + (::getpid() % 500));
    const int64_t sreg = s.client->addSubscription(ch, 9200);
    const int64_t preg = s.client->addExclusivePublication(ch, 9200);
    std::shared_ptr<aeron::Subscription> sub; std::shared_ptr<aeron::ExclusivePublication> pub;
    int64_t now = 1'000;
    for (int i = 0; i < 100'000 && !(sub && pub && pub->isConnected()); ++i) {
        a.doWork(now += 1'000);
        if (!sub) sub = s.client->findSubscription(sreg);
        if (!pub) pub = s.client->findExclusivePublication(preg);
    }
    ASSERT_TRUE(sub && pub && pub->isConnected());
    for (int i = 0; i < 200; ++i) a.doWork(now += 1'000);           // settle: setup/status traffic done
    const uint64_t idle0 = a.driverDutyCyclesForTest();
    a.doWork(now += 1'000);
    EXPECT_EQ(a.driverDutyCyclesForTest() - idle0, 1u) << "an idle driver costs exactly one duty cycle";
    std::vector<uint8_t> buf(1024, 7);
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    int offered = 0;
    for (int i = 0; i < 48; ++i) offered += pub->offer(ab, 0, static_cast<aeron::util::index_t>(buf.size())) > 0 ? 1 : 0;
    ASSERT_GT(offered, 8);
    const uint64_t busy0 = a.driverDutyCyclesForTest();
    a.doWork(now += 1'000);
    EXPECT_GT(a.driverDutyCyclesForTest() - busy0, 1u) << "with data queued the pass keeps driving the driver";
}

// ---------------------------------------------------------------------------
// A7 park-until-work (findings §4.12): the shard runner's idle strategy.
// ---------------------------------------------------------------------------
TEST(ShardAgentWake, AgentExposesItsInvokerDriversWakeFds) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only";
#else
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(s.worker, *s.driver, *s.client, clk);
    // Review M1: the worker's io_uring completion fd joins the driver's two poller fds (when io_uring is the
    // io backend; the portable backend has no fd).
    const int uring = s.worker.ioCompletionFd() >= 0 ? 1 : 0;
    int fds[ParkUntilWorkIdle::kMaxWakeFds];
    EXPECT_EQ(a.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds), 2 + uring) << "receiver + sender pollers (+ io_uring)";
    if (uring) EXPECT_EQ(fds[2], s.worker.ioCompletionFd());
    // Fused placement with a SEPARATE runtime driver: the worker parks on both drivers' sockets.
    Shard rt;
    a.setFusedRuntime(nullptr, rt.driver.get(), rt.client.get());
    EXPECT_EQ(a.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds), 4 + uring);
    // A4: the runtime riding this shard's own driver adds nothing.
    a.setFusedRuntime(nullptr, s.driver.get(), s.client.get());
    EXPECT_EQ(a.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds), 2 + uring);
#endif
}

// Review C1: Aeron's stock poller (epoll-first off) can leave an EPOLLERR socket ready, so a driver that does
// not poll epoll-first hands out no wake fds — the runner keeps the legacy timed backoff.
TEST(ShardAgentWakeRegression, EpollFirstOffExposesNoWakeFdsRegression) {
    fs::path dir = fs::temp_directory_path() / ("rune_shagent_off_" + std::to_string(::getpid()));
    EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024; c.epollPoll = false;
    auto drv = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
    aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
    auto client = aeron::Aeron::connect(ctx);
    Worker w{0, 1 << 16, 1 << 16, 16, 16};
    w.setShardMode(true);
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(w, *drv, *client, clk);
    int fds[ParkUntilWorkIdle::kMaxWakeFds];
    EXPECT_EQ(a.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds), 0)
        << "no driver fds -> no wake fds at all (not even io_uring): the legacy backoff runs";
    client.reset(); drv.reset();
    std::error_code ec; fs::remove_all(dir, ec);
}

// Review D1: the shard runner thread sets its timer slack to 1 µs at its first pass (the default 50 µs slack
// rounds every 1 µs .. park up to >= 51 µs).
TEST(ShardAgent, FirstPassLeavesTimerSlackAloneRegression) {
#if !defined(__linux__)
    GTEST_SKIP() << "PR_SET_TIMERSLACK is Linux-only";
#else
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(s.worker, *s.driver, *s.client, clk);
    long before = -1, after = -1;
    std::thread runner([&] {
        // Threads inherit the creator's slack (an earlier test's pass on the main thread set it): start from
        // the kernel default (50 µs) explicitly.
        (void)::prctl(PR_SET_TIMERSLACK, 50'000UL, 0, 0, 0);
        before = ::prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
        a.doWork(clk.nowNs());
        after = ::prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
    });
    runner.join();
    EXPECT_EQ(before, 50'000L);
    // A7 re-review MEDIUM-1: the pass itself leaves the slack alone — only the readiness-fd park sets it, so the
    // legacy backoff fallback (RUNE_IDLE_WAKE=0 / RUNE_AERON_EPOLL_POLL=0 / no fds) keeps the kernel default.
    EXPECT_EQ(after, 50'000L);
#endif
}



namespace {
struct FakeFds {
    int p[2]{-1, -1};
    FakeFds() { EXPECT_EQ(::pipe(p), 0); }
    ~FakeFds() { ::close(p[0]); ::close(p[1]); }
};
}

TEST(ShardIdle, DefaultWithWakeFdsIsParkUntilWorkAfterTheAeronSpinYieldPhase) {
    FakeFds f;
    NodeConfig c;   // defaults: wake on, spins/yields auto, maxPark 200 us
    auto idle = makeShardIdle(c, f.p, 1);
    auto* p = dynamic_cast<ParkUntilWorkIdle*>(idle.get());
    ASSERT_NE(p, nullptr) << "default shard idle = park-until-work";
    EXPECT_EQ(p->wakeFdCount(), 1);
    EXPECT_EQ(p->shape(), (IdleShape{10, 20, 1'000, 200'000})) << "review M1: Aeron spin/yield before the first park";
}

// A9: with a doorbell the runner parks on it too, with the doorbell shape (Aeron spin / yield, 1 ms park);
// RUNE_IDLE_DOORBELL=0 keeps A7's shape exactly (the A/B off arm).
TEST(ShardIdle, DoorbellGivesTheDoorbellShapeAndOffKeepsA7) {
    FakeFds f;
    rune::node::detail::RingDoorbell bell;
    NodeConfig c;
    auto idle = makeShardIdle(c, f.p, 1, &bell);
    auto* p = dynamic_cast<ParkUntilWorkIdle*>(idle.get());
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->doorbell(), &bell);
    EXPECT_EQ(p->wakeFdCount(), 2) << "driver fd + the doorbell fd";
    EXPECT_EQ(p->shape(), (IdleShape{10, 20, 1'000'000, 1'000'000}));
    EXPECT_NE(describeShardIdle(*idle).find("doorbell="), std::string::npos) << describeShardIdle(*idle);

    c.idleDoorbell = false;
    auto off = makeShardIdle(c, f.p, 1, &bell);
    auto* q = dynamic_cast<ParkUntilWorkIdle*>(off.get());
    ASSERT_NE(q, nullptr);
    EXPECT_EQ(q->doorbell(), nullptr);
    EXPECT_EQ(q->shape(), (IdleShape{10, 20, 1'000, 200'000}));
}

TEST(ShardAgent, AgentExposesItsWorkersDoorbell) {
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent a(s.worker, *s.driver, *s.client, clk);
    EXPECT_EQ(&a.doorbell(), &s.worker.doorbell());
    EXPECT_TRUE(a.doorbell().valid());
    EXPECT_EQ(a.doorbell().watchCount(), 0) << "review M-1: nothing wired until enabled";
    a.enableDoorbell(true);
    EXPECT_TRUE(a.doorbellEnabled());
    EXPECT_GE(a.doorbell().watchCount(), 2) << "the worker's op + session rings";
}

TEST(ShardIdle, WakeOffIsTheLegacyAeronBackoff) {
    FakeFds f;
    NodeConfig c;
    c.idleWake = false;
    auto idle = makeShardIdle(c, f.p, 1);
    EXPECT_NE(dynamic_cast<rune::consensus::BackoffIdleStrategy*>(idle.get()), nullptr)
        << "RUNE_IDLE_WAKE=0 is byte-identical to the pre-A7 shard idle (the A/B off arm)";
}

TEST(ShardIdle, NoWakeFdsFallsBackToLegacyBackoff) {
    NodeConfig c;   // wake on, but the platform gave no fds (non-Linux)
    auto idle = makeShardIdle(c, nullptr, 0);
    EXPECT_NE(dynamic_cast<rune::consensus::BackoffIdleStrategy*>(idle.get()), nullptr);
}

TEST(ShardIdle, ExplicitShapeWithoutFdsIsATimedParkUntilWork) {
    NodeConfig c;
    c.idleWake = false;
    c.idleSpins = 1; c.idleYields = 2; c.idleMaxParkUs = 50;
    auto idle = makeShardIdle(c, nullptr, 0);
    auto* p = dynamic_cast<ParkUntilWorkIdle*>(idle.get());
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->wakeFdCount(), 0);
    EXPECT_EQ(p->shape(), (IdleShape{1, 2, 1'000, 50'000}));
}

TEST(ShardIdle, BusySpinOnlyOnExplicitOptIn) {
    FakeFds f;
    NodeConfig c;
    c.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
    auto idle = makeShardIdle(c, f.p, 1);
    EXPECT_NE(dynamic_cast<rune::consensus::BusySpinIdleStrategy*>(idle.get()), nullptr);
}

// A7 review L3: every strategy makeShardIdle produces is dispatched by the shard runner's ShardIdle without a
// virtual call (the runner itself is BasicAgentRunner<ShardAgent, ShardIdle, MonotonicNanoClock>, all final).
TEST(ShardIdle, EveryShardStrategyIsDispatchedWithoutAVirtualCall) {
    FakeFds f;
    NodeConfig park;                                   // park-until-work
    NodeConfig legacy; legacy.idleWake = false;        // legacy Aeron backoff
    NodeConfig timed;  timed.idleWake = false; timed.idleSpins = 1; timed.idleYields = 2;   // timed park
    NodeConfig spin;   spin.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
    for (const NodeConfig* c : {&park, &legacy, &timed, &spin}) {
        ShardIdle idle(makeShardIdle(*c, f.p, 1));
        EXPECT_TRUE(idle.devirtualizedForTest()) << describeShardIdle(idle.strategy());
        idle.idle(1);   // work: a reset, never a park
        idle.reset();
    }
    ShardIdle none(makeShardIdle(park, nullptr, 0));   // no fds: the legacy backoff
    EXPECT_TRUE(none.devirtualizedForTest());
}

#if defined(__linux__)
TEST(ShardAgentTimerSlack, OnlyTheReadinessParkLowersTimerSlackRegression) {
    FakeFds f;
    long legacy = -1, withFds = -1;
    std::thread t1([&] {
        (void)::prctl(PR_SET_TIMERSLACK, 50'000UL, 0, 0, 0);
        ParkUntilWorkIdle timed(IdleShape{0, 0, 1'000, 1'000}, nullptr, 0);   // no fds = fallback shape
        timed.idle(0);
        legacy = ::prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
    });
    t1.join();
    std::thread t2([&] {
        (void)::prctl(PR_SET_TIMERSLACK, 50'000UL, 0, 0, 0);
        const int fds[1] = {f.p[0]};
        ParkUntilWorkIdle wake(IdleShape{0, 0, 1'000, 1'000}, fds, 1);
        wake.idle(0);
        withFds = ::prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
    });
    t2.join();
    EXPECT_EQ(legacy, 50'000L);
    EXPECT_EQ(withFds, static_cast<long>(kShardTimerSlackNs));
}
#endif
