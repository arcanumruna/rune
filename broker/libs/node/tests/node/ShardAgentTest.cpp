// ShardAgentTest.cpp — the shard pass drives its OWN driver + conductor; the
// loop-duration alarm measures the pass's WORK duration from the injected clock
// only (no wall time; idle parks between passes are never counted).
#include <rune/node/ShardAgent.h>
#include <rune/node/WorkerPool.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <unistd.h>

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
    EXPECT_THROW(pool.startShardRunners(none, clk, /*busySpin=*/false, 200), std::logic_error);
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
