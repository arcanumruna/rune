// ShardAgentTest.cpp — the shard pass drives its OWN driver + conductor; the
// loop-duration alarm uses only the injected clock (no wall time).
#include <rune/node/ShardAgent.h>

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

TEST(ShardAgent, SlowPassAlarmCountsOnlyFromInjectedClock) {
    Shard s;
    ShardAgent a(s.worker, *s.driver, *s.client);
    a.doWork(1'000'000);
    a.doWork(1'050'000);                        // 50 µs gap
    EXPECT_EQ(a.slowPassesForTest(), 0u);
    a.doWork(1'050'000 + ShardAgent::kSlowPassNs + 1);
    EXPECT_EQ(a.slowPassesForTest(), 1u);
}

TEST(ShardAgent, PassDrivesOwnDriverAndConductor) {
    Shard s;
    ShardAgent a(s.worker, *s.driver, *s.client);
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
