// EmbeddedDriverTest.cpp — one embedded driver is driven by exactly one thread
// (spec §3.6); invoker mode has no driver thread; stale/active dir handling.
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

using namespace rune::node;
namespace fs = std::filesystem;

namespace {
struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_edrv_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
EmbeddedDriverConfig cfgAt(const fs::path& p) {
    EmbeddedDriverConfig c;
    c.dir = p.string();
    c.termBufferBytes = 64 * 1024;
    return c;
}
std::shared_ptr<aeron::Aeron> connect(const fs::path& p, bool invoker) {
    aeron::Context ctx;
    ctx.aeronDir(p.string());
    ctx.useConductorAgentInvoker(invoker);
    return aeron::Aeron::connect(ctx);
}
}

TEST(EmbeddedDriverRegression, ThreadedStartNeverPumpsFromCaller) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Threaded);
    EXPECT_EQ(drv.callerPumpsWhileThreadRunning(), 0u)
        << "startup must not drive the driver from the caller while its thread runs";
    auto a = connect(d.path, /*invoker=*/false);
    ASSERT_TRUE(a) << "cnc.dat exists after init — connect needs no caller pumping";
    EXPECT_EQ(drv.doWork(), 0) << "a threaded driver may never be pumped by a second thread";
    EXPECT_EQ(drv.foreignPumpAttempts(), 1u);
    a.reset();
}

TEST(EmbeddedDriver, InvokerModeRoundTripOnOneThreadOnly) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    const int64_t sreg = a->addSubscription("aeron:ipc", 9001);
    const int64_t preg = a->addExclusivePublication("aeron:ipc", 9001);
    std::shared_ptr<aeron::Subscription> sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    for (int i = 0; i < 100'000 && !(sub && pub && pub->isConnected()); ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        if (!sub) sub = a->findSubscription(sreg);
        if (!pub) pub = a->findExclusivePublication(preg);
    }
    ASSERT_TRUE(sub && pub && pub->isConnected()) << "invoker driver must progress when pumped";
    uint8_t msg[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    aeron::concurrent::AtomicBuffer ab(msg, sizeof(msg));
    ASSERT_GT(pub->offer(ab, 0, sizeof(msg)), 0);
    int got = 0;
    for (int i = 0; i < 100'000 && got == 0; ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        got += sub->poll([](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                            aeron::util::index_t, aeron::Header&) {}, 10);
    }
    EXPECT_EQ(got, 1);
    EXPECT_EQ(drv.foreignPumpAttempts(), 0u);
    sub.reset(); pub.reset(); a.reset();
}

TEST(EmbeddedDriverRegression, InvokerForeignThreadPumpIsRefused) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    int r = -1;
    std::thread t([&] { r = drv.doWork(); });
    t.join();
    EXPECT_EQ(r, 0);
    EXPECT_EQ(drv.foreignPumpAttempts(), 1u);
}

TEST(EmbeddedDriverRegression, StaleInactiveDirIsReclaimed) {
    TmpDir d;
    {
        auto c = cfgAt(d.path);
        c.deleteDirOnShutdown = false;
        EmbeddedDriver a(c, DriverMode::Invoker);
        a.doWork();
    }   // closed: conductor nulls its heartbeat (aeron_driver_conductor.c:3103), cnc.dat left behind
    ASSERT_TRUE(fs::exists(d.path / "cnc.dat"));
    EXPECT_NO_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker));
}

TEST(EmbeddedDriverRegression, ActiveDirIsRefused) {
    TmpDir d;
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    a.doWork();
    EXPECT_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker), std::runtime_error);
}

TEST(EmbeddedDriverRegression, AeronSpaceCheck) {
    EXPECT_FALSE(checkAeronSpace(1ULL << 30, 2, 4 * 1024 * 1024).has_value());
    auto e = checkAeronSpace(8ULL * 1024 * 1024, 2, 4 * 1024 * 1024);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_AERON_TERM_BUFFER_BYTES"), std::string::npos);
}

// Fix round 1: close() drains, which pumps the driver.  In Invoker mode a close
// from a thread that is not the bound pumper would make it a second pumper, so
// it must be refused and leave the driver untouched.
TEST(EmbeddedDriverRegression, InvokerForeignThreadCloseIsRefused) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    std::thread t([&] { drv.close(); });
    t.join();
    EXPECT_EQ(drv.foreignCloseAttempts(), 1u);
    ASSERT_TRUE(fs::exists(d.path / "cnc.dat")) << "a refused close must not tear the driver down";
    EXPECT_GE(drv.doWork(), 0) << "the owner still pumps after a refused foreign close";
    EXPECT_EQ(drv.foreignPumpAttempts(), 0u);
    drv.close();                                   // owner close succeeds
    EXPECT_EQ(drv.foreignCloseAttempts(), 1u);
}

// The documented hand-off: the worker pumps, is joined, then the closer binds
// itself as the pumper and closes.  This is not a foreign close.
TEST(EmbeddedDriverRegression, InvokerHandOffThenCloseByNewOwner) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    std::thread worker([&] {
        drv.bindPumpThread();
        for (int i = 0; i < 4; ++i) drv.doWork();
    });
    worker.join();
    drv.bindPumpThread();
    drv.close();
    EXPECT_EQ(drv.foreignCloseAttempts(), 0u);
    EXPECT_EQ(drv.foreignPumpAttempts(), 0u);
}

// Task 9 carry (T7 minor): an Invoker driver destroyed off its pump thread with
// no hand-off cannot close — it would leak the driver + dir silently.  The leak
// is logged critical and debug builds abort, so a wrong-thread shard teardown
// can never go unnoticed.
TEST(EmbeddedDriverRegression, InvokerDestroyedOffPumpThreadWithoutHandOffAbortsInDebug) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    TmpDir d;
    EXPECT_DEBUG_DEATH({
        auto drv = std::make_unique<EmbeddedDriver>(cfgAt(d.path), DriverMode::Invoker);
        std::thread t([&] { drv.reset(); });   // no join-then-bindPumpThread hand-off
        t.join();
    }, "");
}

// The same destruction WITH the hand-off closes cleanly (dir released).
TEST(EmbeddedDriverRegression, InvokerDestroyedAfterHandOffClosesCleanly) {
    TmpDir d;
    auto drv = std::make_unique<EmbeddedDriver>(cfgAt(d.path), DriverMode::Invoker);
    std::thread t([&] { drv->bindPumpThread(); drv.reset(); });
    t.join();
    EXPECT_FALSE(fs::exists(d.path / "cnc.dat")) << "hand-off close must release the driver dir";
}
