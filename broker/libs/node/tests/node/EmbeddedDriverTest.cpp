// EmbeddedDriverTest.cpp — one embedded driver is driven by exactly one thread
// (spec §3.6); invoker mode has no driver thread; stale/active dir handling.
#include <sys/stat.h>
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
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

// Non-sparse term buffers are fully allocated (and zero-filled) when the
// publication is created — no first-write page faults on the hot path, and a full
// tmpfs fails the create instead of SIGBUS-ing a running sender.
namespace {
uint64_t allocatedLogBufferBytes(const fs::path& dir, bool sparse) {
    TmpDir d;
    EmbeddedDriverConfig c = cfgAt(d.path);
    c.termBufferBytes = 1024 * 1024;
    c.termBufferSparse = sparse;
    EmbeddedDriver drv(c, DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    const int64_t preg = a->addExclusivePublication("aeron:ipc", 9301);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    for (int i = 0; i < 100'000 && !pub; ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        pub = a->findExclusivePublication(preg);
    }
    uint64_t bytes = 0;
    for (const auto& e : fs::recursive_directory_iterator(d.path))
        if (e.path().extension() == ".logbuffer") {
            struct stat st{};
            if (::stat(e.path().c_str(), &st) == 0) bytes += static_cast<uint64_t>(st.st_blocks) * 512u;
        }
    (void)dir;
    pub.reset(); a.reset();
    return bytes;
}
}

TEST(EmbeddedDriverRegression, NonSparseTermsAreFullyAllocatedAtCreationRegression) {
    const uint64_t full = 3u * 1024u * 1024u;                        // three 1 MiB terms
    EXPECT_GE(allocatedLogBufferBytes({}, /*sparse=*/false), full)
        << "non-sparse terms must be allocated up front";
    EXPECT_LT(allocatedLogBufferBytes({}, /*sparse=*/true), full / 2)
        << "sparse terms stay unallocated until written (the default is unchanged)";
}

// ---------------------------------------------------------------------------
// A7 park-until-work (findings §4.12): an invoker driver exposes its transport pollers' epoll fds; a
// shard worker parked on them wakes on the next datagram instead of sleeping out its park period.
// Linux only (Aeron's pollers use epoll there; other platforms expose none -> timed park).
// ---------------------------------------------------------------------------
#include <rune/node/ParkUntilWorkIdle.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace {
uint16_t freeUdpPortForWake() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}
void sendJunkDatagram(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons(port);
    const char junk[8] = {1, 2, 3, 4, 5, 6, 7, 8};   // not an Aeron frame: the receiver reads and drops it
    ASSERT_EQ(::sendto(fd, junk, sizeof junk, 0, reinterpret_cast<sockaddr*>(&a), sizeof a), static_cast<ssize_t>(sizeof junk));
    ::close(fd);
}
int64_t msSinceWake(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
}
} // namespace

TEST(EmbeddedDriverWake, ThreadedDriverExposesNoWakeFds) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Threaded);
    int fds[4];
    EXPECT_EQ(drv.wakeFds(fds, 4), 0) << "only the pumping owner of an INVOKER driver may park on its sockets";
}

TEST(EmbeddedDriverWakeRegression, ParkedOwnerWakesOnDatagramNotTimeout) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only (Aeron's transport pollers use epoll there)";
#else
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    int fds[4];
    const int n = drv.wakeFds(fds, 4);
    ASSERT_EQ(n, 2) << "receiver + sender transport pollers";

    const uint16_t port = freeUdpPortForWake();
    const int64_t reg = a->addSubscription("aeron:udp?endpoint=127.0.0.1:" + std::to_string(port), 9101);
    std::shared_ptr<aeron::Subscription> sub;
    for (int i = 0; i < 100'000 && !sub; ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        sub = a->findSubscription(reg);
    }
    ASSERT_TRUE(sub) << "subscription (and its UDP socket) resolved";
    for (int i = 0; i < 100; ++i) { drv.doWork(); a->conductorAgentInvoker().invoke(); }

    // Nothing readable: a park waits out its period.
    rune::node::ParkUntilWorkIdle quiet(rune::node::IdleShape{0, 0, 30'000'000, 30'000'000}, fds, n);
    auto t0 = std::chrono::steady_clock::now();
    quiet.idle(0);
    EXPECT_GE(msSinceWake(t0), 20);
    EXPECT_EQ(quiet.readinessWakes(), 0u);

    // A datagram on the driver's socket ends a 5 s park immediately.
    rune::node::ParkUntilWorkIdle idle(rune::node::IdleShape{0, 0, 5'000'000'000, 5'000'000'000}, fds, n);
    // No sleep (tests never sleep): the datagram races the park — it lands before (level-triggered epoll fd:
    // the park returns at once) or during it (the parked owner wakes).  Either way not the 5 s timeout.
    t0 = std::chrono::steady_clock::now();
    std::thread sender([&] { sendJunkDatagram(port); });
    idle.idle(0);
    const int64_t waited = msSinceWake(t0);
    sender.join();
    EXPECT_LT(waited, 2'000) << "parked until the datagram, not the 5 s timeout";
    EXPECT_EQ(idle.readinessWakes(), 1u);

    // The owner's pass drains it; the fds are quiet again (level-triggered, no sticky wake).
    for (int i = 0; i < 10; ++i) drv.doWork();
    rune::node::ParkUntilWorkIdle after(rune::node::IdleShape{0, 0, 30'000'000, 30'000'000}, fds, n);
    t0 = std::chrono::steady_clock::now();
    after.idle(0);
    EXPECT_GE(msSinceWake(t0), 20) << "a drained socket must not keep waking the owner";
    EXPECT_EQ(after.readinessWakes(), 0u);
    sub.reset();
    a.reset();
#endif
}

// ---------------------------------------------------------------------------
// A7 epoll-first transport polling (findings §4.12): the invoker driver polls a poller with > 1 transport
// through its epoll instance (one epoll_wait + recvmmsg of the ready sockets) instead of recvmmsg-ing every
// transport each duty cycle.  Delivery must be unchanged: several UDP subscriptions on ONE driver (so its
// receiver poller holds > 1 transport) all receive what the driver publishes to them.
// ---------------------------------------------------------------------------
namespace {
void pumpBoth(EmbeddedDriver& drv, aeron::Aeron& a) { drv.doWork(); a.conductorAgentInvoker().invoke(); }

int deliveredOverUdp(EmbeddedDriver& drv, aeron::Aeron& a, int channels) {
    std::vector<std::shared_ptr<aeron::Subscription>> subs;
    std::vector<std::shared_ptr<aeron::ExclusivePublication>> pubs;
    for (int i = 0; i < channels; ++i) {
        const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPortForWake());
        const int64_t s = a.addSubscription(ch, 9200 + i);
        const int64_t p = a.addExclusivePublication(ch, 9200 + i);
        std::shared_ptr<aeron::Subscription> sub;
        std::shared_ptr<aeron::ExclusivePublication> pub;
        for (int k = 0; k < 200'000 && !(sub && pub && sub->isConnected() && pub->isConnected()); ++k) {
            pumpBoth(drv, a);
            if (!sub) sub = a.findSubscription(s);
            if (!pub) pub = a.findExclusivePublication(p);
        }
        if (!(sub && pub && sub->isConnected() && pub->isConnected())) return -1;
        subs.push_back(sub);
        pubs.push_back(pub);
    }
    constexpr int kMsgs = 50;
    int got = 0;
    std::array<uint8_t, 64> buf{};
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    for (int m = 0; m < kMsgs; ++m)
        for (auto& pub : pubs)
            for (int k = 0; k < 100'000 && pub->offer(ab, 0, 32) < 0; ++k) pumpBoth(drv, a);
    for (int k = 0; k < 400'000 && got < kMsgs * channels; ++k) {
        pumpBoth(drv, a);
        for (auto& sub : subs)
            got += sub->poll([](const aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                                const aeron::concurrent::logbuffer::Header&) {}, 16);
    }
    return got;
}
} // namespace

TEST(EmbeddedDriverEpollPoll, InstalledOnlyWhereSupportedAndConfigured) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
#if defined(__linux__)
    EXPECT_TRUE(drv.epollPollActive()) << "default on (Linux)";
#else
    EXPECT_FALSE(drv.epollPollActive()) << "Linux only (Aeron's pollers use epoll there)";
#endif
    TmpDir d2;
    auto off = cfgAt(d2.path);
    off.epollPoll = false;
    EmbeddedDriver stock(off, DriverMode::Invoker);
    EXPECT_FALSE(stock.epollPollActive()) << "RUNE_AERON_EPOLL_POLL=0 -> Aeron's stock poller (the A/B off arm)";
    TmpDir d3;
    auto nogso = cfgAt(d3.path);
    nogso.udpGso = false;   // stock Aeron bindings: patched through a private copy, never the shared struct
    EmbeddedDriver plain(nogso, DriverMode::Invoker);
#if defined(__linux__)
    EXPECT_TRUE(plain.epollPollActive());
#endif
    EXPECT_FALSE(stock.epollPollActive()) << "patching one driver's bindings never leaks into another's";
}

TEST(EmbeddedDriverEpollPollRegression, MultiTransportPollerDeliversEverything) {
    for (bool gso : {true, false}) {
        TmpDir d;
        auto c = cfgAt(d.path);
        c.udpGso = gso;
        EmbeddedDriver drv(c, DriverMode::Invoker);
        auto a = connect(d.path, /*invoker=*/true);
        ASSERT_TRUE(a);
        EXPECT_EQ(deliveredOverUdp(drv, *a, 3), 3 * 50) << "gso=" << gso << " epoll=" << drv.epollPollActive();
        a.reset();
    }
}
