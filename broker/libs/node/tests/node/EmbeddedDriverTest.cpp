// EmbeddedDriverTest.cpp — one embedded driver is driven by exactly one thread
// (spec §3.6); invoker mode has no driver thread; stale/active dir handling.
#include <rune/aeronutil/testing/FreePort.h>
#include <sys/stat.h>
#include <rune/node/EmbeddedDriver.h>
#include "DriverWakePollProbe.h"

#include <Aeron.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <fcntl.h>
#include <aeronmd/aeronmd.h>

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
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); fs::remove(path.string() + ".lock", ec); }
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

// #654: the dir is owned through an exclusive lock, not judged from Aeron's 1 s heartbeat.  `a` is never pumped, so
// its heartbeat may be arbitrarily stale (slow start under a sanitizer); it is still live and still refused.
TEST(EmbeddedDriverRegression, ALiveDriverDirIsRefusedWhateverItsHeartbeatAgeRegression) {
    TmpDir d;
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    EXPECT_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker), std::runtime_error);
}

// A holder killed without a clean shutdown (SIGKILL, docker kill) leaves its dir behind with a fresh heartbeat;
// the kernel drops its lock, so the restart succeeds at once.  The pipe orders the kill after the child's start.
TEST(EmbeddedDriverRegression, ADirIsReusableRightAfterItsHolderIsKilledRegression) {
    TmpDir d;
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::close(fds[0]);
        try {
            EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
            a.doWork();
            const char byte = 1;
            if (::write(fds[1], &byte, 1) != 1) ::_exit(2);
            for (;;) ::pause();
        } catch (...) {
            ::_exit(3);
        }
    }
    ::close(fds[1]);
    char byte = 0;
    ASSERT_EQ(::read(fds[0], &byte, 1), 1) << "the child never got its driver up";
    ::close(fds[0]);
    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    EXPECT_NO_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker));
}

TEST(EmbeddedDriverRegression, ADirIsReusableAfterItsHolderIsDestroyedRegression) {
    TmpDir d;
    { EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker); }
    EXPECT_NO_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker));
}

// ---- #654 adversarial review: the dir lock's fd, its throw paths and its spellings ----
namespace {
/// The fds of this process open on `path` (via /proc/self/fd).
std::vector<int> fdsOpenOn(const std::string& path) {
    std::vector<int> out;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator("/proc/self/fd", ec)) {
        std::error_code lec;
        const fs::path target = fs::read_symlink(e.path(), lec);
        if (!lec && target == fs::path(path)) out.push_back(std::stoi(e.path().filename().string()));
    }
    return out;
}
} // namespace

// The lock fd must never leak into an exec'd child: a leaked fd keeps the dir "in use" for as long as the child
// lives, after the broker itself has exited.  Exactly one fd, close-on-exec.
TEST(EmbeddedDriverRegression, TheDirLockFdIsCloseOnExecAdversarialRegression) {
    TmpDir d;
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    const std::vector<int> fds = fdsOpenOn(d.path.string() + ".lock");
    ASSERT_EQ(fds.size(), 1u) << "exactly one fd holds the lock";
    const int flags = ::fcntl(fds[0], F_GETFD);
    ASSERT_GE(flags, 0);
    EXPECT_NE(flags & FD_CLOEXEC, 0) << "the lock fd is inherited across exec";
}

// A constructor that throws AFTER the lock was taken (an idle strategy Aeron refuses) releases it: the next start on
// the same dir succeeds and no fd on the lock file is left open.
TEST(EmbeddedDriverRegression, AStartThatThrowsAfterTakingTheLockReleasesItAdversarialRegression) {
    TmpDir d;
    auto bad = cfgAt(d.path);
    bad.idleStrategy = "no-such-idle-strategy";
    for (int i = 0; i < 3; ++i)
        EXPECT_THROW(EmbeddedDriver(bad, DriverMode::Threaded), std::runtime_error) << "attempt " << i;
    EXPECT_TRUE(fdsOpenOn(d.path.string() + ".lock").empty()) << "a failed start leaked the lock fd";
    EXPECT_NO_THROW(EmbeddedDriver ok(cfgAt(d.path), DriverMode::Invoker));
}

// Spellings that differ only in trailing slashes name the same dir and the same lock; a stale lock FILE (left by a
// killed holder, nobody holding it) never blocks a start.
TEST(EmbeddedDriverRegression, TrailingSlashSpellingsShareOneLockAdversarialRegression) {
    TmpDir d;
    {   // a stale lock file and a stale dir: reclaimed
        fs::create_directories(d.path);
        { std::FILE* f = std::fopen((d.path.string() + ".lock").c_str(), "w"); ASSERT_NE(f, nullptr); std::fclose(f); }
    }
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    for (const std::string& alias : {d.path.string() + "/", d.path.string() + "//"}) {
        auto c = cfgAt(d.path);
        c.dir = alias;
        EXPECT_THROW(EmbeddedDriver b(c, DriverMode::Invoker), std::runtime_error) << alias;
    }
}

// #654 review — FAILING (not committed).  A live driver that holds no lock (a pre-#654 broker or client during a
// rolling upgrade, a stock aeronmd, any raw C-API driver) must still be refused: the heartbeat check was the only
// guard against it.  Today the new driver takes the (free) lock and dir_delete_on_start wipes the live driver's dir.
TEST(EmbeddedDriverRegression, ALiveDriverThatHoldsNoLockIsRefusedAdversarialRegression) {
    TmpDir d;
    aeron_driver_context_t* ctx = nullptr;
    aeron_driver_t* raw = nullptr;
    ASSERT_EQ(aeron_driver_context_init(&ctx), 0);
    aeron_driver_context_set_dir(ctx, d.path.c_str());
    aeron_driver_context_set_threading_mode(ctx, AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_term_buffer_length(ctx, 64 * 1024);
    ASSERT_EQ(aeron_driver_init(&raw, ctx), 0);
    ASSERT_EQ(aeron_driver_start(raw, /*manual_main_loop=*/true), 0);
    for (int i = 0; i < 16; ++i) aeron_driver_main_do_work(raw);   // heartbeat written: live by Aeron's rule
    ASSERT_TRUE(aeron_is_driver_active(d.path.c_str(), 10000, [](const char*) {}));
    EXPECT_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker), std::runtime_error)
        << "a second driver started on a live driver's dir (and wiped it)";
    aeron_driver_close(raw);   // also closes ctx
}

// #654 review — FAILING (not committed).  The lock is keyed by the path STRING, not by the dir: "dir/." locks
// "dir/..lock" (a file INSIDE the dir) and a symlink "L" -> dir locks "L.lock", so both take a free lock and run
// dir_delete_on_start over the live driver's dir.  "dir/." deletes every file in it before rmdir(".") fails with
// EINVAL; the symlink spelling starts a second driver on the same dir.  Either way the live driver loses its cnc.dat.
TEST(EmbeddedDriverRegression, AnotherSpellingOfTheSameDirIsRefusedAdversarialRegression) {
    TmpDir d;
    TmpDir link;
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    const fs::path cnc = d.path / "cnc.dat";
    ASSERT_TRUE(fs::exists(cnc));
    fs::create_directory_symlink(d.path, link.path);
    for (const std::string& alias : {d.path.string() + "/.", link.path.string()}) {
        auto c = cfgAt(d.path);
        c.dir = alias;
        EXPECT_THROW(EmbeddedDriver b(c, DriverMode::Invoker), std::runtime_error) << alias;
        EXPECT_TRUE(fs::exists(cnc)) << "starting on '" << alias << "' wiped the live driver's dir";
    }
    std::error_code ec;
    fs::remove(link.path, ec);
    fs::remove(d.path.string() + "/..lock", ec);
}

// #654 review: a symlink planted at the lock path is refused (O_NOFOLLOW), never followed: the link's target is
// not locked, created or modified.
TEST(EmbeddedDriverRegression, LockPathSymlinkIsRefusedNotFollowedAdversarialRegression) {
    TmpDir d;
    TmpDir target;
    {
        std::ofstream(target.path) << "precious";
    }
    fs::create_symlink(target.path, d.path.string() + ".lock");
    EXPECT_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker), std::runtime_error);
    std::ifstream in(target.path);
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "precious");
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

// D2 round 2: the node driver's idle SHAPE reaches Aeron — the init args are applied BEFORE the strategy (Aeron
// builds the strategy's state from the args present at that call) — and a malformed shape or an unknown strategy
// refuses construction (Aeron's setter fails and leaves a NULL idle function behind, which the driver thread would
// call on its first idle pass).
TEST(EmbeddedDriverRegression, IdleStrategyArgsReachAeronRegression) {
    TmpDir d;
    auto c = cfgAt(d.path);
    c.idleStrategy     = "backoff";
    c.idleStrategyArgs = "1-0-50000-1000000";
    EmbeddedDriver drv(c, DriverMode::Threaded);
    EXPECT_EQ(drv.idleStrategyArgsForTest(), "1-0-50000-1000000");
    TmpDir d2;
    auto none = cfgAt(d2.path);
    none.idleStrategy = "backoff";   // no args: Aeron's own (env / built-in) shape, untouched
    EmbeddedDriver plain(none, DriverMode::Threaded);
    EXPECT_EQ(plain.idleStrategyArgsForTest(), std::string(std::getenv("AERON_SHARED_IDLE_STRATEGY_INIT_ARGS") != nullptr
                                                               ? std::getenv("AERON_SHARED_IDLE_STRATEGY_INIT_ARGS") : ""));
}

TEST(EmbeddedDriverRegression, MalformedIdleStrategyRefusesStartRegression) {
    TmpDir d;
    auto bad = cfgAt(d.path);
    bad.idleStrategy     = "backoff";
    bad.idleStrategyArgs = "fast";
    EXPECT_THROW(EmbeddedDriver(bad, DriverMode::Threaded), std::runtime_error);
    TmpDir d2;
    auto unknown = cfgAt(d2.path);
    unknown.idleStrategy = "no-such-idle-strategy";
    EXPECT_THROW(EmbeddedDriver(unknown, DriverMode::Threaded), std::runtime_error);
    TmpDir d3;
    auto dedicated = cfgAt(d3.path);
    dedicated.threadingMode    = NodeConfig::AeronThreadingMode::Dedicated;
    dedicated.idleStrategy     = "backoff";
    dedicated.idleStrategyArgs = "fast";
    EXPECT_THROW(EmbeddedDriver(dedicated, DriverMode::Threaded), std::runtime_error);
}

// D2 review R2-L1: with no explicit args the setter builds the strategy from the context's args — initialised from
// AERON_*_IDLE_STRATEGY_INIT_ARGS — so validation must use THOSE (it used NULL args and passed).  The validation MUST
// run before any context setter: a failing Aeron setter frees the strategy name (a heap copy that
// aeron_driver_context_init sets) without resetting it, and aeron_driver_context_close frees it again — a double free
// (ASan-proven by the review; macOS malloc misses it, so this test pins the refusal and its text, not the crash).
TEST(EmbeddedDriverRegression, EnvIdleArgsMismatchRefusesCleanlyRegression) {
    struct EnvGuard {
        std::string name; std::string old; bool had;
        EnvGuard(const char* n, const char* v) : name(n), had(std::getenv(n) != nullptr) {
            if (had) old = std::getenv(n);
            ::setenv(n, v, 1);
        }
        ~EnvGuard() { if (had) ::setenv(name.c_str(), old.c_str(), 1); else ::unsetenv(name.c_str()); }
    };
    for (const char* var : {"AERON_SHARED_IDLE_STRATEGY_INIT_ARGS", "AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS"}) {
        EnvGuard env(var, "1-0-100us-2ms");   // valid for backoff, not for sleep-ns
        TmpDir d;
        auto c = cfgAt(d.path);
        c.idleStrategy = "sleep-ns";
        if (std::string(var).find("CONDUCTOR") != std::string::npos)
            c.threadingMode = NodeConfig::AeronThreadingMode::Dedicated;
        try {
            EmbeddedDriver drv(c, DriverMode::Threaded);
            ADD_FAILURE() << var << ": a strategy Aeron cannot build was accepted";
        } catch (const std::runtime_error& e) {
            // The refusal names the args Aeron actually used (the env's), not "".
            EXPECT_NE(std::string(e.what()).find("1-0-100us-2ms"), std::string::npos) << e.what();
        }
    }
}

// ---------------------------------------------------------------------------
// A7 park-until-work (findings §4.12): an invoker driver exposes its transport pollers' epoll fds; a
// shard worker parked on them wakes on the next datagram instead of sleeping out its park period.
// Linux only (Aeron's pollers use epoll there; other platforms expose none -> timed park).
// ---------------------------------------------------------------------------
#include <rune/node/ParkUntilWorkIdle.h>
#include <rune/node/WorkerPool.h>
#include <rune/consensus/AgentRunner.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace {
uint16_t freeUdpPortForWake() { return rune::aeronutil::testing::allocFreeUdpPort(); }   // D11: never bind-0
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

/// One UDP channel on the driver under test: a subscription (a receive socket in the receiver poller) and a
/// publication to it (a send socket in the sender poller).
struct UdpLane {
    std::shared_ptr<aeron::Subscription>         sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    int                                          got = 0;
};

/// Opens a lane and pumps until both ends are connected (10 s failure deadline).
bool openLane(EmbeddedDriver& drv, aeron::Aeron& a, int32_t stream, UdpLane& lane) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPortForWake());
    const int64_t s = a.addSubscription(ch, stream);
    const int64_t p = a.addExclusivePublication(ch, stream);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        pumpBoth(drv, a);
        if (!lane.sub) lane.sub = a.findSubscription(s);
        if (!lane.pub) lane.pub = a.findExclusivePublication(p);
        if (lane.sub && lane.pub && lane.sub->isConnected() && lane.pub->isConnected()) return true;
    }
    return false;
}

/// Offers `msgs` messages of `bytes` on the lane (pumping through back-pressure, 10 s deadline).
bool offerOn(EmbeddedDriver& drv, aeron::Aeron& a, UdpLane& lane, int msgs, int bytes) {
    std::vector<uint8_t> buf(static_cast<std::size_t>(bytes), 0x5A);
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (int m = 0; m < msgs; ++m)
        while (lane.pub->offer(ab, 0, bytes) < 0) {
            if (std::chrono::steady_clock::now() > deadline) return false;
            pumpBoth(drv, a);
        }
    return true;
}

int pollLane(UdpLane& lane) {
    const int n = lane.sub->poll([](const aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                                    const aeron::concurrent::logbuffer::Header&) {}, 64);
    lane.got += n;
    return n;
}

/// Pumps and polls every lane until each received `want` more (10 s deadline).  True when all arrived.
bool deliverAll(EmbeddedDriver& drv, aeron::Aeron& a, std::vector<UdpLane*> lanes, int want) {
    std::vector<int> base;
    for (auto* l : lanes) base.push_back(l->got);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        pumpBoth(drv, a);
        bool all = true;
        for (std::size_t i = 0; i < lanes.size(); ++i) {
            pollLane(*lanes[i]);
            all = all && lanes[i]->got - base[i] >= want;
        }
        if (all) return true;
    }
    return false;
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

// A7 review L2: epoll-first polling reads Aeron internals — pinned to the verified release.  An Aeron upgrade
// fails HERE (re-verify rune_driver_wake.c against the new poller / bindings layout, then move the pin); at
// runtime any other release keeps the stock polling.
TEST(EmbeddedDriverEpollPoll, AeronIsTheReleaseEpollFirstWasVerifiedAgainst) {
    EXPECT_TRUE(EmbeddedDriver::epollPollSupportsAeron(aeron_version_major(), aeron_version_minor(),
                                                       aeron_version_patch()))
        << "runtime Aeron " << aeron_version_full() << " is not the epoll-first pin";
    EXPECT_FALSE(EmbeddedDriver::epollPollSupportsAeron(1, 42, 0));
    EXPECT_FALSE(EmbeddedDriver::epollPollSupportsAeron(1, 41, 5));
    EXPECT_FALSE(EmbeddedDriver::epollPollSupportsAeron(2, 41, 4));
}

// Review M2: delivery is unchanged AND the epoll-first branch demonstrably ran (an epoll_wait selected a
// socket, EmbeddedDriver::epollSelectedForTest) — with rune_gso's bindings and with the stock-bindings copy.
// The stock arm (epoll-first off) delivers the same and never takes the epoll path below 6 transports.
TEST(EmbeddedDriverEpollPollRegression, MultiTransportPollerDeliversEverything) {
    for (bool gso : {true, false}) {
        for (bool epoll : {true, false}) {
            TmpDir d;
            auto c = cfgAt(d.path);
            c.udpGso = gso;
            c.epollPoll = epoll;
            EmbeddedDriver drv(c, DriverMode::Invoker);
            auto a = connect(d.path, /*invoker=*/true);
            ASSERT_TRUE(a);
            UdpLane lanes[3];
            for (int i = 0; i < 3; ++i) ASSERT_TRUE(openLane(drv, *a, 9200 + i, lanes[i])) << i;
            (void)drv.epollSelectedForTest(/*reset=*/true);
            for (auto& l : lanes) ASSERT_TRUE(offerOn(drv, *a, l, 50, 32));
            EXPECT_TRUE(deliverAll(drv, *a, {&lanes[0], &lanes[1], &lanes[2]}, 50))
                << "gso=" << gso << " epoll=" << epoll << " got " << lanes[0].got << "/" << lanes[1].got << "/"
                << lanes[2].got;
#if defined(__linux__)
            if (epoll) EXPECT_GT(drv.epollSelectedForTest(false), 0) << "gso=" << gso << ": the epoll-first branch ran";
            else       EXPECT_EQ(drv.epollSelectedForTest(false), 0) << "gso=" << gso << ": stock <= 5 path only";
#endif
            for (auto& l : lanes) { l.pub.reset(); l.sub.reset(); }
            a.reset();
        }
    }
}

// Review M2: transports added and removed while the driver runs cross the 1 <-> 2 threshold of the epoll-first
// branch (one transport: Aeron's recvmmsg path; more: epoll_wait).  Delivery holds after every step.
TEST(EmbeddedDriverEpollPoll, TransportsAddedAndRemovedAtRuntimeKeepDelivering) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    UdpLane first;
    ASSERT_TRUE(openLane(drv, *a, 9500, first));
    // One transport per poller: the stock recvmmsg path (no epoll_wait selected anything).
    (void)drv.epollSelectedForTest(true);
    ASSERT_TRUE(offerOn(drv, *a, first, 20, 32));
    ASSERT_TRUE(deliverAll(drv, *a, {&first}, 20)) << "1 lane";
#if defined(__linux__)
    EXPECT_EQ(drv.epollSelectedForTest(false), 0) << "a single transport keeps Aeron's one-recvmmsg path";
#endif
    // 1 -> 2: the epoll branch takes over.
    UdpLane second;
    ASSERT_TRUE(openLane(drv, *a, 9501, second));
    (void)drv.epollSelectedForTest(true);
    ASSERT_TRUE(offerOn(drv, *a, first, 20, 32));
    ASSERT_TRUE(offerOn(drv, *a, second, 20, 32));
    ASSERT_TRUE(deliverAll(drv, *a, {&first, &second}, 20)) << "2 lanes";
#if defined(__linux__)
    EXPECT_GT(drv.epollSelectedForTest(false), 0);
#endif
    // 2 -> 1: close the second lane's subscription and publication; the first keeps delivering.
    second.sub.reset();
    second.pub.reset();
    for (int i = 0; i < 2'000; ++i) pumpBoth(drv, *a);   // the conductor removes the endpoints
    ASSERT_TRUE(offerOn(drv, *a, first, 20, 32));
    ASSERT_TRUE(deliverAll(drv, *a, {&first}, 20)) << "after removing a lane";
    // Add again (a new port): both deliver.
    UdpLane third;
    ASSERT_TRUE(openLane(drv, *a, 9502, third));
    ASSERT_TRUE(offerOn(drv, *a, first, 20, 32));
    ASSERT_TRUE(offerOn(drv, *a, third, 20, 32));
    ASSERT_TRUE(deliverAll(drv, *a, {&first, &third}, 20)) << "after re-adding a lane";
    first.pub.reset(); first.sub.reset(); third.pub.reset(); third.sub.reset();
    a.reset();
}

// Review M2: a backlog larger than one recvmmsg batch on one socket stays readable (level-triggered) until it is
// drained, and a quiet socket next to it is not starved: its message arrives while the hot backlog is pending.
// Receiver io vector 1 (one datagram per recvmmsg) against 16 datagrams per send.
TEST(EmbeddedDriverEpollPoll, BacklogBeyondOneBatchDrainsAndQuietSocketIsNotStarved) {
    TmpDir d;
    auto c = cfgAt(d.path);
    c.termBufferBytes    = 4 * 1024 * 1024;
    c.udpGso             = false;   // one datagram per recvmmsg slot (no GRO coalescing)
    c.receiverIoVector   = 1;
    c.maxMessagesPerSend = 16;
    EmbeddedDriver drv(c, DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    UdpLane hot, quiet;
    ASSERT_TRUE(openLane(drv, *a, 9600, hot));
    ASSERT_TRUE(openLane(drv, *a, 9601, quiet));
    constexpr int kHot = 400;
    // The whole hot burst sits in the term before anything is polled; then one quiet message.
    std::vector<uint8_t> big(1'000, 0x11);
    aeron::concurrent::AtomicBuffer bab(big.data(), big.size());
    // (A back-pressured offer pumps and retries: the publication limit follows the receiver's status messages.)
    auto offerRetry = [&](aeron::ExclusivePublication& pub, aeron::concurrent::AtomicBuffer& ab, int len) {
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (pub.offer(ab, 0, len) < 0) {
            if (std::chrono::steady_clock::now() > until) return false;
            pumpBoth(drv, *a);
        }
        return true;
    };
    for (int m = 0; m < kHot; ++m) ASSERT_TRUE(offerRetry(*hot.pub, bab, 1'000)) << m;
    std::array<uint8_t, 16> small{};
    aeron::concurrent::AtomicBuffer sab(small.data(), small.size());
    ASSERT_TRUE(offerRetry(*quiet.pub, sab, 16));
    int hotWhenQuietArrived = -1;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline && (hot.got < kHot || quiet.got < 1)) {
        pumpBoth(drv, *a);
        pollLane(hot);
        if (pollLane(quiet) > 0 && hotWhenQuietArrived < 0) hotWhenQuietArrived = hot.got;
    }
    EXPECT_EQ(hot.got, kHot) << "the backlog beyond one recvmmsg batch is fully drained";
    ASSERT_EQ(quiet.got, 1);
    EXPECT_LT(hotWhenQuietArrived, kHot) << "the quiet socket waited for the whole hot backlog (starved)";
    hot.pub.reset(); hot.sub.reset(); quiet.pub.reset(); quiet.sub.reset();
    a.reset();
}

// Review M2: wake through the RUNNER, end to end.  An AgentRunner whose agent pumps an invoker driver parks on
// that driver's wake fds (2 s park period): a datagram from another driver must be processed promptly, not at
// the end of the park.  Linux only.
namespace {
struct PumpAgent final : rune::consensus::Agent {
    EmbeddedDriver&                      drv;
    aeron::Aeron&                        a;
    std::shared_ptr<aeron::Subscription> sub;
    std::atomic<int64_t>                 deliveredAtNs{0};   // test-only cross-thread observation
    std::atomic<int64_t>                 mailboxPostedNs{0}; // stands in for a cross-thread ring (no fd)
    std::atomic<int64_t>                 mailboxTakenNs{0};
    std::atomic<int64_t>                 passes{0};
    bool                                 bound = false;
    PumpAgent(EmbeddedDriver& d, aeron::Aeron& c) : drv(d), a(c) {}
    static int64_t steadyNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    int doWork(int64_t) override {
        if (!bound) { drv.bindPumpThread(); bound = true; }
        passes.fetch_add(1);
        int w = drv.doWork() + a.conductorAgentInvoker().invoke();
        if (sub) w += sub->poll([this](const aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                                       const aeron::concurrent::logbuffer::Header&) {
            if (deliveredAtNs.load() == 0) deliveredAtNs.store(steadyNs());
        }, 16);
        if (mailboxPostedNs.load() != 0 && mailboxTakenNs.load() == 0) { mailboxTakenNs.store(steadyNs()); ++w; }
        return w;
    }
};
template <class Pred>
bool waitFor(std::chrono::milliseconds limit, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::yield();
    }
    return pred();
}
} // namespace

// A7 re-review LOW-2: coverage (the wake already worked before the C1 fix) — not a regression test.
TEST(EmbeddedDriverWake, ParkedRunnerProcessesDatagramPromptly) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only";
#else
    TmpDir rd, td;
    EmbeddedDriver rdrv(cfgAt(rd.path), DriverMode::Invoker);
    auto ra = connect(rd.path, /*invoker=*/true);
    EmbeddedDriver tdrv(cfgAt(td.path), DriverMode::Threaded);
    auto ta = connect(td.path, /*invoker=*/false);
    ASSERT_TRUE(ra && ta);
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPortForWake());
    PumpAgent agent(rdrv, *ra);
    const int64_t sreg = ra->addSubscription(ch, 9700);
    const int64_t preg = ta->addPublication(ch, 9700);
    std::shared_ptr<aeron::Publication> pub;
    for (auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
         std::chrono::steady_clock::now() < deadline && !(agent.sub && pub && pub->isConnected());) {
        pumpBoth(rdrv, *ra);
        if (!agent.sub) agent.sub = ra->findSubscription(sreg);
        if (!pub) pub = ta->findPublication(preg);
    }
    ASSERT_TRUE(agent.sub && pub && pub->isConnected());
    int fds[ParkUntilWorkIdle::kMaxWakeFds];
    const int n = rdrv.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds);
    ASSERT_EQ(n, 2);
    // Every idle pass parks up to 2 s: only readiness can make delivery prompt.
    ParkUntilWorkIdle idle(IdleShape{0, 0, 2'000'000'000, 2'000'000'000}, fds, n);
    rune::consensus::MonotonicNanoClock clk;
    {
        rune::consensus::AgentRunner runner(agent, idle, clk);   // the agent binds the pump on its first pass
        runner.start();
        std::array<uint8_t, 32> msg{};
        aeron::concurrent::AtomicBuffer ab(msg.data(), msg.size());
        int64_t sentNs = 0;
        ASSERT_TRUE(waitFor(std::chrono::seconds(5), [&] {
            if (pub->offer(ab, 0, 32) > 0) { sentNs = PumpAgent::steadyNs(); return true; }
            return false;
        }));
        ASSERT_TRUE(waitFor(std::chrono::seconds(5), [&] { return agent.deliveredAtNs.load() != 0; }));
        EXPECT_LT(agent.deliveredAtNs.load() - sentNs, 500'000'000) << "the parked runner slept out its 2 s park";
        runner.stopAndJoin();
    }
    rdrv.bindPumpThread();   // hand-off back after the join (close on this thread)
    pub.reset();
    agent.sub.reset();
    ta.reset();
    ra.reset();
#endif
}

// Review M1/M2: work with NO fd (a cross-thread ring message) reaches a parked runner within the park cap.
// A7 re-review LOW-2: coverage (the cap held before the fix too) — not a regression test.
TEST(EmbeddedDriverWake, ParkedRunnerTakesFdLessWorkWithinTheParkCap) {
    TmpDir rd;
    EmbeddedDriver rdrv(cfgAt(rd.path), DriverMode::Invoker);
    auto ra = connect(rd.path, /*invoker=*/true);
    ASSERT_TRUE(ra);
    for (int i = 0; i < 100; ++i) pumpBoth(rdrv, *ra);
    PumpAgent agent(rdrv, *ra);
    int fds[ParkUntilWorkIdle::kMaxWakeFds];
    const int n = rdrv.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds);
    NodeConfig cfg;
    cfg.idleMaxParkUs = 20'000;   // 20 ms cap: the bound for fd-less work
    auto idle = makeShardIdle(cfg, fds, n);
    rune::consensus::MonotonicNanoClock clk;
    rune::consensus::AgentRunner runner(agent, *idle, clk);
    runner.start();
    // Let the runner reach its longest park (no traffic: every pass is idle) — observed, not slept: 10 spins +
    // 20 yields + 15 doublings (1 µs .. 16 ms) reach the 20 ms cap; 80 passes are past it.
    ASSERT_TRUE(waitFor(std::chrono::seconds(5), [&] { return agent.passes.load() >= 80; }));
    const int64_t postedNs = PumpAgent::steadyNs();
    agent.mailboxPostedNs.store(postedNs);
    ASSERT_TRUE(waitFor(std::chrono::seconds(5), [&] { return agent.mailboxTakenNs.load() != 0; }));
    EXPECT_LT(agent.mailboxTakenNs.load() - postedNs, 20'000'000 + 100'000'000)
        << "fd-less work must be bounded by the park cap (20 ms + scheduling margin)";
    runner.stopAndJoin();
    rdrv.bindPumpThread();
    ra.reset();
}

// ---------------------------------------------------------------------------
// A7 review C1: a peer that is down (its host up, its port closed) answers every send on a CONNECTED Aeron send
// socket with ICMP port-unreachable; the kernel parks the error on the socket (sk_err) and epoll reports it as
// EPOLLERR without EPOLLIN.  Epoll-first polling skipped such events, so the error stayed pending, the poller's
// epoll fd stayed readable, and a worker parked on it woke at once forever: 100% CPU per worker while a peer is
// down.  Every pass must consume what epoll reports: after a pass with no work the wake fds are quiet.
// Linux only (Aeron's pollers use epoll there).
// ---------------------------------------------------------------------------
namespace {
template <class Pred>
bool pumpUntilFor(EmbeddedDriver& drv, aeron::Aeron& a, std::chrono::milliseconds limit, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        drv.doWork();
        a.conductorAgentInvoker().invoke();
        if (pred()) return true;
    }
    return false;
}
} // namespace

TEST(EmbeddedDriverWakeRegression, PeerDownNeverLeavesWakeFdsReadyAfterAnIdlePassRegression) {
#if !defined(__linux__)
    GTEST_SKIP() << "driver wake fds are Linux-only (Aeron's transport pollers use epoll there)";
#else
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    int fds[4];
    const int n = drv.wakeFds(fds, 4);
    ASSERT_EQ(n, 2);
    // Two publications to two CLOSED loopback ports: two connected send sockets in the sender poller (> 1
    // transport: the epoll-first branch).  Every setup frame they send comes back as ICMP port-unreachable.
    const int64_t r1 = a->addExclusivePublication("aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPortForWake()), 9301);
    const int64_t r2 = a->addExclusivePublication("aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPortForWake()), 9302);
    std::shared_ptr<aeron::ExclusivePublication> p1, p2;
    ASSERT_TRUE(pumpUntilFor(drv, *a, std::chrono::seconds(10), [&] {
        if (!p1) p1 = a->findExclusivePublication(r1);
        if (!p2) p2 = a->findExclusivePublication(r2);
        return p1 && p2;
    }));
    auto wakeReady = [&] {
        pollfd pf[4];
        for (int i = 0; i < n; ++i) pf[i] = pollfd{fds[i], POLLIN, 0};
        const timespec zero{0, 0};
        return ::ppoll(pf, static_cast<nfds_t>(n), &zero, nullptr) > 0;
    };
    // A7 re-review LOW-1: the positive control — an ICMP error was actually pending at least once (a wake fd
    // readable between passes: the only readiness these send-only sockets can have).  Without it the quiet-park
    // assertions below would pass vacuously if setup frames / ICMP stopped (an Aeron upgrade, a sysctl, a route).
    int pendingSeen = 0;
    // Second witness (client 35924fb): an epoll_wait of the SENDER poller that selected a socket — these
    // send-only sockets have no peer, so a selection is an EPOLLERR the pass consumed.  It does not depend on
    // catching the error between two sub-microsecond passes (the next poll usually consumes it first).
    (void)drv.epollSelectedForTest(/*reset=*/true);
    for (int round = 0; round < 3; ++round) {
        // Setup frames go out every ~100 ms: pump until an ICMP error is pending (a wake fd readable after a
        // pass) or 350 ms (>= 3 setup frames per publication) have passed.
        if (pumpUntilFor(drv, *a, std::chrono::milliseconds(350), wakeReady)) ++pendingSeen;
        // The next pass that finds no work (no send after its poll) must leave nothing readable behind.
        ASSERT_TRUE(pumpUntilFor(drv, *a, std::chrono::seconds(5), [&] { return drv.doWork() == 0; }));
        rune::node::ParkUntilWorkIdle idle(rune::node::IdleShape{0, 0, 30'000'000, 30'000'000}, fds, n);
        const auto t0 = std::chrono::steady_clock::now();
        idle.idle(0);
        EXPECT_GE(msSinceWake(t0), 20) << "round " << round << ": a peer-down error must not end the park";
        EXPECT_EQ(idle.readinessWakes(), 0u) << "round " << round << ": wake fd left ready by an unconsumed EPOLLERR";
    }
    // Positive control: without a pending ICMP error the test exercised nothing.
    // Some network namespaces (Docker bridge / container CI) never deliver the
    // port-unreachable error to the sender — that environment cannot exercise
    // the path, so it SKIPS (loudly) instead of passing vacuously or failing.
    const int errorsConsumed = drv.epollSelectedForTest(/*reset=*/false);
    if (pendingSeen == 0 && errorsConsumed == 0) {
        p1.reset();
        p2.reset();
        a.reset();
        GTEST_SKIP() << "no ICMP port-unreachable error was ever delivered in this network namespace — "
                        "the peer-down path cannot be exercised here";
    }
    p1.reset();
    p2.reset();
    a.reset();
#endif
}

// ---------------------------------------------------------------------------
// Aeron 1.51 upgrade: Aeron's transport poller no longer ends a pass on one socket's recvmmsg failure.  It logs the
// error to the driver's distinct error log and errors counter and goes on with the remaining sockets.  The
// epoll-first poller mirrors that: under 1.41.4's rule (return -1 at the first failing socket) the other ready
// socket's work was dropped from the pass and the error never reached the error log.
// ---------------------------------------------------------------------------
TEST(EmbeddedDriverWakeRegression, EpollFirstPollLogsAFailingSocketAndPollsTheRestRegression) {
#if !defined(__linux__)
    GTEST_SKIP() << "epoll-first polling is Linux-only";
#else
    rune_poll_probe_result_t r{};
    ASSERT_EQ(rune_probe_epoll_poll_one_failing_socket(&r), 0) << "probe setup failed";
    ASSERT_EQ(r.installed, 1) << "the epoll-first poller must install on a stock driver context";
    EXPECT_EQ(r.work, 1) << "the healthy socket's work counts; the failing socket does not end the pass";
    EXPECT_EQ(r.errors_counter, 1) << "the failure is counted on the driver's errors counter";
    EXPECT_EQ(r.observations, 1) << "the failure is recorded in the driver's distinct error log";
#endif
}
