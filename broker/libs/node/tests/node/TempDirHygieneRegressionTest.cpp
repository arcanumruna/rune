// TempDirHygieneRegressionTest.cpp - catalog history Task 21 fix round 1 (R5 / I5): the catalog cluster harness
// removes its directory tree on success AND when its constructor throws (the destructor of a half-built object
// never runs, so the tree is owned by a member guard).
#include "support/CatalogClusterHarness.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

using rune::node::test::CatalogClusterHarness;
namespace fs = std::filesystem;

std::vector<std::string> ownTrees() {
    std::vector<std::string> out;
    const std::string prefix = "rune_cluster_" + std::to_string(::getpid()) + "_";
    for (const auto& e : fs::directory_iterator(fs::temp_directory_path())) {
        const std::string n = e.path().filename().string();
        if (n.rfind(prefix, 0) == 0) out.push_back(n);
    }
    return out;
}

} // namespace

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirOnSuccess) {
    ASSERT_TRUE(ownTrees().empty());
    {
        CatalogClusterHarness h("hyg", 3);
        EXPECT_EQ(ownTrees().size(), 1u);
    }
    EXPECT_TRUE(ownTrees().empty()) << "a destroyed CatalogClusterHarness left its tree behind";
}

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirWhenTheCtorThrowsBeforeOpen) {
    ASSERT_TRUE(ownTrees().empty());
    EXPECT_THROW(CatalogClusterHarness("hyg", 2), std::logic_error);   // brokers != kBrokers: thrown in the ctor body
    EXPECT_TRUE(ownTrees().empty());
}

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirWhenTheCtorThrowsAfterTheBrokersOpened) {
    ASSERT_TRUE(ownTrees().empty());
    CatalogClusterHarness::failAfterOpenForTest = true;
    EXPECT_THROW(CatalogClusterHarness("hyg", 3), std::logic_error);   // after open_(1..3) made the broker dirs
    CatalogClusterHarness::failAfterOpenForTest = false;
    EXPECT_TRUE(ownTrees().empty()) << "a throwing ctor leaked the 3-broker tree";
}

// ── Issue #375: the per-process test temp root ──────────────────────────────────────────────────────────────────
// Every gtest binary runs under a per-process temp root (cmake/test_support/TestTmpRoot.cpp, wired by
// cmake/RuneTestTmpRoot.cmake), removed at exit pass or fail; a dead owner's root is swept by the next binary that
// starts in the same TMPDIR.  The children are this same binary, started with posix_spawn and waited on through a
// pipe EOF with a hard poll() deadline (no sleeps); a child that overruns it is SIGKILLed and the test fails.
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

extern char** environ;

namespace {

namespace fs = std::filesystem;

constexpr int kChildDeadlineMs = 120'000;

std::string selfExe() {
#if defined(__APPLE__)
    char buf[4096];
    uint32_t size = sizeof buf;
    if (_NSGetExecutablePath(buf, &size) != 0) return {};
    std::error_code ec;
    return fs::canonical(buf, ec).string();
#else
    std::error_code ec;
    return fs::read_symlink("/proc/self/exe", ec).string();
#endif
}

std::string ownRootName() { return "rune_t" + std::to_string(static_cast<long>(::getpid())); }

struct ChildResult {
    bool exited    = false;  // false: spawn failed or the deadline killed it
    int  exitCode  = -1;
    std::string error;
};

// Runs this binary with `args` and the current environment with TMPDIR replaced by `tmpdir` (plus `extraEnv`);
// stdout/stderr go to `logPath`.
ChildResult runSelf(const std::vector<std::string>& args, const std::string& tmpdir,
                    const std::vector<std::string>& extraEnv, const std::string& logPath) {
    ChildResult r;
    const std::string exe = selfExe();
    if (exe.empty()) { r.error = "cannot resolve own executable"; return r; }

    std::vector<std::string> envStore;
    for (char** e = environ; *e != nullptr; ++e) {
        const std::string kv{*e};
        if (kv.rfind("TMPDIR=", 0) == 0 || kv.rfind("RUNE_TEST_KEEP_TMP=", 0) == 0) continue;
        if (kv.rfind("GTEST_", 0) == 0) continue;  // no inherited sharding / output env in the child
        envStore.push_back(kv);
    }
    envStore.push_back("TMPDIR=" + tmpdir);
    for (const auto& kv : extraEnv) envStore.push_back(kv);
    std::vector<char*> envp;
    for (auto& kv : envStore) envp.push_back(kv.data());
    envp.push_back(nullptr);

    std::vector<std::string> argStore{exe};
    for (const auto& a : args) argStore.push_back(a);
    std::vector<char*> argv;
    for (auto& a : argStore) argv.push_back(a.data());
    argv.push_back(nullptr);

    int fds[2];
    if (::pipe(fds) != 0) { r.error = "pipe failed"; return r; }
    ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);  // read end stays with us; the write end is inherited by the child

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, logPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&fa, STDOUT_FILENO, STDERR_FILENO);
    pid_t pid = -1;
    const int rc = ::posix_spawn(&pid, exe.c_str(), &fa, nullptr, argv.data(), envp.data());
    posix_spawn_file_actions_destroy(&fa);
    ::close(fds[1]);
    if (rc != 0) { ::close(fds[0]); r.error = "posix_spawn failed: " + std::to_string(rc); return r; }

    // The child holds the write end until it exits: EOF on the read end = it is gone.
    pollfd p{fds[0], POLLIN, 0};
    bool done = false;
    for (int iter = 0; iter < 64 && !done; ++iter) {
        const int n = ::poll(&p, 1, kChildDeadlineMs);
        if (n == 0) break;                                   // deadline
        if (n < 0) { if (errno == EINTR) continue; break; }
        char c;
        const ssize_t got = ::read(fds[0], &c, 1);
        if (got == 0) done = true;                           // EOF
        else if (got < 0 && errno != EINTR) break;
    }
    ::close(fds[0]);
    if (!done) {
        ::kill(pid, SIGKILL);
        int st = 0;
        ::waitpid(pid, &st, 0);
        r.error = "child overran the " + std::to_string(kChildDeadlineMs) + " ms deadline";
        return r;
    }
    int st = 0;
    while (::waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    if (WIFEXITED(st)) { r.exited = true; r.exitCode = WEXITSTATUS(st); }
    else r.error = "child died by signal " + std::to_string(WIFSIGNALED(st) ? WTERMSIG(st) : -1);
    return r;
}

std::string slurp(const std::string& path) {
    std::ifstream in{path};
    return {std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
}

}  // namespace

TEST(TempDirHygieneRegression, TheTestTmpRootIsPerProcessRegression) {
    const fs::path tmp = fs::temp_directory_path();
    // temp_directory_path() may carry a trailing separator; compare the last real component.
    const fs::path norm = tmp.filename().empty() ? tmp.parent_path() : tmp;
    EXPECT_EQ(norm.filename().string(), ownRootName()) << "temp_directory_path() = " << tmp;
    EXPECT_TRUE(fs::is_directory(norm)) << norm;
}

TEST(TempDirHygieneRegression, ADeadOwnersRootIsSweptAtStartupRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    // The child starts in our root's parent (the inherited TMPDIR), as every sibling test binary does.
    const fs::path base = root.parent_path();

    // A dead owner: a child that exits at once, reaped before its root is planted.
    const pid_t dead = ::fork();
    ASSERT_GE(dead, 0);
    if (dead == 0) ::_exit(0);
    int st = 0;
    ASSERT_EQ(::waitpid(dead, &st, 0), dead);
    const fs::path deadRoot = base / ("rune_t" + std::to_string(static_cast<long>(dead)));
    fs::create_directories(deadRoot / "x" / "y");
    { std::ofstream{deadRoot / "x" / "y" / "f"} << "leftover"; }
    // A live owner (our own root) and a name that is not rune_t<digits>.
    const fs::path liveRoot = base / ownRootName();
    ASSERT_EQ(liveRoot, root);
    fs::create_directories(liveRoot / "z");
    const fs::path unrelated = base / ("rune_tx_keep" + std::to_string(static_cast<long>(::getpid())));
    fs::create_directories(unrelated);
    ASSERT_TRUE(fs::exists(deadRoot / "x" / "y" / "f"));

    const ChildResult r = runSelf({"--gtest_filter=TempDirHygieneRegression.TheTestTmpRootIsPerProcessRegression"},
                                  base.string(), {}, (root / "sweep_child.log").string());
    ASSERT_TRUE(r.exited) << r.error << "\n" << slurp((root / "sweep_child.log").string());
    EXPECT_EQ(r.exitCode, 0) << slurp((root / "sweep_child.log").string());

    EXPECT_FALSE(fs::exists(deadRoot)) << "a dead owner's root survived the child's startup sweep: " << deadRoot;
    EXPECT_TRUE(fs::is_directory(liveRoot / "z")) << "a live owner's root was touched";
    EXPECT_TRUE(fs::is_directory(unrelated)) << "a non rune_t<digits> entry was touched";
    fs::remove_all(liveRoot / "z");
    fs::remove_all(unrelated);
}

// Helper body for AFailingBinaryRemovesItsRootAtExitRegression: fails only inside that test's child.
TEST(TempDirHygieneChildHelper, FailsOnlyInsideTheHygieneChild) {
    const char* marker = std::getenv("RUNE_TMP_HYG_CHILD");
    if (marker == nullptr) GTEST_SKIP() << "runs only as the child of AFailingBinaryRemovesItsRootAtExitRegression";
    const fs::path root = fs::temp_directory_path();
    fs::create_directories(root / "data" / "seg");
    { std::ofstream{root / "data" / "seg" / "00000000.log"} << std::string(4096, 'x'); }
    { std::ofstream{marker} << root.string(); }  // tell the parent where our root was
    FAIL() << "intentional failure (child of the temp-root hygiene test)";
}

TEST(TempDirHygieneRegression, AFailingBinaryRemovesItsRootAtExitRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    const fs::path d = root / "hyg_fail_tmpdir";
    fs::remove_all(d);
    fs::create_directories(d);
    const fs::path marker = root / "hyg_fail_marker";
    fs::remove(marker);

    const ChildResult r = runSelf({"--gtest_filter=TempDirHygieneChildHelper.FailsOnlyInsideTheHygieneChild"},
                                  d.string(), {"RUNE_TMP_HYG_CHILD=" + marker.string()},
                                  (root / "fail_child.log").string());
    ASSERT_TRUE(r.exited) << r.error << "\n" << slurp((root / "fail_child.log").string());
    EXPECT_NE(r.exitCode, 0) << "the helper must fail\n" << slurp((root / "fail_child.log").string());

    const std::string childRoot = slurp(marker.string());
    ASSERT_FALSE(childRoot.empty()) << "the child never ran its body\n" << slurp((root / "fail_child.log").string());
    EXPECT_EQ(fs::path(childRoot).parent_path(), d) << "child root " << childRoot << " not under its TMPDIR";
    EXPECT_FALSE(fs::exists(childRoot)) << "the failing child's root survived its exit: " << childRoot;
    for (const auto& e : fs::directory_iterator(d))
        EXPECT_NE(e.path().filename().string().rfind("rune_t", 0), 0u) << "left behind: " << e.path();
    fs::remove_all(d);
    fs::remove(marker);
}

// ── PR #574 adversarial review: attacks on the per-process temp root (#375) ──────────────────────────────────────
// Each attack runs its children in a private base directory under this process's own root (never the shared TMPDIR),
// so a concurrent test binary on the host can neither disturb nor be disturbed by the planted entries.
#include <gtest/gtest-spi.h>

namespace {

// A child of this binary that keeps running: stdin is a pipe the parent closes to release it, stdout a pipe the
// parent reads its READY line from.  stderr goes to `logPath`.
struct LiveChild {
    pid_t pid   = -1;
    int   inFd  = -1;   // parent's write end of the child's stdin
    int   outFd = -1;   // parent's read end of the child's stdout
};

LiveChild spawnSelfLive(const std::vector<std::string>& args, const std::string& tmpdir,
                        const std::vector<std::string>& extraEnv, const std::string& logPath) {
    LiveChild c;
    const std::string exe = selfExe();
    if (exe.empty()) return c;
    std::vector<std::string> envStore;
    for (char** e = environ; *e != nullptr; ++e) {
        const std::string kv{*e};
        if (kv.rfind("TMPDIR=", 0) == 0 || kv.rfind("RUNE_TEST_KEEP_TMP=", 0) == 0 || kv.rfind("GTEST_", 0) == 0) continue;
        envStore.push_back(kv);
    }
    envStore.push_back("TMPDIR=" + tmpdir);
    for (const auto& kv : extraEnv) envStore.push_back(kv);
    std::vector<char*> envp;
    for (auto& kv : envStore) envp.push_back(kv.data());
    envp.push_back(nullptr);
    std::vector<std::string> argStore{exe};
    for (const auto& a : args) argStore.push_back(a);
    std::vector<char*> argv;
    for (auto& a : argStore) argv.push_back(a.data());
    argv.push_back(nullptr);

    int in[2], out[2];
    if (::pipe(in) != 0) return c;
    if (::pipe(out) != 0) { ::close(in[0]); ::close(in[1]); return c; }
    ::fcntl(in[1], F_SETFD, FD_CLOEXEC);
    ::fcntl(out[0], F_SETFD, FD_CLOEXEC);
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, in[0], STDIN_FILENO);
    posix_spawn_file_actions_adddup2(&fa, out[1], STDOUT_FILENO);
    posix_spawn_file_actions_addopen(&fa, STDERR_FILENO, logPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_addclose(&fa, in[0]);
    posix_spawn_file_actions_addclose(&fa, out[1]);
    pid_t pid = -1;
    const int rc = ::posix_spawn(&pid, exe.c_str(), &fa, nullptr, argv.data(), envp.data());
    posix_spawn_file_actions_destroy(&fa);
    ::close(in[0]);
    ::close(out[1]);
    if (rc != 0) { ::close(in[1]); ::close(out[0]); return c; }
    c.pid = pid; c.inFd = in[1]; c.outFd = out[0];
    return c;
}

// Reads lines from the child's stdout (gtest prints its own banner there) until one starts with "READY "; empty on
// deadline / EOF.
std::string readLine(int fd, int deadlineMs = kChildDeadlineMs);
std::string readReady(int fd) {
    for (int i = 0; i < 64; ++i) {
        const std::string l = readLine(fd);
        if (l.rfind("READY ", 0) == 0) return l;
        if (l.empty()) return {};
    }
    return {};
}
// Reads one line from the child's stdout with a hard poll() deadline; empty on deadline / EOF.
std::string readLine(int fd, int deadlineMs) {
    std::string line;
    pollfd p{fd, POLLIN, 0};
    for (int iter = 0; iter < 4096; ++iter) {
        const int n = ::poll(&p, 1, deadlineMs);
        if (n == 0) return {};
        if (n < 0) { if (errno == EINTR) continue; return {}; }
        char ch;
        const ssize_t got = ::read(fd, &ch, 1);
        if (got == 0) return {};
        if (got < 0) { if (errno == EINTR) continue; return {}; }
        if (ch == '\n') return line;
        line.push_back(ch);
    }
    return {};
}

// Releases a LiveChild (EOF on its stdin) and reaps it with a hard deadline (EOF on its stdout = it is gone).
int releaseAndReap(LiveChild& c) {
    if (c.inFd >= 0) { ::close(c.inFd); c.inFd = -1; }
    bool gone = false;
    pollfd p{c.outFd, POLLIN, 0};
    for (int iter = 0; iter < 4096 && !gone; ++iter) {
        const int n = ::poll(&p, 1, kChildDeadlineMs);
        if (n == 0) break;
        if (n < 0) { if (errno == EINTR) continue; break; }
        char ch;
        const ssize_t got = ::read(c.outFd, &ch, 1);
        if (got == 0) gone = true;
        else if (got < 0 && errno != EINTR) break;
    }
    ::close(c.outFd); c.outFd = -1;
    if (!gone) ::kill(c.pid, SIGKILL);
    int st = 0;
    while (::waitpid(c.pid, &st, 0) < 0 && errno == EINTR) {}
    return gone && WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

pid_t deadPid() {
    const pid_t p = ::fork();
    if (p == 0) ::_exit(0);
    int st = 0;
    if (p > 0) ::waitpid(p, &st, 0);
    return p;
}

std::vector<std::string> rootsIn(const fs::path& d) {
    std::vector<std::string> out;
    std::error_code ec;
    for (fs::directory_iterator it{d, ec}, end; !ec && it != end; it.increment(ec)) {
        const std::string n = it->path().filename().string();
        if (n.rfind("rune_t", 0) == 0) out.push_back(n);
    }
    return out;
}

const char* kSweeperFilter = "--gtest_filter=TempDirHygieneRegression.TheTestTmpRootIsPerProcessRegression";
const char* kHolderFilter  = "--gtest_filter=TempDirHygieneChildHelper.HoldsItsRootUntilReleased";

}  // namespace

// Helper body: plants a sentinel in its own root, prints "READY <root>", blocks until its stdin closes, then checks that
// nothing removed the sentinel meanwhile (a concurrent startup sweep of a LIVE root fails it).
TEST(TempDirHygieneChildHelper, HoldsItsRootUntilReleased) {
    if (std::getenv("RUNE_TMP_HYG_HOLD") == nullptr) GTEST_SKIP() << "runs only as a child of the temp-root attacks";
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName());
    fs::create_directories(root / "seg");
    { std::ofstream{root / "seg" / "sentinel"} << "live"; }
    std::printf("READY %s\n", root.c_str());
    std::fflush(stdout);
    char buf[64];
    while (::read(STDIN_FILENO, buf, sizeof buf) > 0) {}
    EXPECT_TRUE(fs::exists(root / "seg" / "sentinel")) << "a live owner's root was swept under it";
}

// Input bounds + a symlink planted under a dead owner's name: only an exact rune_t<digits> name whose owner is dead is
// swept; a symlink is removed as a link, never followed into its target.
TEST(TempDirHygieneRegression, SweepNameAndTypeBoundsAdversarialRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    const fs::path d = root / "hyg_bounds";
    fs::remove_all(d);
    fs::create_directories(d);
    const pid_t dead1 = deadPid(), dead2 = deadPid(), dead3 = deadPid();
    ASSERT_GT(dead1, 0); ASSERT_GT(dead2, 0); ASSERT_GT(dead3, 0);
    const auto name = [](long pid) { return "rune_t" + std::to_string(pid); };

    fs::create_directories(d / name(dead1) / "a" / "b");
    { std::ofstream{d / name(dead1) / "a" / "b" / "f"} << "x"; }
    { std::ofstream{d / name(dead2)} << "a plain file under a dead owner's name"; }
    const fs::path victim = root / "hyg_victim";
    fs::remove_all(victim);
    fs::create_directories(victim);
    { std::ofstream{victim / "precious"} << "must survive"; }
    fs::create_directory_symlink(victim, d / name(dead3));

    const std::vector<std::string> kept = {
        "rune_t", "rune_t0", "rune_t00", "rune_t-" + std::to_string(dead1), name(dead1) + "x", "x" + name(dead1),
        "rune_T" + std::to_string(dead1), "rune_t99999999999999999999999", "rune_t" + std::string(4096 / 64, '9'),
        name(1), name(::getpid())};
    for (const auto& k : kept) fs::create_directories(d / k / "keep");

    const ChildResult r = runSelf({kSweeperFilter}, d.string(), {}, (root / "hyg_bounds.log").string());
    ASSERT_TRUE(r.exited) << r.error;
    EXPECT_EQ(r.exitCode, 0) << slurp((root / "hyg_bounds.log").string());

    EXPECT_FALSE(fs::exists(fs::symlink_status(d / name(dead1)))) << "a dead owner's root survived the sweep";
    EXPECT_FALSE(fs::exists(fs::symlink_status(d / name(dead2)))) << "a dead owner's plain file survived the sweep";
    EXPECT_FALSE(fs::exists(fs::symlink_status(d / name(dead3)))) << "a dead owner's symlink survived the sweep";
    EXPECT_TRUE(fs::exists(victim / "precious")) << "the sweep followed a symlink and removed its target's content";
    for (const auto& k : kept) EXPECT_TRUE(fs::is_directory(d / k / "keep")) << "touched: " << k;
    std::size_t keptRoots = 0;
    for (const auto& k : kept) keptRoots += k.rfind("rune_t", 0) == 0 ? 1 : 0;
    EXPECT_EQ(rootsIn(d).size(), keptRoots) << "the child's own root was left behind (or a kept entry went)";
    fs::remove_all(d);
    fs::remove_all(victim);
}

// Concurrency: many binaries starting at once in one TMPDIR while others are live - every startup sweeps, none removes
// a live owner's root, every dead one goes.
TEST(TempDirHygieneRegression, ConcurrentStartupsNeverSweepALiveRootAdversarialRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    const fs::path d = root / "hyg_conc";
    fs::remove_all(d);
    fs::create_directories(d);
    constexpr int kHolders = 6, kSweepers = 6;
    std::vector<LiveChild> holders;
    std::vector<std::string> holderRoots;
    for (int i = 0; i < kHolders; ++i) {
        holders.push_back(spawnSelfLive({kHolderFilter}, d.string(), {"RUNE_TMP_HYG_HOLD=1"},
                                        (root / ("hyg_holder" + std::to_string(i) + ".log")).string()));
        ASSERT_GT(holders.back().pid, 0) << "spawn failed";
    }
    for (int i = 0; i < kHolders; ++i) {
        const std::string line = readReady(holders[i].outFd);
        ASSERT_EQ(line.rfind("READY ", 0), 0u) << "holder " << i << " never became ready: '" << line << "'";
        holderRoots.push_back(line.substr(6));
    }
    const pid_t dead = deadPid();
    fs::create_directories(d / ("rune_t" + std::to_string(dead)) / "old");

    std::vector<LiveChild> sweepers;
    for (int i = 0; i < kSweepers; ++i) {
        sweepers.push_back(spawnSelfLive({kSweeperFilter}, d.string(), {},
                                         (root / ("hyg_sweeper" + std::to_string(i) + ".log")).string()));
        ASSERT_GT(sweepers.back().pid, 0);
    }
    for (int i = 0; i < kSweepers; ++i)
        EXPECT_EQ(releaseAndReap(sweepers[i]), 0) << slurp((root / ("hyg_sweeper" + std::to_string(i) + ".log")).string());
    for (int i = 0; i < kHolders; ++i) {
        EXPECT_TRUE(fs::exists(fs::path(holderRoots[i]) / "seg" / "sentinel")) << "live root swept: " << holderRoots[i];
        EXPECT_EQ(releaseAndReap(holders[i]), 0) << slurp((root / ("hyg_holder" + std::to_string(i) + ".log")).string());
        EXPECT_FALSE(fs::exists(holderRoots[i])) << "a holder's root survived its normal exit";
    }
    EXPECT_FALSE(fs::exists(d / ("rune_t" + std::to_string(dead)))) << "no concurrent startup swept the dead root";
    EXPECT_TRUE(rootsIn(d).empty()) << "left behind: " << rootsIn(d).size();
    fs::remove_all(d);
}

// Crash: a SIGKILLed binary runs no atexit - its root stays until the next startup in the same TMPDIR sweeps it.
TEST(TempDirHygieneRegression, ASigkilledBinaryRootIsSweptByTheNextStartupAdversarialRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    const fs::path d = root / "hyg_kill";
    fs::remove_all(d);
    fs::create_directories(d);
    LiveChild h = spawnSelfLive({kHolderFilter}, d.string(), {"RUNE_TMP_HYG_HOLD=1"}, (root / "hyg_kill.log").string());
    ASSERT_GT(h.pid, 0);
    const std::string line = readReady(h.outFd);
    ASSERT_EQ(line.rfind("READY ", 0), 0u) << "holder never became ready: '" << line << "'";
    const fs::path killedRoot = line.substr(6);
    ::kill(h.pid, SIGKILL);
    EXPECT_EQ(releaseAndReap(h), -1);
    ASSERT_TRUE(fs::exists(killedRoot / "seg" / "sentinel")) << "precondition: a SIGKILLed run leaves its root";

    const ChildResult r = runSelf({kSweeperFilter}, d.string(), {}, (root / "hyg_kill_sweep.log").string());
    ASSERT_TRUE(r.exited) << r.error;
    EXPECT_EQ(r.exitCode, 0);
    EXPECT_FALSE(fs::exists(killedRoot)) << "the next startup did not sweep the killed run's root";
    EXPECT_TRUE(rootsIn(d).empty());
    fs::remove_all(d);
}

// fork without exec (gtest's default "fast" death tests, fork helpers): the child inherits the atexit hook; a normal
// exit() in it must not remove the parent's root.
TEST(TempDirHygieneRegression, AForkedChildExitingNormallyKeepsTheOwnersRootAdversarialRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    { std::ofstream{root / "hyg_fork_parent"} << "parent"; }
    const std::string style = GTEST_FLAG_GET(death_test_style);
    GTEST_FLAG_SET(death_test_style, "fast");
    EXPECT_EXIT({ std::ofstream{root / "hyg_fork_child"} << "child"; std::exit(0); }, ::testing::ExitedWithCode(0), "");
    GTEST_FLAG_SET(death_test_style, style);
    // A raw fork too (not every forking helper is a gtest death test).
    const pid_t p = ::fork();
    ASSERT_GE(p, 0);
    if (p == 0) std::exit(0);
    int st = 0;
    ASSERT_EQ(::waitpid(p, &st, 0), p);
    EXPECT_TRUE(fs::is_directory(root)) << "a forked child's exit() removed the owner's root";
    EXPECT_TRUE(fs::exists(root / "hyg_fork_parent"));
    EXPECT_TRUE(fs::exists(root / "hyg_fork_child")) << "the death-test child's file vanished with the root";
    fs::remove(root / "hyg_fork_parent");
    fs::remove(root / "hyg_fork_child");
}

// exec / posix_spawn of a non-gtest program (a broker, a shell): it inherits the root as its TMPDIR, so whatever it
// writes lands under the root too.
TEST(TempDirHygieneRegression, ASpawnedProgramInheritsTheRootAsTmpdirAdversarialRegression) {
    const fs::path root = fs::temp_directory_path();
    ASSERT_EQ(root.filename().string(), ownRootName()) << "precondition: the per-process root is wired";
    int out[2];
    ASSERT_EQ(::pipe(out), 0);
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, out[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&fa, out[0]);
    char sh[] = "/bin/sh", c[] = "-c", cmd[] = "printf '%s\\n' \"$TMPDIR\"";
    char* argv[] = {sh, c, cmd, nullptr};
    pid_t pid = -1;
    const int rc = ::posix_spawn(&pid, "/bin/sh", &fa, nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    ::close(out[1]);
    ASSERT_EQ(rc, 0);
    const std::string got = readLine(out[0]);
    ::close(out[0]);
    int st = 0;
    while (::waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    EXPECT_EQ(fs::path(got), root) << "a spawned program's TMPDIR is not the owner's root";
}
