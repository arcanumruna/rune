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
