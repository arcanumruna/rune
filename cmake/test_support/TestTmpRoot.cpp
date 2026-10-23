// Per-process temp root for every gtest binary (issue #375).
//
// Linked into each gtest executable by cmake/RuneTestTmpRoot.cmake; nothing includes it and no product code depends
// on it.  Before main() it:
//   1. sweeps <TMPDIR>/rune_t<pid> roots whose owner process is dead (a SIGKILLed / timed-out / crashed run);
//   2. creates <TMPDIR>/rune_t<getpid()> and points TMPDIR at it, so every std::filesystem::temp_directory_path()
//      in the binary (and in the processes it starts) lands under one root;
//   3. removes that root at normal process exit (atexit), pass or fail, only in the owning process: a forked child
//      (death test, fork helper) has another pid and leaves it alone.
// No signal handlers: a killed run's root is swept by the next test binary that starts in the same TMPDIR.
// RUNE_TEST_KEEP_TMP (any value) keeps the root after exit, for debugging.
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;

constexpr const char* kPrefix = "rune_t";

// "rune_t<digits>" exactly -> pid, else -1.
long ownerPidOf(const std::string& name) {
    const std::string prefix{kPrefix};
    if (name.size() <= prefix.size() || name.compare(0, prefix.size(), prefix) != 0) return -1;
    long pid = 0;
    for (std::size_t i = prefix.size(); i < name.size(); ++i) {
        const char c = name[i];
        if (c < '0' || c > '9') return -1;
        if (pid > 100'000'000L) return -1;  // far beyond any pid_t in use; also guards overflow
        pid = pid * 10 + (c - '0');
    }
    return pid > 0 ? pid : -1;
}

void sweepDeadRoots(const fs::path& base, long self) {
    std::error_code ec;
    fs::directory_iterator it{base, ec};
    if (ec) return;
    for (const fs::directory_iterator end; it != end; it.increment(ec)) {
        if (ec) return;
        const long pid = ownerPidOf(it->path().filename().string());
        if (pid <= 0 || pid == self) continue;
        if (::kill(static_cast<pid_t>(pid), 0) == -1 && errno == ESRCH) {
            std::error_code rmEc;
            fs::remove_all(it->path(), rmEc);
        }
    }
}

struct TestTmpRoot {
    static inline std::string root;
    static inline pid_t ownerPid = 0;
    static inline bool keep = false;

    TestTmpRoot() {
        std::error_code ec;
        const fs::path base = fs::temp_directory_path(ec);
        if (ec) {
            std::fprintf(stderr, "rune test tmp root: no temp directory (%s); TMPDIR unchanged\n",
                         ec.message().c_str());
            return;
        }
        const pid_t self = ::getpid();
        sweepDeadRoots(base, static_cast<long>(self));
        const fs::path dir = base / (std::string{kPrefix} + std::to_string(static_cast<long>(self)));
        fs::create_directories(dir, ec);
        if (ec || ::setenv("TMPDIR", dir.c_str(), 1) != 0) {
            std::fprintf(stderr, "rune test tmp root: cannot create %s (%s); TMPDIR unchanged\n", dir.c_str(),
                         ec ? ec.message().c_str() : "setenv failed");
            return;
        }
        root     = dir.string();
        ownerPid = self;
        keep     = std::getenv("RUNE_TEST_KEEP_TMP") != nullptr;
        std::atexit(&TestTmpRoot::removeAtExit);
    }

    static void removeAtExit() {
        if (keep || root.empty() || ::getpid() != ownerPid) return;
        std::error_code ec;
        fs::remove_all(root, ec);
    }
};

const TestTmpRoot gTestTmpRoot;

}  // namespace
