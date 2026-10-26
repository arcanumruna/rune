#pragma once
// Test rig: run a body in a forked child that owns a tiny private tmpfs, so a
// write fault on a full filesystem is deterministic.  The child unshares a user
// + mount namespace (no root needed where unprivileged user namespaces are
// allowed) and mounts a `sizeBytes` tmpfs at a fresh directory.
//
// Child exit status: 0 = body passed, 1 = body failed, 77 = rig unavailable
// (not Linux, or user namespaces are blocked) → the test skips.  A signal
// (SIGBUS from a write into an unbacked mapped page) is reported as a failure.
#include <gtest/gtest.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__linux__)
#include <sched.h>
#include <sys/mount.h>
#endif

namespace rune::test {

#if defined(__linux__)
inline bool writeProcFile_(const char* path, const std::string& s) {
    const int fd = ::open(path, O_WRONLY);
    if (fd < 0) return false;
    const bool ok = ::write(fd, s.data(), s.size()) == static_cast<ssize_t>(s.size());
    ::close(fd);
    return ok;
}
#endif

/// Fills the filesystem holding `dir` until a write answers ENOSPC.
inline bool fillDisk(const std::string& dir) {
    const std::string p = dir + "/filler";
    const int fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    char buf[4096] = {};
    for (int i = 0; i < 1 << 20; ++i) {
        if (::write(fd, buf, sizeof buf) < 0) { const int e = errno; ::close(fd); return e == ENOSPC; }
    }
    ::close(fd);
    return false;   // never filled: the cap did not hold
}

/// Runs `body(dir)` in a child on a `sizeBytes` private tmpfs at `dir`.
/// `body` returns true on pass.  Skips the calling test when the rig is unavailable.
template <class Body>
void runOnTinyTmpfs(std::size_t sizeBytes, Body body) {
#if !defined(__linux__)
    (void)sizeBytes; (void)body;
    GTEST_SKIP() << "tiny-tmpfs rig is Linux-only";
#else
    char tmpl[] = "/tmp/rune_diskfull_XXXXXX";
    if (::mkdtemp(tmpl) == nullptr) GTEST_SKIP() << "mkdtemp failed";
    const std::string dir = tmpl;
    std::fflush(nullptr);
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        const uid_t uid = ::getuid();
        const gid_t gid = ::getgid();
        if (::unshare(CLONE_NEWUSER | CLONE_NEWNS) != 0) ::_exit(77);
        (void)writeProcFile_("/proc/self/setgroups", "deny");
        if (!writeProcFile_("/proc/self/uid_map", "0 " + std::to_string(uid) + " 1")) ::_exit(77);
        if (!writeProcFile_("/proc/self/gid_map", "0 " + std::to_string(gid) + " 1")) ::_exit(77);
        if (::mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr) != 0) ::_exit(77);
        const std::string opts = "size=" + std::to_string(sizeBytes);
        if (::mount("tmpfs", dir.c_str(), "tmpfs", 0, opts.c_str()) != 0) ::_exit(77);
        ::_exit(body(dir) ? 0 : 1);
    }
    int st = 0;
    while (::waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    ::rmdir(dir.c_str());
    if (WIFEXITED(st) && WEXITSTATUS(st) == 77) GTEST_SKIP() << "user/mount namespaces unavailable here";
    ASSERT_FALSE(WIFSIGNALED(st)) << "child died on signal " << WTERMSIG(st)
                                  << " (SIGBUS=" << SIGBUS << "): a write hit an unbacked mapped page";
    ASSERT_TRUE(WIFEXITED(st));
    EXPECT_EQ(WEXITSTATUS(st), 0) << "body assertions failed in the child (see its stderr)";
#endif
}

} // namespace rune::test
