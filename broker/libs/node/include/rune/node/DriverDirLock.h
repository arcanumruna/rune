#pragma once
// DriverDirLock.h — exclusive ownership of an embedded Aeron driver's directory.
//
// An embedded driver owns its Aeron dir through an exclusive, non-blocking flock(2) on a lock file NEXT TO the dir
// (<canonical-dir>.lock; every spelling of the dir -- trailing slash, "/.", a symlink alias -- shares one lock).  Not inside the dir: dir_delete_on_start wipes the dir, so a lock file
// inside it could be deleted and recreated under a live holder.  The kernel drops the lock when the holder dies
// (SIGKILL, docker kill), so a restart right after a crash always succeeds; a live holder, in another process or
// another driver object in this process (flock is per open file description), is always refused, whatever its
// Aeron heartbeat age.  A live driver that holds NO lock (a pre-lock version, a stock aeronmd, a raw C-API driver)
// is still refused, from the pid in its cnc.dat; a stale heartbeat of a dead pid is reclaimed.  Move-only; the destructor closes the fd and so releases the lock.
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

#include <aeron/aeronc.h>
#include <aeronmd/aeronmd.h>
#include <csignal>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/file.h>
#include <unistd.h>

namespace rune::node {

class DriverDirLock {
public:
    /// The pid of a live Aeron driver on `canonDir` that is NOT covered by a lock we hold, or 0 when the dir has no
    /// live driver (absent, or only the last heartbeat stamp of a dead holder).  -1 = live but its pid is
    /// unreadable.  Call it after taking the lock: a lock-holding driver is then impossible, so any live heartbeat
    /// belongs to a driver that never took one (or to this very process).
    [[nodiscard]] static std::int64_t liveDriverPidWithoutLock(const std::string& canonDir) noexcept {
        if (!aeron_is_driver_active(canonDir.c_str(), 1000, [](const char*) {})) return 0;
        // Public CnC reader (non-blocking, validates the CnC version): unreadable or foreign layout counts as live.
        aeron_cnc_t* cnc = nullptr;
        if (aeron_cnc_init(&cnc, canonDir.c_str(), 0) < 0) return -1;
        aeron_cnc_constants_t md;
        const int crc = aeron_cnc_constants(cnc, &md);
        aeron_cnc_close(cnc);
        if (crc < 0) return -1;
        const std::int64_t pid = md.pid;
        if (pid <= 0) return -1;
        if (pid == static_cast<std::int64_t>(::getpid())) return pid;
        if (::kill(static_cast<pid_t>(pid), 0) == 0 || errno == EPERM) return pid;
        return 0;   // ESRCH: the heartbeat is the last stamp of a dead holder
    }

    /// nullopt + `err` when the dir is held by a live owner ("in use") or the lock file cannot be taken.
    [[nodiscard]] static std::optional<DriverDirLock> tryAcquire(const std::string& dir, std::string& err) {
        std::error_code ec;
        std::filesystem::path canon = std::filesystem::weakly_canonical(std::filesystem::path(dir), ec);
        if (ec) {
            err = "cannot resolve driver dir '" + dir + "': " + ec.message();
            return std::nullopt;
        }
        // "dir/" and "dir/." end in an empty / "." component: drop them so one dir has one lock path.
        while (canon.has_parent_path() && (!canon.has_filename() || canon.filename() == ".")) canon = canon.parent_path();
        const std::string canonDir = canon.string();
        std::string path = canonDir + ".lock";
        const std::filesystem::path parent = std::filesystem::path(path).parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
        int fd;
        // O_NOFOLLOW: a symlink planted at the lock path is refused, never followed to truncate-free-create or
        // lock some other file.
        do { fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600); } while (fd < 0 && errno == EINTR);
        if (fd < 0) {
            err = "cannot open driver dir lock '" + path + "': " + std::strerror(errno) +
                  (ec ? " (create_directories: " + ec.message() + ")" : std::string{});
            return std::nullopt;
        }
        int rc;
        do { rc = ::flock(fd, LOCK_EX | LOCK_NB); } while (rc < 0 && errno == EINTR);
        if (rc < 0) {
            const int e = errno;
            ::close(fd);
            err = e == EWOULDBLOCK ? "in use: driver dir lock '" + path + "' is held by a live driver"
                                   : "cannot lock driver dir lock '" + path + "': " + std::strerror(e);
            return std::nullopt;
        }
        if (const std::int64_t pid = liveDriverPidWithoutLock(canonDir); pid != 0) {
            ::close(fd);   // releases the lock
            err = "in use: driver dir '" + canonDir + "' is in use by a live media driver" +
                  (pid > 0 ? " (pid " + std::to_string(pid) + ")" : std::string{}) + " that holds no dir lock";
            return std::nullopt;
        }
        return DriverDirLock(fd, std::move(path));
    }

    DriverDirLock(DriverDirLock&& o) noexcept : fd_(std::exchange(o.fd_, -1)), path_(std::move(o.path_)) {}
    DriverDirLock& operator=(DriverDirLock&& o) noexcept {
        if (this != &o) { release_(); fd_ = std::exchange(o.fd_, -1); path_ = std::move(o.path_); }
        return *this;
    }
    DriverDirLock(const DriverDirLock&)            = delete;
    DriverDirLock& operator=(const DriverDirLock&) = delete;
    ~DriverDirLock() { release_(); }

    [[nodiscard]] const std::string& path() const noexcept { return path_; }

private:
    DriverDirLock(int fd, std::string path) noexcept : fd_(fd), path_(std::move(path)) {}
    void release_() noexcept { if (fd_ >= 0) { ::close(fd_); fd_ = -1; } }
    int         fd_;
    std::string path_;
};

} // namespace rune::node
