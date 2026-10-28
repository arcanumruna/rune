#pragma once
// DriverDirLock.h — exclusive ownership of an embedded Aeron driver's directory.
//
// An embedded driver owns its Aeron dir through an exclusive, non-blocking flock(2) on a lock file NEXT TO the dir
// (<dir-without-trailing-slash>.lock).  Not inside the dir: dir_delete_on_start wipes the dir, so a lock file
// inside it could be deleted and recreated under a live holder.  The kernel drops the lock when the holder dies
// (SIGKILL, docker kill), so a restart right after a crash always succeeds; a live holder, in another process or
// another driver object in this process (flock is per open file description), is always refused, whatever its
// Aeron heartbeat age.  Move-only; the destructor closes the fd and so releases the lock.
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace rune::client::transport {

class DriverDirLock {
public:
    /// nullopt + `err` when the dir is held by a live owner ("in use") or the lock file cannot be taken.
    [[nodiscard]] static std::optional<DriverDirLock> tryAcquire(const std::string& dir, std::string& err) {
        std::string base = dir;
        while (base.size() > 1 && base.back() == '/') base.pop_back();
        std::string path = base + ".lock";
        std::error_code ec;
        const std::filesystem::path parent = std::filesystem::path(path).parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
        int fd;
        do { fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600); } while (fd < 0 && errno == EINTR);
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

} // namespace rune::client::transport
