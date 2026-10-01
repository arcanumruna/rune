#pragma once
// IoBackend.h — the worker's async-durability backend, selected ONCE at
// construction (Task 10 fix 1 / C1).
//
//   RUNE_IO_BACKEND unset   → platform default: Linux = UringFlushBackend (raw
//                             io_uring); elsewhere = PortableFlushBackend.
//   RUNE_IO_BACKEND=uring   → UringFlushBackend (Linux only).
//   RUNE_IO_BACKEND=portable→ the REAL PortableFlushBackend on EVERY platform
//                             (dedicated flusher thread + SPSC rings; completions
//                             through reap()).  Use where io_uring_setup is not
//                             available (Docker Desktop / Rosetta).  This used to
//                             be a no-op on Linux (submit always kNoToken) — never
//                             again: a selection either really submits or is
//                             refused at startup.
//   anything else           → Invalid: acceptsSubmissions() == false.  Shard mode
//                             refuses to start (error names RUNE_IO_BACKEND);
//                             legacy Strict produces get Internal back-pressure.
//
// DESIGN: no vtable on the hot path ([[rune-no-virtual-calls-hot-path]]).  The
// choice is a construction-time kind; submit()/reap() are one well-predicted
// branch to the concrete backend's inline call.
//
// FlushCompletion.flushBoundary is informational only (the portable backend
// echoes the submitted value; io_uring reports 0).  Completions are matched by
// TOKEN only — an entry is stamped with token T after its append, and an
// fdatasync covers every write issued before the submit (I2).

#include <rune/node/PortableFlushBackend.h>   // shared types + PortableFlushBackend

#if defined(__linux__)
#include <rune/node/UringFlushBackend.h>
#endif

#include <cstdlib>
#include <fcntl.h>
#include <optional>
#include <string_view>

namespace rune::node {

enum class IoBackendKind : uint8_t { Uring, Portable, Invalid };

/// The env var that selects the backend (named in startup refusals).
inline constexpr const char* kIoBackendEnv = "RUNE_IO_BACKEND";

[[nodiscard]] inline IoBackendKind selectIoBackendFromEnv() noexcept {
    const char* v = std::getenv(kIoBackendEnv);
    if (v == nullptr || *v == '\0') {
#if defined(__linux__)
        return IoBackendKind::Uring;
#else
        return IoBackendKind::Portable;
#endif
    }
    const std::string_view s(v);
    if (s == "portable") return IoBackendKind::Portable;
#if defined(__linux__)
    if (s == "uring") return IoBackendKind::Uring;
#endif
    return IoBackendKind::Invalid;
}

class IoBackend {
public:
    IoBackend(std::size_t submitDepth, std::size_t completionDepth)
        : kind_(selectIoBackendFromEnv()) {
        switch (kind_) {
        case IoBackendKind::Portable: portable_.emplace(submitDepth, completionDepth); break;
#if defined(__linux__)
        case IoBackendKind::Uring:    uring_.emplace(submitDepth, completionDepth); break;
#endif
        default: break;
        }
    }
    IoBackend(const IoBackend&)            = delete;
    IoBackend& operator=(const IoBackend&) = delete;

    /// Returns kNoToken on back-pressure (in-flight depth reached) or when the
    /// selected backend cannot submit (Invalid).
    [[nodiscard]] CompletionToken submit(int fd, int64_t flushBoundary) noexcept {
#if defined(__linux__)
        if (kind_ == IoBackendKind::Uring) [[likely]] return uring_->submit(fd, flushBoundary);
#endif
        if (kind_ == IoBackendKind::Portable) return portable_->submit(fd, flushBoundary);
        return kNoToken;
    }

    /// F15 S4 fix round 2 (RR1-4): true when read-ahead hints can be issued
    /// asynchronously (io_uring IORING_OP_FADVISE); else the caller uses a
    /// synchronous hint (counted).
    [[nodiscard]] bool asyncHints() const noexcept {
#if defined(__linux__)
        // RR2-1: only where the kernel supports IORING_OP_FADVISE (probed).
        return kind_ == IoBackendKind::Uring && uring_->fadviseSupported();
#else
        return false;
#endif
    }
    /// An async POSIX_FADV_WILLNEED hint on [off, off + len) of fd: a token
    /// (completion reaped like a flush — route it), or kNoToken (SQ full / no
    /// async hints: skip this pass).
    [[nodiscard]] CompletionToken submitWillNeed(int fd, int64_t off, uint32_t len) noexcept {
#if defined(__linux__)
        if (kind_ == IoBackendKind::Uring) return uring_->submitFadvise(fd, off, len, POSIX_FADV_WILLNEED);
#endif
        (void)fd; (void)off; (void)len;
        return kNoToken;
    }

    template <class H>
    int reap(H&& handler) noexcept {
#if defined(__linux__)
        if (kind_ == IoBackendKind::Uring) [[likely]] return uring_->reap(std::forward<H>(handler));
#endif
        if (kind_ == IoBackendKind::Portable) return portable_->reap(std::forward<H>(handler));
        return 0;
    }

    [[nodiscard]] bool acceptsSubmissions() const noexcept { return kind_ != IoBackendKind::Invalid; }
    [[nodiscard]] IoBackendKind kind() const noexcept { return kind_; }
    [[nodiscard]] const char* name() const noexcept {
        switch (kind_) {
        case IoBackendKind::Uring:    return "uring";
        case IoBackendKind::Portable: return "portable";
        default:                      return "invalid";
        }
    }

private:
    IoBackendKind                       kind_;
    std::optional<PortableFlushBackend> portable_;
#if defined(__linux__)
    std::optional<UringFlushBackend>    uring_;
#endif
};

} // namespace rune::node
