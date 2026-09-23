// PortableFlushBackend.cpp — E3: flusher thread implementation.
//
// See PortableFlushBackend.h for the design notes and thread-ownership rules.

#include <rune/node/PortableFlushBackend.h>

#include <cerrno>
#include <thread>

#include <unistd.h>
// fdatasync is POSIX but not available on macOS; fall back to fsync.
#if defined(__APPLE__)
static int rune_fdatasync(int fd) { return ::fsync(fd); }
#else
static int rune_fdatasync(int fd) { return ::fdatasync(fd); }
#endif

namespace rune::node {

PortableFlushBackend::PortableFlushBackend(std::size_t submitDepth,
                                           std::size_t completionDepth)
    : submitRing_(submitDepth)
    , completionRing_(completionDepth)
    , nextToken_(0)
    , stop_(false)
    , flusher_([this]{ flusherLoop_(); })
{
}

PortableFlushBackend::~PortableFlushBackend() {
    // Enqueue a sentinel FlushRequest{fd=-1} to signal the flusher to stop.
    // Spin until the ring has space — the flusher is draining continuously.
    FlushRequest sentinel{};
    sentinel.fd    = -1;
    sentinel.token = 0;
    while (!submitRing_.tryEnqueue(sentinel)) {
        std::this_thread::yield();
    }
    flusher_.join();
}

CompletionToken PortableFlushBackend::submit(int fd,
                                              int64_t flushBoundary) noexcept {
    const CompletionToken tok = ++nextToken_;
    FlushRequest req{};
    req.fd            = fd;
    req.flushBoundary = flushBoundary;
    req.token         = tok;
    if (!submitRing_.tryEnqueue(req)) {
        // Back-pressure: ring full.  Roll back so the sequence stays
        // contiguous for the next successful submit.
        --nextToken_;
        return kNoToken;
    }
    return tok;
}

void PortableFlushBackend::flusherLoop_() {
    // The flusher loop: drain submit ring → fdatasync → post completion.
    // Loops until a sentinel (fd == -1) is seen.
    while (!stop_) {
        submitRing_.drain([this](FlushRequest&& req) {
            if (req.fd == -1) {
                // Sentinel: the dtor wants us to stop.
                stop_ = true;
                return;
            }

            // Perform the datasync (may block — intentional; off the worker path).
            int rc = rune_fdatasync(req.fd);
            int saved_errno = (rc != 0) ? errno : 0;

            FlushCompletion comp{};
            comp.token         = req.token;
            comp.flushBoundary = req.flushBoundary;
            comp.errorCode     = (rc == 0) ? 0 : saved_errno;

            // Spin-enqueue completion — the flusher may block, the completion
            // ring will drain on the worker's next reap() call.
            while (!completionRing_.tryEnqueue(comp)) {
                std::this_thread::yield();
            }
        });

        if (!stop_) {
            std::this_thread::yield();
        }
    }
}

} // namespace rune::node
