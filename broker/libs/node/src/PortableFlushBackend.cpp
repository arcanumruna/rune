// PortableFlushBackend.cpp — E3: flusher thread implementation.
//
// See PortableFlushBackend.h for the design notes and thread-ownership rules.
//
// Ring encoding (both rings, msgTypeId=1):
//   write:  memcpy the trivially-copyable POD struct into a stack scratch buffer,
//           then OneToOneByteRing::write(scratch, sizeof(T), 1).
//   read:   agrona zero-copy view — reinterpret_cast<const T*>(buf.buffer()+off).
//
// Back-pressure:
//   submit() enforces inFlight_ < submitDepth_ (worker-side only, no synchronisation)
//   before calling write() — this preserves the depth-as-slot-count semantic of the
//   old SpscRing<FlushRequest> irrespective of the agrona ring's byte capacity.
//   reap() decrements inFlight_ for each reaped completion (also worker-side only).

#include <rune/node/PortableFlushBackend.h>

#include <cerrno>
#include <cstring>
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
    : submitRing_(detail::flushRingByteCapacity(submitDepth, sizeof(FlushRequest)))
    , completionRing_(detail::flushRingByteCapacity(completionDepth, sizeof(FlushCompletion)))
    , nextToken_(0)
    , submitDepth_(submitDepth)
    , inFlight_(0)
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
    uint8_t buf[sizeof(FlushRequest)];
    std::memcpy(buf, &sentinel, sizeof(FlushRequest));
    while (!submitRing_.write(buf, sizeof(FlushRequest), /*msgTypeId=*/1)) {
        std::this_thread::yield();
    }
    flusher_.join();
}

CompletionToken PortableFlushBackend::submit(int fd,
                                              int64_t flushBoundary) noexcept {
    // Back-pressure: honour the depth limit (same semantic as SpscRing<T>(depth)).
    // inFlight_ is worker-owned — no synchronisation needed.
    if (inFlight_ >= submitDepth_) {
        return kNoToken;
    }

    const CompletionToken tok = ++nextToken_;
    FlushRequest req{};
    req.fd            = fd;
    req.flushBoundary = flushBoundary;
    req.token         = tok;
    uint8_t buf[sizeof(FlushRequest)];
    std::memcpy(buf, &req, sizeof(FlushRequest));
    if (!submitRing_.write(buf, sizeof(FlushRequest), /*msgTypeId=*/1)) {
        // Byte ring is full (shouldn't happen if depth is respected, but guard
        // anyway).  Roll back the token sequence.
        --nextToken_;
        return kNoToken;
    }
    ++inFlight_;
    return tok;
}

void PortableFlushBackend::flusherLoop_() {
    // The flusher loop: drain submit ring → fdatasync → post completion.
    // Loops until a sentinel (fd == -1) is seen.
    while (!stop_) {
        submitRing_.drain(
            [this](int32_t /*msgTypeId*/,
                   aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t offset,
                   aeron::util::index_t /*length*/) {
                // Zero-copy view into the ring.
                const FlushRequest* req =
                    reinterpret_cast<const FlushRequest*>(buf.buffer() + offset);

                if (req->fd == -1) {
                    // Sentinel: the dtor wants us to stop.
                    stop_ = true;
                    return;
                }

                // Perform the datasync (may block — intentional; off the worker path).
                int rc = rune_fdatasync(req->fd);
                int saved_errno = (rc != 0) ? errno : 0;

                FlushCompletion comp{};
                comp.token         = req->token;
                comp.flushBoundary = req->flushBoundary;
                comp.errorCode     = (rc == 0) ? 0 : saved_errno;

                // Encode the completion and spin-enqueue it.
                // The flusher may block (off worker path); the completion ring
                // will drain on the worker's next reap() call.
                uint8_t compBuf[sizeof(FlushCompletion)];
                std::memcpy(compBuf, &comp, sizeof(FlushCompletion));
                while (!completionRing_.write(compBuf, sizeof(FlushCompletion),
                                               /*msgTypeId=*/1)) {
                    std::this_thread::yield();
                }
            },
            /*limit=*/16);

        if (!stop_) {
            std::this_thread::yield();
        }
    }
}

} // namespace rune::node
