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
#include <rune/consensus/IdleStrategy.h>

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
    : submitRing_(detail::flushRingByteCapacity(submitDepth, kFlushRequestWireLen))
    , completionRing_(detail::flushRingByteCapacity(completionDepth, kFlushCompletionWireLen))
    , nextToken_(0)
    , submitDepth_(submitDepth)
    , inFlight_(0)
    , stop_(false)
    , flusher_([this]{ flusherLoop_(); })
{
}

PortableFlushBackend::~PortableFlushBackend() {
    // Task #12 fix: signal the flusher to stop via the atomic stop_ flag —
    // NOT by writing a sentinel to the submit ring.  The submit ring is a
    // single-producer/single-consumer ring whose sole producer is the worker's
    // submit(); if the dtor (which may run on a DIFFERENT thread than the worker)
    // also wrote to it, that would be a two-producer data race on the ring's
    // producer position (TSan-flagged).  A release-store here pairs with the
    // flusher's acquire-load; join() then establishes happens-before for teardown.
    stop_.store(true, std::memory_order_release);
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
    // read-rework SBE-C: SBE wrap-in-place encode directly into the claimed slot
    // (no intermediate struct/memcpy).  tryClaim < 0 = back-pressure.
    const auto idx = submitRing_.tryClaim(flushsbe::FlushRequest::sbeTemplateId(),
                                          kFlushRequestWireLen);
    if (idx < 0) {
        // Byte ring is full (shouldn't happen if depth is respected, but guard
        // anyway).  Roll back the token sequence.
        --nextToken_;
        return kNoToken;
    }
    flushsbe::FlushRequest enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(submitRing_.base() + idx), 0,
                           kFlushRequestWireLen);
    enc.fd(fd).flushBoundary(flushBoundary).token(static_cast<int64_t>(tok));
    submitRing_.commit(idx);
    ++inFlight_;
    return tok;
}

void PortableFlushBackend::flusherLoop_() {
    // The flusher loop: drain submit ring → fdatasync → post completion.
    // Runs until the dtor sets the atomic stop_ flag (acquire-load below pairs
    // with the dtor's release-store).  No sentinel: submit() is the ring's sole
    // producer, so the dtor never writes to it (Task #12).
    // Idle strategy for the flusher: when the submit ring is empty, back off
    // (spin → yield → progressively longer parks) instead of a raw
    // std::this_thread::yield() spin, which pegged a whole core PER WORKER even
    // at zero load (one flusher thread per worker → N cores burned idle,
    // starving the Aeron driver thread of CPU).  Under continuous load drain()
    // returns >0 every iteration, which resets the strategy — so an fsync
    // submitted while traffic is flowing is still picked up with no park delay;
    // parks only accrue during a genuine lull, where latency is irrelevant.
    // ("always use the backoff aeron idle strategy".)
    rune::consensus::BackoffIdleStrategy idle;

    while (!stop_.load(std::memory_order_acquire)) {
        const int drained = submitRing_.drain(
            [this](int32_t /*msgTypeId*/,
                   aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t offset,
                   aeron::util::index_t length) {
                // read-rework SBE-C: SBE-decode the request in place (zero-copy).
                char* rp = reinterpret_cast<char*>(buf.buffer() + offset);
                const std::uint64_t rcap = static_cast<std::uint64_t>(length);
                flushsbe::MessageHeader rhdr;
                rhdr.wrap(rp, 0, 0, rcap);
                flushsbe::FlushRequest req;
                req.wrapForDecode(rp, flushsbe::MessageHeader::encodedLength(),
                                  rhdr.blockLength(), rhdr.version(), rcap);
                const int             reqFd    = req.fd();
                const int64_t         reqBound = req.flushBoundary();
                const CompletionToken reqTok   = static_cast<CompletionToken>(req.token());

                // Perform the datasync (may block — intentional; off the worker path).
                int rc = rune_fdatasync(reqFd);
                int saved_errno = (rc != 0) ? errno : 0;
                const int32_t ec = (rc == 0) ? 0 : saved_errno;

                // SBE wrap-in-place encode of the completion into the claimed slot.
                // The flusher may block (off worker path); the completion ring drains
                // on the worker's next reap() call.  Spin until a slot frees.
                for (;;) {
                    const auto idx =
                        completionRing_.tryClaim(flushsbe::FlushCompletion::sbeTemplateId(),
                                                 kFlushCompletionWireLen);
                    if (idx >= 0) {
                        flushsbe::FlushCompletion cenc;
                        cenc.wrapAndApplyHeader(
                            reinterpret_cast<char*>(completionRing_.base() + idx), 0,
                            kFlushCompletionWireLen);
                        cenc.token(static_cast<int64_t>(reqTok))
                            .flushBoundary(reqBound)
                            .errorCode(ec);
                        completionRing_.commit(idx);
                        break;
                    }
                    std::this_thread::yield();
                }
            },
            /*limit=*/16);

        if (!stop_.load(std::memory_order_acquire)) {
            idle.idle(drained); // parks when idle; resets to spin when draining
        }
    }

    // Teardown final drain: complete any requests enqueued before stop_ so their
    // data is durable on graceful shutdown.  We do the fdatasync but do NOT post
    // completions — the worker has stopped reaping, so writing to the (possibly
    // full) completion ring could spin forever and hang join().
    submitRing_.drain(
        [](int32_t /*msgTypeId*/,
           aeron::concurrent::AtomicBuffer& buf,
           aeron::util::index_t offset,
           aeron::util::index_t /*length*/) {
            const FlushRequest* req =
                reinterpret_cast<const FlushRequest*>(buf.buffer() + offset);
            (void)rune_fdatasync(req->fd);
        },
        /*limit=*/1 << 30);
}

} // namespace rune::node
