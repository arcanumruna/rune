#pragma once
// PortableFlushBackend.h — E3: portable async-durability backend.
//
// Also defines the shared I/O types (FlushRequest, FlushCompletion,
// CompletionToken, kNoToken) that are used by ALL backends.  IoBackend.h
// includes this file before adding the platform alias.
//
// A dedicated flusher std::thread communicates with the worker ONLY via two
// OneToOneByteRings (submit + completion).  No shared mutable state, no mutex,
// no condvar, no thread_local on the hot path ([[rune-shared-nothing-law]]).
//
// The flusher thread MAY block in ::fdatasync — it is off the worker's path.
// The worker-facing submit() and reap() are non-blocking memory ops (agrona
// OneToOne ring reads/writes) with NO allocation on the hot path.
//
// Back-pressure:
//   submit() returns kNoToken when the submit ring is full — the caller must
//   hold the produce and retry next cycle (never drops, never blocks).
//
// Lifecycle:
//   - ctor: spawns the flusher thread.
//   - dtor: posts a Stop command on the dedicated stopRing_ (F9a: no atomic
//     flag) and joins the thread.  The dtor NEVER writes to the submit ring —
//     that ring stays strictly single-producer (only the worker's submit() ever
//     writes it), so there is no two-producer data race between the worker and
//     the destroying thread (Task #12 fix).
//
// Thread ownership:
//   - submit(), nextToken_, inFlight_, and submitDepth_ are WORKER-side (only
//     the worker calls submit/reap).
//   - The flusher thread owns the submit ring's consumer side and the
//     completion ring's producer side.
//   - reap() is WORKER-side (only the worker calls reap, draining completions).
//
// Back-pressure accounting:
//   inFlight_ tracks the number of submitted-but-not-yet-reaped FlushRequests.
//   submit() enforces inFlight_ < submitDepth_ as the ring-full check — this
//   preserves the depth-as-message-count semantic from SpscRing (independent of
//   the agrona ring byte capacity, which must satisfy maxMsgLength >= sizeof(T)).
//   reap() decrements inFlight_ for each completion drained.  Both accesses are
//   on the worker thread only — no shared state.
//
// Ring sizing:
//   submitDepth / completionDepth are in MESSAGES.  The ctor computes the
//   required byte capacity: nextPow2(max(depth*(sizeof(T)+8), 8*sizeof(T))).
//   This satisfies agrona's maxMsgLength >= sizeof(T) requirement (maxMsgLength
//   = capacity/8) while fitting at least `depth` messages byte-wise.

#include <rune/node/OneToOneByteRing.h>
#include <rune/consensus/ControlRing.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/FlushRequest.h>
#include <rune_protocol_internal_sbe/FlushCompletion.h>
#include <rune_protocol_internal_sbe/WriteSyncRequest.h>
#include <cstdint>
#include <cstring>
#include <thread>

#include <rune/consensus/IdleStrategy.h>
#include <flight_recorder/FlightRecorder.h>

#include <cerrno>
#include <unistd.h>

namespace rune::node {

// read-rework SBE-C: encoded wire sizes of the Flush ring messages (SBE header +
// fixed block) — used for ring sizing + tryClaim reserves.
namespace flushsbe = rune::protocol::internal::sbe;
inline constexpr std::size_t kFlushRequestWireLen =
    flushsbe::MessageHeader::encodedLength() + flushsbe::FlushRequest::sbeBlockLength();
inline constexpr std::size_t kFlushCompletionWireLen =
    flushsbe::MessageHeader::encodedLength() + flushsbe::FlushCompletion::sbeBlockLength();
/// E1d: a write+fdatasync request (same submit ring, template id 7).
inline constexpr std::size_t kWriteSyncWireLen =
    flushsbe::MessageHeader::encodedLength() + flushsbe::WriteSyncRequest::sbeBlockLength();
/// Largest inline block a write+fdatasync request carries.
inline constexpr std::size_t kWriteSyncMaxBlock = flushsbe::WriteSyncRequest::blockLength();
/// E1d MEDIUM-2: a FULL fsync request (a directory) — the FlushRequest body
/// under its own ring record type (the record type is the ring's, not the SBE
/// template id), so no schema change: the flusher calls ::fsync, never the
/// data-sync primitive.
inline constexpr std::int32_t kFullFsyncRecordType = 1001;
inline constexpr std::size_t kSubmitRingMaxWireLen =
    kWriteSyncWireLen > kFlushRequestWireLen ? kWriteSyncWireLen : kFlushRequestWireLen;

// ---------------------------------------------------------------------------
// Shared I/O backend types
// ---------------------------------------------------------------------------

using CompletionToken = uint64_t;
inline constexpr CompletionToken kNoToken = 0;   // back-pressure: submit ring full

struct FlushRequest {
    int fd                = -1;
    int64_t flushBoundary = 0;
    CompletionToken token = 0;
};

struct FlushCompletion {
    CompletionToken token = 0;
    int32_t errorCode     = 0;   // 0 = OK, else errno
    int64_t flushBoundary = 0;
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------
namespace detail {

/// Compute the OneToOneByteRing payload capacity (bytes, power-of-two) needed
/// to hold at least `depth` messages of `msgSize` bytes each.
///
/// agrona header per record = 8 bytes (4 length + 4 type).
/// maxMsgLength = capacity/8, so capacity must be >= 8 * msgSize.
/// Combined: capacity = nextPow2(max(depth*(msgSize+8), 8*msgSize)).
inline int flushRingByteCapacity(std::size_t depth, std::size_t msgSize) {
    const std::size_t slotsNeeded = depth * (msgSize + 8u);
    const std::size_t maxMsgNeeded = 8u * msgSize;
    std::size_t n = std::max(slotsNeeded, maxMsgNeeded);
    // Round up to next power of two (minimum 64 bytes for agrona).
    if (n < 64u) n = 64u;
    std::size_t p = 1u;
    while (p < n) p <<= 1u;
    return static_cast<int>(p);
}

} // namespace detail

// ---------------------------------------------------------------------------
// BasicPortableFlushBackend<Sync> / PortableFlushBackend
//
// `Sync` is the data-sync primitive the flusher calls — a COMPILE-TIME seam
// (template parameter, F9a fix round 1): production uses the platform
// fdatasync (fsync on macOS) as a direct call; tests instantiate the template
// with a recording function.  No runtime indirect call on the flush path.
// ---------------------------------------------------------------------------
using PortableSyncFn = int (*)(int fd);

namespace detail {
// fdatasync is POSIX but not available on macOS; fall back to fsync.
inline int platformFdatasync(int fd) {
#if defined(__APPLE__)
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}
// SBE-decode one submit-ring slot (zero-copy view).  The ONE decoder for both
// the duty loop and the teardown drain.
inline FlushRequest decodeFlushRequest(aeron::concurrent::AtomicBuffer& buf,
                                aeron::util::index_t offset,
                                aeron::util::index_t length) noexcept {
    char* rp = reinterpret_cast<char*>(buf.buffer() + offset);
    const std::uint64_t rcap = static_cast<std::uint64_t>(length);
    flushsbe::MessageHeader rhdr;
    rhdr.wrap(rp, 0, 0, rcap);
    flushsbe::FlushRequest req;
    req.wrapForDecode(rp, flushsbe::MessageHeader::encodedLength(),
                      rhdr.blockLength(), rhdr.version(), rcap);
    FlushRequest r;
    r.fd            = req.fd();
    r.flushBoundary = req.flushBoundary();
    r.token         = static_cast<CompletionToken>(req.token());
    return r;
}
} // namespace detail

template <PortableSyncFn Sync>
class BasicPortableFlushBackend {
public:
    // ctor: allocate the two rings and spawn the flusher thread.
    // submitDepth and completionDepth are in messages (>= 2).
    explicit BasicPortableFlushBackend(std::size_t submitDepth,
                                       std::size_t completionDepth);

    // Non-copyable, non-movable (OneToOneByteRing + std::thread).
    BasicPortableFlushBackend(const BasicPortableFlushBackend&)            = delete;
    BasicPortableFlushBackend& operator=(const BasicPortableFlushBackend&) = delete;
    BasicPortableFlushBackend(BasicPortableFlushBackend&&)                  = delete;
    BasicPortableFlushBackend& operator=(BasicPortableFlushBackend&&)       = delete;

    // dtor: post Stop on stopRing_ and join (never writes the submit ring).
    ~BasicPortableFlushBackend();

    // -----------------------------------------------------------------------
    // submit — worker side (non-blocking, no allocation on hot path).
    //
    // Assigns the next token, builds a FlushRequest, encodes it into the
    // submit ring via write().  Returns kNoToken (back-pressure) if the
    // in-flight count is >= submitDepth_ — the caller must hold the produce
    // and retry next cycle.
    // -----------------------------------------------------------------------
    [[nodiscard]] CompletionToken submit(int fd, int64_t flushBoundary) noexcept;

    // E1d: pwrite `len` (<= kWriteSyncMaxBlock) bytes of `data` at `offset` of
    // fd, then fdatasync(fd), on the flusher thread.  The bytes travel INLINE
    // in the request (copied now — no lifetime coupling).  ONE completion
    // (same token space): errorCode = the write's or the sync's errno (a short
    // write = ENOSPC).  kNoToken on back-pressure / len too large.
    [[nodiscard]] CompletionToken submitWriteSync(int fd, const uint8_t* data, uint32_t len,
                                                  int64_t offset) noexcept;

    // E1d MEDIUM-2: a FULL ::fsync(fd) on the flusher thread (a directory fd:
    // makes its entries durable).  Same token space; kNoToken on back-pressure.
    [[nodiscard]] CompletionToken submitFsync(int fd) noexcept;

    // -----------------------------------------------------------------------
    // reap — worker side (non-blocking).
    //
    // Drains all available completions from the completion ring and calls
    // handler(const FlushCompletion&) for each one.  Returns count reaped.
    // H must be callable as H(const FlushCompletion&).  No std::function.
    // -----------------------------------------------------------------------
    template <class H>
    int reap(H&& handler) noexcept {
        int count = 0;
        completionRing_.drain(
            [&](int32_t /*msgTypeId*/,
                aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t offset,
                aeron::util::index_t length) {
                // read-rework SBE-C: SBE-decode the completion in place (zero-copy
                // view into the ring) into a FlushCompletion the handler consumes.
                char* fp = reinterpret_cast<char*>(buf.buffer() + offset);
                const std::uint64_t cap = static_cast<std::uint64_t>(length);
                flushsbe::MessageHeader hdr;
                hdr.wrap(fp, 0, 0, cap);
                flushsbe::FlushCompletion dec;
                dec.wrapForDecode(fp, flushsbe::MessageHeader::encodedLength(),
                                  hdr.blockLength(), hdr.version(), cap);
                FlushCompletion comp;
                comp.token         = static_cast<CompletionToken>(dec.token());
                comp.flushBoundary = dec.flushBoundary();
                comp.errorCode     = dec.errorCode();
                handler(comp);
                ++count;
                // Decrement in-flight count: this slot is now free.
                // inFlight_ is worker-only — no synchronisation needed.
                if (inFlight_ > 0) { --inFlight_; }
            });
        return count;
    }

    /// A9: the worker's doorbell on the completion ring (worker = consumer, flusher = producer).  Startup
    /// only: before the first submit (the flusher commits completions only after a submission, which the
    /// worker publishes on the submit ring after this call — the ring's release/acquire orders the write).
    void attachCompletionDoorbell(detail::RingDoorbell& bell) noexcept {
        (void)completionRing_.attachConsumerDoorbell(bell);
    }

private:
    // flusherLoop_ — runs on the dedicated flusher thread.
    void flusherLoop_();

    OneToOneByteRing submitRing_;
    OneToOneByteRing completionRing_;

    // Worker-owned: only submit()/reap() touch these.
    CompletionToken nextToken_{0};
    std::size_t     submitDepth_{0};  // max in-flight submits
    std::size_t     inFlight_{0};     // current in-flight count (worker-side only)

    // Stop command ring — destroying thread → flusher (F9a: replaces the atomic
    // stop flag; also keeps the submit ring strictly single-producer).
    rune::consensus::ControlRing<256> stopRing_;
    static constexpr std::int32_t kStopCmd = 1;

    // The dedicated flusher thread — must be last so it starts after all
    // members are initialised.
    std::thread flusher_;
};

// ---------------------------------------------------------------------------
// Implementation (header: the template is instantiated by tests with a
// recording Sync; production's instantiation is compiled once in the .cpp).
// ---------------------------------------------------------------------------
template <PortableSyncFn Sync>
BasicPortableFlushBackend<Sync>::BasicPortableFlushBackend(std::size_t submitDepth,
                                                          std::size_t completionDepth)
    : submitRing_(detail::flushRingByteCapacity(submitDepth, kSubmitRingMaxWireLen))
    , completionRing_(detail::flushRingByteCapacity(completionDepth, kFlushCompletionWireLen))
    , nextToken_(0)
    , submitDepth_(submitDepth)
    , inFlight_(0)
    , flusher_([this]{ flusherLoop_(); })
{
}

template <PortableSyncFn Sync>
BasicPortableFlushBackend<Sync>::~BasicPortableFlushBackend() {
    // Task #12 fix: signal the flusher to stop on its own stop ring — NOT by
    // writing a sentinel to the submit ring.  The submit ring's sole producer is
    // the worker's submit(); if the dtor (which may run on a DIFFERENT thread than
    // the worker) also wrote to it, that would be a two-producer race on the
    // ring's producer position (TSan-flagged).  F9a: the stop travels as a ring
    // command (no atomic flag); join() establishes happens-before for teardown.
    (void)stopRing_.offer(kStopCmd);   // one command, otherwise-empty ring
    flusher_.join();
}

template <PortableSyncFn Sync>
CompletionToken BasicPortableFlushBackend<Sync>::submit(int fd, int64_t flushBoundary) noexcept {
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

template <PortableSyncFn Sync>
CompletionToken BasicPortableFlushBackend<Sync>::submitWriteSync(int fd, const uint8_t* data, uint32_t len,
                                                                 int64_t offset) noexcept {
    if (inFlight_ >= submitDepth_ || len > kWriteSyncMaxBlock) return kNoToken;
    const CompletionToken tok = ++nextToken_;
    const auto idx = submitRing_.tryClaim(flushsbe::WriteSyncRequest::sbeTemplateId(), kWriteSyncWireLen);
    if (idx < 0) { --nextToken_; return kNoToken; }
    flushsbe::WriteSyncRequest enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(submitRing_.base() + idx), 0, kWriteSyncWireLen);
    enc.fd(fd).length(static_cast<uint16_t>(len)).fileOffset(offset).token(static_cast<int64_t>(tok));
    std::memset(enc.block(), 0, kWriteSyncMaxBlock);
    std::memcpy(enc.block(), data, len);
    submitRing_.commit(idx);
    ++inFlight_;
    return tok;
}

template <PortableSyncFn Sync>
CompletionToken BasicPortableFlushBackend<Sync>::submitFsync(int fd) noexcept {
    if (inFlight_ >= submitDepth_) return kNoToken;
    const CompletionToken tok = ++nextToken_;
    const auto idx = submitRing_.tryClaim(kFullFsyncRecordType, kFlushRequestWireLen);
    if (idx < 0) { --nextToken_; return kNoToken; }
    flushsbe::FlushRequest enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(submitRing_.base() + idx), 0, kFlushRequestWireLen);
    enc.fd(fd).flushBoundary(0).token(static_cast<int64_t>(tok));
    submitRing_.commit(idx);
    ++inFlight_;
    return tok;
}

namespace detail {
// E1d: execute one WriteSyncRequest slot (flusher thread): pwrite then sync.
template <PortableSyncFn Sync>
inline int32_t runWriteSync(aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                            aeron::util::index_t length, CompletionToken& tokOut, int& fdOut) noexcept {
    char* rp = reinterpret_cast<char*>(buf.buffer() + offset);
    const std::uint64_t rcap = static_cast<std::uint64_t>(length);
    flushsbe::MessageHeader rhdr;
    rhdr.wrap(rp, 0, 0, rcap);
    flushsbe::WriteSyncRequest req;
    req.wrapForDecode(rp, flushsbe::MessageHeader::encodedLength(), rhdr.blockLength(), rhdr.version(), rcap);
    tokOut = static_cast<CompletionToken>(req.token());
    fdOut  = req.fd();
    const std::size_t n = req.length();
    const ssize_t w = ::pwrite(fdOut, req.block(), n, static_cast<off_t>(req.fileOffset()));
    if (w != static_cast<ssize_t>(n)) return w < 0 ? (errno != 0 ? errno : EIO) : ENOSPC;
    if (Sync(fdOut) != 0) return errno != 0 ? errno : EIO;
    return 0;
}
} // namespace detail

template <PortableSyncFn Sync>
void BasicPortableFlushBackend<Sync>::flusherLoop_() {
    // The flusher loop: drain submit ring → fdatasync → post completion.
    // Runs until the dtor posts Stop on stopRing_ (F9a).  No sentinel: submit()
    // is the submit ring's sole producer, so the dtor never writes to it (#12).
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

    bool stop = false;
    auto pollStop = [this, &stop]() noexcept {
        stopRing_.poll([&stop](std::int32_t type, std::int64_t, std::int64_t) noexcept {
            if (type == kStopCmd) stop = true;
        });
    };
    pollStop();
    while (!stop) {
        const int drained = submitRing_.drain(
            [this](int32_t msgTypeId,
                   aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t offset,
                   aeron::util::index_t length) {
                int64_t         reqBound = 0;
                CompletionToken reqTok   = 0;
                int32_t         ec       = 0;
                {
                // FR: the write/fdatasync itself, on the flusher thread (off the worker).  The span is
                // scoped to the disk request only (the completion post below stays outside it); its
                // commit may register a ring, so errno is captured INSIDE it.
                FLIGHT_RECORDER_SPAN_STORAGE(flight_recorder::Site::StorageFsync, static_cast<uint32_t>(msgTypeId));
                if (msgTypeId == static_cast<int32_t>(flushsbe::WriteSyncRequest::sbeTemplateId())) {
                    int fd = -1;   // E1d: pwrite + fdatasync (may block — off the worker path)
                    ec = detail::runWriteSync<Sync>(buf, offset, length, reqTok, fd);
                } else if (msgTypeId == kFullFsyncRecordType) {
                    // E1d MEDIUM-2: a directory — full fsync (entries are metadata).
                    const FlushRequest r = detail::decodeFlushRequest(buf, offset, length);
                    reqTok = r.token;
                    ec = ::fsync(r.fd) == 0 ? 0 : (errno != 0 ? errno : EIO);
                } else {
                    // read-rework SBE-C: SBE-decode the request in place (zero-copy).
                    const FlushRequest r = detail::decodeFlushRequest(buf, offset, length);
                    reqBound = r.flushBoundary;
                    reqTok   = r.token;
                    // Perform the datasync (may block — intentional; off the worker path).
                    const int rc = Sync(r.fd);
                    ec = (rc == 0) ? 0 : errno;
                }
                }

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

        pollStop();   // checked before idling
        if (!stop) {
            idle.idle(drained); // parks when idle; resets to spin when draining
        }
    }

    // Teardown final drain: complete any requests enqueued before Stop so their
    // data is durable on graceful shutdown.  We do the fdatasync but do NOT post
    // completions — the worker has stopped reaping, so writing to the (possibly
    // full) completion ring could spin forever and hang join().
    submitRing_.drain(
        [this](int32_t msgTypeId,
               aeron::concurrent::AtomicBuffer& buf,
               aeron::util::index_t offset,
               aeron::util::index_t length) {
            // F9a fix: the slot is SBE-encoded — decode it exactly like the main
            // loop (it was reinterpret_cast as the POD FlushRequest, syncing the
            // SBE header bytes as an fd).
            if (msgTypeId == static_cast<int32_t>(flushsbe::WriteSyncRequest::sbeTemplateId())) {
                CompletionToken t = 0; int fd = -1;
                (void)detail::runWriteSync<Sync>(buf, offset, length, t, fd);
                return;
            }
            if (msgTypeId == kFullFsyncRecordType) {
                (void)::fsync(detail::decodeFlushRequest(buf, offset, length).fd);
                return;
            }
            (void)Sync(detail::decodeFlushRequest(buf, offset, length).fd);
        },
        /*limit=*/1 << 30);
}

/// Production backend: direct platform fdatasync.
using PortableFlushBackend = BasicPortableFlushBackend<&detail::platformFdatasync>;
extern template class BasicPortableFlushBackend<&detail::platformFdatasync>;

} // namespace rune::node
