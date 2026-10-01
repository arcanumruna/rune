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
#include <cstdint>
#include <cstring>
#include <thread>

namespace rune::node {

// read-rework SBE-C: encoded wire sizes of the Flush ring messages (SBE header +
// fixed block) — used for ring sizing + tryClaim reserves.
namespace flushsbe = rune::protocol::internal::sbe;
inline constexpr std::size_t kFlushRequestWireLen =
    flushsbe::MessageHeader::encodedLength() + flushsbe::FlushRequest::sbeBlockLength();
inline constexpr std::size_t kFlushCompletionWireLen =
    flushsbe::MessageHeader::encodedLength() + flushsbe::FlushCompletion::sbeBlockLength();

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
// PortableFlushBackend
// ---------------------------------------------------------------------------
class PortableFlushBackend {
public:
    /// The data-sync primitive the flusher calls (fdatasync; fsync on macOS).
    /// Injectable for tests ONLY (fixed at construction, before the flusher
    /// thread starts — immutable afterwards).  nullptr = the platform default.
    using SyncFn = int (*)(int fd);

    // ctor: allocate the two rings and spawn the flusher thread.
    // submitDepth and completionDepth are in messages (>= 2).
    explicit PortableFlushBackend(std::size_t submitDepth,
                                  std::size_t completionDepth,
                                  SyncFn      syncFn = nullptr);

    // Non-copyable, non-movable (OneToOneByteRing + std::thread).
    PortableFlushBackend(const PortableFlushBackend&)            = delete;
    PortableFlushBackend& operator=(const PortableFlushBackend&) = delete;
    PortableFlushBackend(PortableFlushBackend&&)                  = delete;
    PortableFlushBackend& operator=(PortableFlushBackend&&)       = delete;

    // dtor: post Stop on stopRing_ and join (never writes the submit ring).
    ~PortableFlushBackend();

    // -----------------------------------------------------------------------
    // submit — worker side (non-blocking, no allocation on hot path).
    //
    // Assigns the next token, builds a FlushRequest, encodes it into the
    // submit ring via write().  Returns kNoToken (back-pressure) if the
    // in-flight count is >= submitDepth_ — the caller must hold the produce
    // and retry next cycle.
    // -----------------------------------------------------------------------
    [[nodiscard]] CompletionToken submit(int fd, int64_t flushBoundary) noexcept;

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

private:
    // flusherLoop_ — runs on the dedicated flusher thread.
    // Implemented in PortableFlushBackend.cpp.
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

    // Immutable after construction (set before the flusher thread starts).
    SyncFn syncFn_;

    // The dedicated flusher thread — must be last so it starts after all
    // members are initialised.
    std::thread flusher_;
};

} // namespace rune::node
