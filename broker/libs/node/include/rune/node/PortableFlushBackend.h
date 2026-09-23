#pragma once
// PortableFlushBackend.h — E3: portable async-durability backend.
//
// Also defines the shared I/O types (FlushRequest, FlushCompletion,
// CompletionToken, kNoToken) that are used by ALL backends.  IoBackend.h
// includes this file before adding the platform alias.
//
// A dedicated flusher std::thread communicates with the worker ONLY via two
// SpscRings (submit + completion).  No shared mutable state, no mutex, no
// condvar, no thread_local on the hot path ([[rune-shared-nothing-law]]).
//
// The flusher thread MAY block in ::fdatasync — it is off the worker's path.
// The worker-facing submit() and reap() are non-blocking memory ops (SPSC ring
// reads/writes) with NO allocation on the hot path.
//
// Back-pressure:
//   submit() returns kNoToken when the submit ring is full — the caller must
//   hold the produce and retry next cycle (never drops, never blocks).
//
// Lifecycle:
//   - ctor: spawns the flusher thread.
//   - dtor: enqueues a sentinel FlushRequest{fd=-1} and joins the thread.
//
// Thread ownership:
//   - submit() and nextToken_ are WORKER-side (only the worker calls submit).
//   - The flusher thread owns the submit ring's consumer side and the
//     completion ring's producer side.
//   - reap() is WORKER-side (only the worker calls reap, draining completions).

#include <rune/node/SpscRing.h>
#include <cstdint>
#include <thread>

namespace rune::node {

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
// PortableFlushBackend
// ---------------------------------------------------------------------------
class PortableFlushBackend {
public:
    // ctor: allocate the two rings and spawn the flusher thread.
    // submitDepth and completionDepth must be powers of two and >= 2.
    explicit PortableFlushBackend(std::size_t submitDepth,
                                  std::size_t completionDepth);

    // Non-copyable, non-movable (SpscRing + std::thread).
    PortableFlushBackend(const PortableFlushBackend&)            = delete;
    PortableFlushBackend& operator=(const PortableFlushBackend&) = delete;
    PortableFlushBackend(PortableFlushBackend&&)                  = delete;
    PortableFlushBackend& operator=(PortableFlushBackend&&)       = delete;

    // dtor: signal stop via a sentinel (fd=-1) and join.
    ~PortableFlushBackend();

    // -----------------------------------------------------------------------
    // submit — worker side (non-blocking, no allocation on hot path).
    //
    // Assigns the next token, builds a FlushRequest, and tries to enqueue it
    // on the submit ring.  Returns kNoToken (back-pressure) if the ring is
    // full — the caller must hold the produce and retry next cycle.
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
        completionRing_.drain([&](FlushCompletion&& c) {
            handler(static_cast<const FlushCompletion&>(c));
            ++count;
        });
        return count;
    }

private:
    // flusherLoop_ — runs on the dedicated flusher thread.
    // Implemented in PortableFlushBackend.cpp.
    void flusherLoop_();

    SpscRing<FlushRequest>    submitRing_;
    SpscRing<FlushCompletion> completionRing_;

    // Worker-owned: only submit() touches this.
    CompletionToken nextToken_{0};

    // Flusher-thread stop flag — written by flusher only after seeing the
    // sentinel; read by flusher only.  Plain bool: single-writer.
    bool stop_{false};

    // The dedicated flusher thread — must be last so it starts after all
    // members are initialised.
    std::thread flusher_;
};

} // namespace rune::node
