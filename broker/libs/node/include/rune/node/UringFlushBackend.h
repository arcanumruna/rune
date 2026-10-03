#pragma once
#include <vector>
// UringFlushBackend.h — E3 Task 8a: Linux raw io_uring async-durability backend.
//
// This backend is compiled and usable ONLY on Linux.  On non-Linux platforms
// the class definition is completely absent; IoBackend.h selects
// PortableFlushBackend instead, so nothing in the non-Linux build references
// this class.  The header is #include-safe on macOS (parses to empty).
//
// DESIGN (Linux only):
//   Wraps a single io_uring instance opened via the raw io_uring_setup(2) syscall
//   (syscall 425).  The SQ ring, CQ ring, and SQE array are mmap-d per the
//   standard io_uring memory layout using IORING_OFF_SQ_RING / IORING_OFF_CQ_RING /
//   IORING_OFF_SQES offsets.
//
//   submit(fd, flushBoundary):
//     - Takes the next free SQE slot from the SQ ring.
//     - Issues IORING_OP_FSYNC with IORING_FSYNC_DATASYNC on the given fd.
//     - Stores user_data = token so the completion can be matched.
//     - flushBoundary is NOT tracked (Task 10 fix 1 / I2: the old token % SQ
//       depth table aliased); completions report flushBoundary = 0.
//     - Calls io_uring_enter(2) (syscall 426) with submit_count=1 to push the SQE.
//     - Returns kNoToken if the SQ is full (back-pressure — caller retries).
//
//   reap(H&&):
//     - Reads the CQ ring head/tail; for each CQE builds FlushCompletion
//       {token, errorCode, flushBoundary} and calls handler.  Advances CQ head.
//     - Returns count of completions reaped.
//
// Memory barriers:
//   io_uring ABI requires a store-release before advancing the SQ tail (visible
//   to the kernel) and a load-acquire to read the CQ tail (written by kernel).
//   These are issued via std::atomic_thread_fence(std::memory_order_release /
//   acquire) at the appropriate points.
//
// Thread ownership:
//   submit() and reap() are WORKER-side only (single-threaded caller).
//   No internal threads; no shared mutable state.
//
// Back-pressure:
//   Returns kNoToken when the SQ ring has no free slots.
//   The caller must hold the produce and retry next cycle (never blocks, never drops).
//
// Interface parity with PortableFlushBackend:
//   ctor(std::size_t submitDepth, std::size_t completionDepth)
//   CompletionToken submit(int fd, int64_t flushBoundary) noexcept
//   template<class H> int reap(H&&) noexcept
//   Non-copyable, non-movable.  dtor cleans up (munmap + close).

#if defined(__linux__)

#include <rune/node/PortableFlushBackend.h>   // shared types: CompletionToken, kNoToken,
                                               // FlushRequest, FlushCompletion
#include <cstdint>
#include <cstddef>
#include <memory>

// Full io_uring kernel struct definitions are required here because reap() is an
// inline template that dereferences io_uring_cqe::res.  This include sits inside
// the #if defined(__linux__) guard, so it never reaches the macOS build (which
// selects PortableFlushBackend and never instantiates this class).
#include <linux/io_uring.h>

namespace rune::node {

// ---------------------------------------------------------------------------
// UringFlushBackend
// ---------------------------------------------------------------------------
class UringFlushBackend {
public:
    // ctor: creates the io_uring with entries >= submitDepth (rounded to
    // the next power of two).  completionDepth is accepted for interface
    // parity; the kernel CQ ring is typically 2× the SQ ring size which is
    // sufficient for our depth.  Throws std::system_error on setup failure.
    explicit UringFlushBackend(std::size_t submitDepth,
                               std::size_t completionDepth);

    /// Bind the CALLING THREAD's io-wq (io_uring keeps io-wq per task, not per ring) to `cpus`
    /// (IORING_REGISTER_IOWQ_AFF, Linux 5.14+).  Must run on the thread that submits — that is the
    /// task whose io-wq is bound.  Submits one internal NOP first so the thread's io-wq context
    /// exists (counts against the in-flight cap; reap drains it silently).  Returns 0, EBUSY (at
    /// the in-flight cap / SQ full — retry later) or the kernel's errno.  Cold.
    int setIoWorkerAffinity(const std::vector<int32_t>& cpus) noexcept;
    /// Submitted-not-yet-reaped operations (including the internal NOP).  Worker thread / tests.
    [[nodiscard]] std::size_t inFlight() const noexcept { return inFlight_; }
    /// user_data of the internal NOP (never a real token: tokens count up from 1).
    static constexpr uint64_t kInternalNopUserData = ~uint64_t{0};

    // Non-copyable, non-movable.
    UringFlushBackend(const UringFlushBackend&)            = delete;
    UringFlushBackend& operator=(const UringFlushBackend&) = delete;
    UringFlushBackend(UringFlushBackend&&)                  = delete;
    UringFlushBackend& operator=(UringFlushBackend&&)       = delete;

    // dtor: munmap rings + close ring_fd_.
    ~UringFlushBackend();

    // submit — worker side (non-blocking, no allocation).
    //
    // Assigns the next token, fills an SQE for IORING_OP_FSYNC|DATASYNC on fd,
    // (flushBoundary is ignored — completions match by token), advances the SQ tail
    // with a release fence, and submits via io_uring_enter.
    //
    // Returns kNoToken if the SQ ring is full (back-pressure).
    [[nodiscard]] CompletionToken submit(int fd, int64_t flushBoundary) noexcept;

    // F15 S4 fix round 2 (RR1-4): an async IORING_OP_FADVISE(advice) on
    // [off, off + len) of fd.  kNoToken when the SQ / in-flight cap is full
    // (the caller skips this pass).  Same reap path; route the token.
    [[nodiscard]] CompletionToken submitFadvise(int fd, int64_t off, uint32_t len, int advice) noexcept;
    /// RR2-1: IORING_OP_FADVISE is supported by this kernel / ring (probed at
    /// construction with IORING_REGISTER_PROBE; a failing probe = unsupported).
    [[nodiscard]] bool fadviseSupported() const noexcept { return fadviseSupported_; }

    // reap — worker side (non-blocking).
    //
    // Drains all available CQEs.  For each CQE calls handler(FlushCompletion).
    // Returns the count of completions processed.
    // H must be callable as H(const FlushCompletion&).
    template <class H>
    int reap(H&& handler) noexcept;

private:
    // io_uring ring fd (returned by io_uring_setup).
    int ringFd_{-1};

    // Actual SQ ring size (power-of-two, >= submitDepth_).
    uint32_t sqRingSize_{0};

    // SQ ring mmap pointers (mapped at IORING_OFF_SQ_RING).
    uint32_t* sqHead_{nullptr};
    uint32_t* sqTail_{nullptr};
    uint32_t* sqRingMask_{nullptr};
    uint32_t* sqArray_{nullptr};    // SQ index array (maps sq slot → sqe index)
    void*     sqRingMmap_{nullptr};
    std::size_t sqRingMmapLen_{0};

    // SQE array mmap (mapped at IORING_OFF_SQES).
    void*     sqesMmap_{nullptr};
    std::size_t sqesMmapLen_{0};
    struct io_uring_sqe* sqes_{nullptr};

    // CQ ring mmap (mapped at IORING_OFF_CQ_RING — may alias sqRingMmap_ when
    // IORING_FEAT_SINGLE_MMAP is set).
    uint32_t* cqHead_{nullptr};
    uint32_t* cqTail_{nullptr};
    uint32_t* cqRingMask_{nullptr};
    struct io_uring_cqe* cqes_{nullptr};
    void*     cqRingMmap_{nullptr};
    std::size_t cqRingMmapLen_{0};
    bool      singleMmap_{false};  // true when CQ ring aliases SQ ring mmap

    // Worker-owned next-token counter (starts at 1; 0 == kNoToken is reserved).
    CompletionToken nextToken_{0};

    // Worker-owned in-flight counter (submitted-but-not-yet-reaped).  io_uring's
    // SQ ring DRAINS on each io_uring_enter (the kernel consumes the SQE), so an
    // SQ-fullness check never trips; instead we bound in-flight completions to the
    // configured depth for deterministic back-pressure (mirrors PortableFlushBackend).
    // submit() returns kNoToken when inFlight_ >= submitDepth_; reap() decrements.
    std::size_t inFlight_{0};

    // Depth actually allocated for the SQ ring (== sqRingSize_).
    std::size_t submitDepth_{0};


    bool fadviseSupported_{false};
    bool probeOpcode_(uint8_t opcode) noexcept;

    // The one SQE submission path (fsync, fadvise, internal NOP): reserve the SQ
    // slot, copy `sqe` in, publish the tail, io_uring_enter; rolls the tail back
    // on failure.  Returns 0, EBUSY (SQ full) or the enter errno.
    int pushSqe_(const struct io_uring_sqe& sqe) noexcept;
    CompletionToken allocToken_() noexcept;
};

// ---------------------------------------------------------------------------
// reap — inline template implementation
// ---------------------------------------------------------------------------
template <class H>
int UringFlushBackend::reap(H&& handler) noexcept {
    // CQ tail is written by the kernel; acquire fence to observe kernel writes.
    const uint32_t mask = *cqRingMask_;
    int count = 0;

    while (true) {
        // Load-acquire the CQ tail (kernel-written).
        const uint32_t head = __atomic_load_n(cqHead_, __ATOMIC_ACQUIRE);
        const uint32_t tail = __atomic_load_n(cqTail_, __ATOMIC_ACQUIRE);
        if (head == tail) {
            break;  // no pending completions
        }

        const uint32_t idx = head & mask;
        const struct io_uring_cqe* cqe = cqes_ + idx;

        // setIoWorkerAffinity's internal NOP is not a caller token: drained, never handed out.
        if (cqe->user_data != kInternalNopUserData) [[likely]] {
            // Decode: user_data == token, res == 0 (success) or -errno (failure).
            FlushCompletion comp{};
            comp.token     = static_cast<CompletionToken>(cqe->user_data);
            comp.errorCode = (cqe->res < 0) ? -cqe->res : 0;

            // flushBoundary is informational and not tracked by io_uring (I2: the
            // old token % ring-size table aliased).  Completions match by token.
            comp.flushBoundary = 0;

            handler(comp);
            ++count;
        }
        if (inFlight_ > 0) --inFlight_;   // one completion drained → free a slot

        // Advance CQ head — store-release so the kernel sees the freed slot.
        __atomic_store_n(cqHead_, head + 1, __ATOMIC_RELEASE);
    }

    return count;
}

} // namespace rune::node

#endif // defined(__linux__)
