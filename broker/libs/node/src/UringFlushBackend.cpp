// UringFlushBackend.cpp — E3 Task 8a: raw io_uring backend (Linux only).
//
// On non-Linux this translation unit is empty (the #ifdef guards everything).
//
// Implementation notes:
//   - Uses only raw syscalls via syscall(2): io_uring_setup (NR 425) and
//     io_uring_enter (NR 426).  NO liburing dependency.
//   - The three mmaps follow the standard io_uring layout:
//       IORING_OFF_SQ_RING (0)        — SQ ring metadata + index array
//       IORING_OFF_SQES   (0x10000000) — SQE array
//       IORING_OFF_CQ_RING (0x8000000) — CQ ring metadata + CQE array
//     When the kernel sets IORING_FEAT_SINGLE_MMAP the SQ and CQ rings are in
//     the same mmap region (IORING_OFF_SQ_RING) and the CQ mmap is skipped.
//   - Memory-ordering: io_uring ABI requires
//       * A store-release of the SQ tail before calling io_uring_enter so the
//         kernel sees the submitted SQE.
//       * A load-acquire of the CQ tail to observe kernel-written completions.
//       * A store-release of the CQ head after consuming CQEs so the kernel can
//         reuse the slot.
//     We use GCC/Clang __atomic builtins directly on the shared ring fields.
//   - SQPOLL is NOT used in this first version — each submit calls
//     io_uring_enter(ring_fd, 1, 0, 0, nullptr, 0) to kick the kernel.  SQPOLL
//     is an optional optimisation deferred to a follow-up.
//   - Back-pressure: submit() returns kNoToken if the SQ ring has no free slots
//     (sqHead_ == sqTail_ + sqRingSize_), i.e. all slots are in flight.
//     The caller must hold the produce and retry next doWork cycle.

#if defined(__linux__)

#include <rune/node/UringFlushBackend.h>

#include <linux/io_uring.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

// ---------------------------------------------------------------------------
// io_uring syscall wrappers (no liburing)
// ---------------------------------------------------------------------------

static int rune_io_uring_setup(uint32_t entries, struct io_uring_params* p) {
    return static_cast<int>(::syscall(__NR_io_uring_setup, entries, p));
}

static int rune_io_uring_enter(int fd, uint32_t to_submit, uint32_t min_complete,
                               uint32_t flags) {
    return static_cast<int>(::syscall(__NR_io_uring_enter, fd, to_submit,
                                      min_complete, flags, nullptr,
                                      static_cast<std::size_t>(0)));
}

// ---------------------------------------------------------------------------
// Helper: round up to next power of two (minimum 1)
// ---------------------------------------------------------------------------
static uint32_t nextPow2(uint32_t n) {
    if (n == 0) return 1;
    --n;
    n |= n >> 1;  n |= n >> 2;  n |= n >> 4;  n |= n >> 8;  n |= n >> 16;
    return n + 1;
}

namespace rune::node {

// ---------------------------------------------------------------------------
// ctor
// ---------------------------------------------------------------------------
UringFlushBackend::UringFlushBackend(std::size_t submitDepth,
                                     std::size_t /*completionDepth*/) {
    if (submitDepth == 0) submitDepth = 1;

    // Round up to power of two — the kernel adjusts entries to the next pow2
    // anyway; doing it ourselves lets us reason about the ring size precisely.
    const uint32_t entries = nextPow2(static_cast<uint32_t>(submitDepth));

    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));
    // No flags: use basic interruption-driven completion (no SQPOLL/IOPOLL).

    ringFd_ = rune_io_uring_setup(entries, &params);
    if (ringFd_ < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "io_uring_setup failed");
    }

    // Actual ring sizes from kernel params.
    sqRingSize_ = params.sq_entries;  // power-of-two

    submitDepth_ = sqRingSize_;

    // Allocate the token→boundary table (indexed by token % sqRingSize_).
    boundaryTable_ = std::make_unique<int64_t[]>(sqRingSize_);
    std::memset(boundaryTable_.get(), 0, sqRingSize_ * sizeof(int64_t));

    // -----------------------------------------------------------------------
    // mmap 1: SQ ring  (IORING_OFF_SQ_RING = 0)
    // -----------------------------------------------------------------------
    sqRingMmapLen_ = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    sqRingMmap_ = ::mmap(nullptr, sqRingMmapLen_,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE,
                         ringFd_,
                         IORING_OFF_SQ_RING);
    if (sqRingMmap_ == MAP_FAILED) {
        ::close(ringFd_);
        ringFd_ = -1;
        throw std::system_error(errno, std::generic_category(),
                                "mmap SQ ring failed");
    }

    char* sqBase = static_cast<char*>(sqRingMmap_);
    sqHead_     = reinterpret_cast<uint32_t*>(sqBase + params.sq_off.head);
    sqTail_     = reinterpret_cast<uint32_t*>(sqBase + params.sq_off.tail);
    sqRingMask_ = reinterpret_cast<uint32_t*>(sqBase + params.sq_off.ring_mask);
    sqArray_    = reinterpret_cast<uint32_t*>(sqBase + params.sq_off.array);

    // -----------------------------------------------------------------------
    // mmap 2: SQE array  (IORING_OFF_SQES = 0x10000000)
    // -----------------------------------------------------------------------
    sqesMmapLen_ = params.sq_entries * sizeof(struct io_uring_sqe);
    sqesMmap_ = ::mmap(nullptr, sqesMmapLen_,
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_POPULATE,
                       ringFd_,
                       IORING_OFF_SQES);
    if (sqesMmap_ == MAP_FAILED) {
        ::munmap(sqRingMmap_, sqRingMmapLen_);
        ::close(ringFd_);
        ringFd_ = -1;
        throw std::system_error(errno, std::generic_category(),
                                "mmap SQE array failed");
    }
    sqes_ = static_cast<struct io_uring_sqe*>(sqesMmap_);

    // -----------------------------------------------------------------------
    // mmap 3: CQ ring  (IORING_OFF_CQ_RING = 0x8000000)
    //   If IORING_FEAT_SINGLE_MMAP is set the SQ mmap already covers the CQ
    //   ring — skip the second mmap and alias the same pointer.
    // -----------------------------------------------------------------------
    singleMmap_ = (params.features & IORING_FEAT_SINGLE_MMAP) != 0;

    if (singleMmap_) {
        // CQ ring is in the same mmap as the SQ ring (kernel >= 5.4).
        cqRingMmap_    = sqRingMmap_;
        cqRingMmapLen_ = 0;  // no separate mapping to munmap
    } else {
        cqRingMmapLen_ = params.cq_off.cqes +
                         params.cq_entries * sizeof(struct io_uring_cqe);
        cqRingMmap_ = ::mmap(nullptr, cqRingMmapLen_,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_POPULATE,
                             ringFd_,
                             IORING_OFF_CQ_RING);
        if (cqRingMmap_ == MAP_FAILED) {
            ::munmap(sqesMmap_, sqesMmapLen_);
            ::munmap(sqRingMmap_, sqRingMmapLen_);
            ::close(ringFd_);
            ringFd_ = -1;
            throw std::system_error(errno, std::generic_category(),
                                    "mmap CQ ring failed");
        }
    }

    char* cqBase  = static_cast<char*>(cqRingMmap_);
    cqHead_       = reinterpret_cast<uint32_t*>(cqBase + params.cq_off.head);
    cqTail_       = reinterpret_cast<uint32_t*>(cqBase + params.cq_off.tail);
    cqRingMask_   = reinterpret_cast<uint32_t*>(cqBase + params.cq_off.ring_mask);
    cqes_         = reinterpret_cast<struct io_uring_cqe*>(cqBase + params.cq_off.cqes);
}

// ---------------------------------------------------------------------------
// dtor
// ---------------------------------------------------------------------------
UringFlushBackend::~UringFlushBackend() {
    if (!singleMmap_ && cqRingMmap_ != nullptr && cqRingMmap_ != MAP_FAILED) {
        ::munmap(cqRingMmap_, cqRingMmapLen_);
    }
    if (sqesMmap_ != nullptr && sqesMmap_ != MAP_FAILED) {
        ::munmap(sqesMmap_, sqesMmapLen_);
    }
    if (sqRingMmap_ != nullptr && sqRingMmap_ != MAP_FAILED) {
        ::munmap(sqRingMmap_, sqRingMmapLen_);
    }
    if (ringFd_ >= 0) {
        ::close(ringFd_);
    }
}

// ---------------------------------------------------------------------------
// submit
// ---------------------------------------------------------------------------
CompletionToken UringFlushBackend::submit(int fd,
                                          int64_t flushBoundary) noexcept {
    // Deterministic back-pressure: bound in-flight (submitted-not-yet-reaped)
    // completions to the configured depth.  The SQ ring itself drains on every
    // io_uring_enter (the kernel consumes SQEs immediately), so an SQ-fullness
    // check alone never trips — cap in-flight instead (mirrors PortableFlushBackend).
    if (inFlight_ >= submitDepth_) {
        return kNoToken;
    }

    // Load SQ head with acquire semantics — the kernel writes head after
    // consuming SQEs, so we need to observe that write here.
    const uint32_t head = __atomic_load_n(sqHead_, __ATOMIC_ACQUIRE);
    const uint32_t tail = __atomic_load_n(sqTail_, __ATOMIC_RELAXED);
    const uint32_t mask = *sqRingMask_;

    // SQ ring full (defensive; normally never trips given the in-flight cap above).
    if (tail - head >= sqRingSize_) {
        return kNoToken;
    }

    // Allocate the next token (0 == kNoToken is reserved; skip it).
    CompletionToken tok = ++nextToken_;
    if (tok == kNoToken) {
        tok = ++nextToken_;
    }

    const CompletionToken result = submitWithToken(fd, flushBoundary, tok, tail, mask);
    if (result != kNoToken) {
        ++inFlight_;   // one more submitted-not-yet-reaped
    }
    return result;
}

// ---------------------------------------------------------------------------
// Private helper: fill SQE and kick the kernel.
// Separated so submit() can handle the token-wraparound case cleanly.
// ---------------------------------------------------------------------------
CompletionToken UringFlushBackend::submitWithToken(int fd,
                                                   int64_t flushBoundary,
                                                   CompletionToken tok,
                                                   uint32_t tail,
                                                   uint32_t mask) noexcept {
    // The SQE slot to fill is indexed by the tail position.
    const uint32_t sqeIdx  = tail & mask;
    struct io_uring_sqe* sqe = sqes_ + sqeIdx;

    // Zero the SQE first — the ABI reserves many fields that must be 0.
    std::memset(sqe, 0, sizeof(*sqe));
    sqe->opcode      = IORING_OP_FSYNC;
    sqe->flags       = 0;
    sqe->ioprio      = 0;
    sqe->fd          = fd;
    sqe->off         = 0;
    sqe->addr        = 0;
    sqe->len         = 0;
    sqe->fsync_flags = IORING_FSYNC_DATASYNC;
    sqe->user_data   = static_cast<uint64_t>(tok);

    // Map sqe index into the SQ index array (one-to-one for simple rings).
    sqArray_[sqeIdx] = sqeIdx;

    // Store flushBoundary in the worker-owned table (no cross-thread access).
    const uint32_t slot = static_cast<uint32_t>(tok) % sqRingSize_;
    boundaryTable_[slot] = flushBoundary;

    // Advance the SQ tail — store-release so the kernel sees the new SQE.
    __atomic_store_n(sqTail_, tail + 1, __ATOMIC_RELEASE);

    // Submit: wake the kernel so it processes our SQE.
    // to_submit=1, min_complete=0 (async — don't wait for completion here).
    const int ret = rune_io_uring_enter(ringFd_, /*to_submit=*/1,
                                        /*min_complete=*/0, /*flags=*/0);
    if (ret < 0) {
        // io_uring_enter failed — undo the tail advance.  This is a serious
        // error; log or metric in production; here we return kNoToken as the
        // back-pressure signal so the caller retries.
        __atomic_store_n(sqTail_, tail, __ATOMIC_RELEASE);
        return kNoToken;
    }

    return tok;
}

} // namespace rune::node

#endif // defined(__linux__)
