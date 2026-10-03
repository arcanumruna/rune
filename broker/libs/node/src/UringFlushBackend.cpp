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
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string_view>
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

    // RUNE_IO_BACKEND=portable no longer reaches this class: IoBackend
    // constructs the REAL PortableFlushBackend instead (Task 10 fix 1 / C1 —
    // the old in-class "portable fallback" silently returned kNoToken forever).

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

    // F15 S4 fix round 3 (RR2-1): IORING_OP_FADVISE exists from kernel 5.6 —
    // the same release as IORING_REGISTER_PROBE.  A failing probe (older
    // kernel, restricted ring, seccomp) or an unsupported opcode → no async
    // hints: the worker uses the synchronous hint instead (never a hint that
    // fails forever and stalls a read_committed build).
    fadviseSupported_ = probeOpcode_(IORING_OP_FADVISE);

    // E1d: pre-allocate the linked-write blocks (at most one per two in-flight
    // slots) — submitWriteSync never allocates.
    const std::size_t nBlocks = submitDepth_ / 2 + 1;
    writeBlocks_.resize(nBlocks);
    freeWriteBlocks_.reserve(nBlocks);
    for (std::size_t i = nBlocks; i > 0; --i) freeWriteBlocks_.push_back(static_cast<uint32_t>(i - 1));
    shortWriteTokens_.reserve(nBlocks);
}

bool UringFlushBackend::probeOpcode_(uint8_t opcode) noexcept {
    constexpr unsigned kOps = 256;
    alignas(struct io_uring_probe) unsigned char buf[sizeof(struct io_uring_probe) +
                                                     kOps * sizeof(struct io_uring_probe_op)] = {};
    auto* probe = reinterpret_cast<struct io_uring_probe*>(buf);
    const long rc = ::syscall(__NR_io_uring_register, ringFd_, IORING_REGISTER_PROBE, probe, kOps);
    if (rc < 0) return false;
    if (opcode > probe->last_op) return false;
    return (probe->ops[opcode].flags & IO_URING_OP_SUPPORTED) != 0;
}

// ---------------------------------------------------------------------------
// setIoWorkerAffinity — bind the CALLING thread's io-wq workers (A4)
// ---------------------------------------------------------------------------
int UringFlushBackend::setIoWorkerAffinity(const std::vector<int32_t>& cpus) noexcept {
    if (cpus.empty() || ringFd_ < 0) return EINVAL;
    if (inFlight_ >= submitDepth_) return EBUSY;   // keep the in-flight bound; the worker retries next pass
    // 1. Create this thread's io_uring task context + io-wq: the kernel attaches it on the first
    //    io_uring_enter that submits.  One NOP (completes inline; reap drains its CQE silently).
    struct io_uring_sqe sqe{};
    sqe.opcode    = IORING_OP_NOP;
    sqe.user_data = kInternalNopUserData;
    if (const int e = pushSqe_(sqe); e != 0) return e;
    ++inFlight_;
    // 2. Bind THIS thread's io-wq.
    cpu_set_t set;
    CPU_ZERO(&set);
    for (const int32_t c : cpus) if (c >= 0 && c < CPU_SETSIZE) CPU_SET(c, &set);
    constexpr unsigned kRegisterIowqAff = 17;   // IORING_REGISTER_IOWQ_AFF (absent from older uapi headers)
    if (::syscall(__NR_io_uring_register, ringFd_, kRegisterIowqAff, &set, sizeof(set)) != 0) return errno;
    return 0;
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
// pushSqe_ — the single SQE submission path
// ---------------------------------------------------------------------------
int UringFlushBackend::pushSqes_(const struct io_uring_sqe* sqes, uint32_t n) noexcept {
    // Load SQ head with acquire semantics — the kernel writes head after
    // consuming SQEs, so we need to observe that write here.
    const uint32_t head = __atomic_load_n(sqHead_, __ATOMIC_ACQUIRE);
    const uint32_t tail = __atomic_load_n(sqTail_, __ATOMIC_RELAXED);
    // SQ ring full (defensive; normally never trips given the in-flight cap).
    if (tail - head + n > sqRingSize_) return EBUSY;

    // The SQE slots to fill are indexed by the tail position; one-to-one SQ index array.
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t sqeIdx = (tail + i) & *sqRingMask_;
        sqes_[sqeIdx] = sqes[i];
        sqArray_[sqeIdx] = sqeIdx;
    }

    // Advance the SQ tail — store-release so the kernel sees the new SQEs.
    __atomic_store_n(sqTail_, tail + n, __ATOMIC_RELEASE);

    // Submit ALL n with ONE enter (E1d: a linked chain must not end a
    // submission batch); min_complete=0 (async — don't wait for completion).
    if (rune_io_uring_enter(ringFd_, /*to_submit=*/n, /*min_complete=*/0, /*flags=*/0) < 0) {
        const int e = errno;
        // io_uring_enter failed — undo the tail advance; the caller sees back-pressure.
        __atomic_store_n(sqTail_, tail, __ATOMIC_RELEASE);
        return e;
    }
    return 0;
}

int UringFlushBackend::pushSqe_(const struct io_uring_sqe& sqe) noexcept { return pushSqes_(&sqe, 1); }

CompletionToken UringFlushBackend::allocToken_() noexcept {
    // 0 == kNoToken is reserved; skip it on wraparound.
    CompletionToken tok = ++nextToken_;
    if (tok == kNoToken) tok = ++nextToken_;
    return tok;
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

    // flushBoundary is not tracked (I2): a token % ring-size table aliased when
    // completions arrived out of order (T and T+ringSize both in flight) and
    // could wedge acks forever.  Completions are matched by token only.
    (void)flushBoundary;

    const CompletionToken tok = allocToken_();
    struct io_uring_sqe sqe{};
    sqe.opcode      = IORING_OP_FSYNC;
    sqe.fd          = fd;
    sqe.fsync_flags = IORING_FSYNC_DATASYNC;
    sqe.user_data   = static_cast<uint64_t>(tok);
    if (pushSqe_(sqe) != 0) return kNoToken;   // back-pressure: the caller retries
    ++inFlight_;   // one more submitted-not-yet-reaped
    return tok;
}

// ---------------------------------------------------------------------------
// E1d: linked write + fdatasync (recovery-point checkpoint slot).
// ---------------------------------------------------------------------------
CompletionToken UringFlushBackend::submitWriteSync(int fd, const uint8_t* data, uint32_t len,
                                                   int64_t offset) noexcept {
    if (inFlight_ + 2 > submitDepth_ || len > kWriteSyncMaxBlock || freeWriteBlocks_.empty())
        return kNoToken;
    const uint32_t bi = freeWriteBlocks_.back();
    WriteBlock& wb = writeBlocks_[bi];
    std::memcpy(wb.bytes, data, len);
    wb.len = len;
    const CompletionToken tok = allocToken_();
    wb.token = tok;
    struct io_uring_sqe sqe[2]{};
    sqe[0].opcode    = IORING_OP_WRITE;
    sqe[0].fd        = fd;
    sqe[0].addr      = reinterpret_cast<uint64_t>(wb.bytes);
    sqe[0].len       = len;
    sqe[0].off       = static_cast<uint64_t>(offset);
    sqe[0].flags     = IOSQE_IO_LINK;
    sqe[0].user_data = kLinkedWriteTag | bi;
    sqe[1].opcode      = IORING_OP_FSYNC;
    sqe[1].fd          = fd;
    sqe[1].fsync_flags = IORING_FSYNC_DATASYNC;
    sqe[1].user_data   = static_cast<uint64_t>(tok);
    if (pushSqes_(sqe, 2) != 0) return kNoToken;   // back-pressure: the caller retries
    freeWriteBlocks_.pop_back();
    inFlight_ += 2;   // two CQEs will be reaped
    return tok;
}

// ---------------------------------------------------------------------------
// F15 S4 fix round 2 (RR1-4): IORING_OP_FADVISE — an asynchronous page-cache
// read-ahead hint (POSIX_FADV_WILLNEED) on `fd` for [off, off + len).  The
// kernel forces FADVISE async (io-wq): the worker pays one SQE write and the
// shared submit.  Same in-flight cap / token space / reap as the fsyncs; the
// caller routes the token (RouteKind::Hint) so a hint completion — success or
// error — never reaches produce-ack matching or the fsync fail-stop.
// ---------------------------------------------------------------------------
CompletionToken UringFlushBackend::submitFadvise(int fd, int64_t off, uint32_t len, int advice) noexcept {
    if (inFlight_ >= submitDepth_) return kNoToken;
    const CompletionToken tok = allocToken_();
    struct io_uring_sqe sqe{};
    sqe.opcode         = IORING_OP_FADVISE;
    sqe.fd             = fd;
    sqe.off            = static_cast<uint64_t>(off);
    sqe.len            = len;
    sqe.fadvise_advice = static_cast<uint32_t>(advice);
    sqe.user_data      = static_cast<uint64_t>(tok);
    if (pushSqe_(sqe) != 0) return kNoToken;   // SQ full / enter failed: the caller skips this pass
    ++inFlight_;
    return tok;
}

} // namespace rune::node

#endif // defined(__linux__)
