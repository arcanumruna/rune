#pragma once
// RingDoorbell — A9: the wake-up half of the O2O ring (ring internals; findings §4.15).
// (Lives with the consensus ring internals so rune_storage can embed one — D19 L3.)
//
// A shard worker parks in ppoll() on readiness fds (A7: driver sockets, io_uring).  Work that arrives on a
// cross-thread ring (runtime → worker ops, session lifecycle, worker → fused-runtime returns, portable
// fsync completions) has no fd, so A7 had to keep the Aeron spin/yield phase and a 200 µs park cap.  A
// doorbell gives those rings an fd: ONE per consumer thread (eventfd on Linux, a non-blocking pipe
// elsewhere), shared by every ring that thread consumes.
//
// Lost-wakeup-free protocol (Dekker: each side stores, full fence, then loads the other side's word):
//   consumer, before a park:  parking_ = 1;  fence(seq_cst);  any watched ring with a COMMITTED record at its
//                             head?  yes → parking_ = 0, do not park (run the pass);  no → ppoll(.., fd()).
//                             After the park: parking_ = 0; drain fd() if it fired.
//   producer, after commit:   (the record length is stored, release)  fence(seq_cst);
//                             parking_ != 0 && exchange(parking_, 0) != 0 → write fd().
// With both fences, at least one side sees the other's store: either the consumer sees the committed record
// (and does not park) or the producer sees parking_ (and rings).  A record committed while the consumer runs
// costs the producer one fence + one load — no syscall; only a commit that finds the consumer parking writes
// the fd, and the exchange dedupes several producers (rings) to one write per park.
//
// Watches see COMMITTED records only (length > 0 at the head), never claimed-but-uncommitted ones: a claim
// whose commit is late would otherwise make the consumer re-run passes until the commit (a spin).  A watch
// may be gated off by its consumer (`enabled`, owner-thread state): a ring the consumer deliberately leaves
// unconsumed (back-pressure) must not keep it awake — that consumer waits on the gate's own wake source.
//
// Space wait (the reverse direction, O2ORingBuffer::armSpaceWait): a producer refused for space sets the
// ring's want-space word, fences, re-checks; the consumer, after freeing space, fences and rings the
// producer's doorbell iff the word was set (one fence per non-empty read, only on rings that have a producer
// doorbell).
//
// Shared-nothing law: the only atomics are the parking_ word here and the want-space word in the ring —
// ring-implementation internals (scripts/atomics_allowlist.txt, category RING).  Never busy-spins, never
// allocates after construction, no virtual call.
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/eventfd.h>
#endif

namespace rune::consensus::detail {

/// What a doorbell needs to see whether a ring holds a committed record at its head — the ring's buffer, its
/// pow2 payload capacity and its head-position index (Aeron RingBufferDescriptor layout).
struct RingWatch {
    std::uint8_t*        buffer            = nullptr;
    aeron::util::index_t length            = 0;   ///< capacity + trailer
    aeron::util::index_t capacity          = 0;
    aeron::util::index_t headPositionIndex = 0;
};

class RingDoorbell {
public:
    static constexpr int kMaxWatches = 32;

    RingDoorbell() noexcept {
#if defined(__linux__)
        readFd_ = writeFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
#else
        int p[2];
        if (::pipe(p) == 0) {
            for (int fd : p) {
                ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
                ::fcntl(fd, F_SETFD, FD_CLOEXEC);
            }
            readFd_ = p[0];
            writeFd_ = p[1];
        }
#endif
    }
    ~RingDoorbell() {
        if (readFd_ >= 0) ::close(readFd_);
        if (writeFd_ >= 0 && writeFd_ != readFd_) ::close(writeFd_);
    }
    RingDoorbell(const RingDoorbell&)            = delete;
    RingDoorbell& operator=(const RingDoorbell&) = delete;

    /// Readable once rung; -1 if the fd could not be created (then no ring attaches to this doorbell).
    [[nodiscard]] int fd() const noexcept { return readFd_; }
    [[nodiscard]] bool valid() const noexcept { return readFd_ >= 0 && writeFd_ >= 0; }

    /// Startup only (before any producer thread exists): watch a ring this doorbell's thread consumes.
    /// `enabled` (may be null = always): owner-thread gate.  False when full or invalid — the caller must then
    /// not attach the ring (a ring rung without a watch could lose the wakeup in the arm window).
    bool addWatch(const RingWatch& w, const bool* enabled = nullptr) noexcept {
        if (!valid() || nwatches_ >= kMaxWatches) return false;
        watches_[static_cast<std::size_t>(nwatches_)] =
            Watch{aeron::concurrent::AtomicBuffer(w.buffer, w.length), w.capacity, w.headPositionIndex, enabled};
        ++nwatches_;
        return true;
    }
    [[nodiscard]] int watchCount() const noexcept { return nwatches_; }

    // -- consumer (the owner thread) ----------------------------------------------------------------------

    /// Announce a park, then re-check every watched ring.  true = nothing committed is waiting: park (on fd()).
    /// false = a watched ring holds a committed record: do NOT park (run the pass; the parking word is clear).
    [[nodiscard]] bool armForPark() noexcept {
        markParking();
        if (anyWatchReady()) {
            parking_.store(0, std::memory_order_relaxed);
            return false;
        }
        return true;
    }
    /// After the park.  `fired`: fd() was reported readable (drain it so the next park is not cut short).
    void disarm(bool fired) noexcept {
        parking_.store(0, std::memory_order_relaxed);
        if (fired) drain_();
    }

    /// armForPark's two steps, exposed so tests can interleave a producer between them (deterministic seams).
    void markParking() noexcept {
        parking_.store(1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
    [[nodiscard]] bool anyWatchReady() const noexcept {
        for (int i = 0; i < nwatches_; ++i) {
            const Watch& w = watches_[static_cast<std::size_t>(i)];
            if (w.enabled != nullptr && !*w.enabled) continue;
            const std::int64_t head = w.buf.getInt64(w.headPositionIndex);   // consumer-owned
            const auto idx = static_cast<aeron::util::index_t>(head & (w.capacity - 1));
            if (w.buf.getInt32Volatile(idx) > 0) return true;                 // committed record or padding
        }
        return false;
    }

    // -- producer (any thread) ------------------------------------------------------------------------------

    /// After publishing a record: wake the consumer iff it is parking.  One fence + one load otherwise.
    void ringIfParked() noexcept {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (parking_.load(std::memory_order_relaxed) != 0 &&
            parking_.exchange(0, std::memory_order_acq_rel) != 0)
            ring();
    }
    /// Unconditional wake (space wait: the consumer of a ring saw its producer's want-space word).
    void ring() noexcept {
#if defined(__linux__)
        const std::uint64_t one = 1;   // eventfd: an 8-byte counter add
#else
        const std::uint8_t one = 1;    // pipe: one byte (review M-1: never a partial / oversized write)
#endif
        ssize_t r;
        do { r = ::write(writeFd_, &one, sizeof(one)); } while (r < 0 && errno == EINTR);
        // EAGAIN: the eventfd counter / pipe is already full, i.e. already readable — the consumer is woken
        // either way, and it drains everything when it wakes (drain_ reads until EAGAIN).
    }

    // -- diagnostics / tests ---------------------------------------------------------------------------------
    [[nodiscard]] bool parkingForTest() const noexcept { return parking_.load(std::memory_order_relaxed) != 0; }
    /// fd() readable right now (a zero-timeout poll).
    [[nodiscard]] bool firedForTest() const noexcept;

private:
    struct Watch {
        aeron::concurrent::AtomicBuffer buf;
        aeron::util::index_t            capacity          = 0;
        aeron::util::index_t            headPositionIndex = 0;
        const bool*                     enabled           = nullptr;
    };

    void drain_() noexcept {
        std::uint64_t sink[8];
        while (::read(readFd_, sink, sizeof(sink)) > 0) {}
    }

    alignas(64) std::atomic<std::uint32_t> parking_{0};
    alignas(64) std::array<Watch, kMaxWatches> watches_{};
    int nwatches_ = 0;
    int readFd_   = -1;
    int writeFd_  = -1;
};

} // namespace rune::consensus::detail

#include <poll.h>
inline bool rune::consensus::detail::RingDoorbell::firedForTest() const noexcept {
    pollfd p{readFd_, POLLIN, 0};
    return ::poll(&p, 1, 0) > 0;
}
