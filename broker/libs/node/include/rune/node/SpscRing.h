#pragma once
// SpscRing.h — E2 inc2a: bounded lock-free single-producer/single-consumer ring.
//
// DESIGN NOTES
// ============
// Memory ordering discipline (mirrored on Aeron OneToOneRingBuffer):
//   Producer: writes element then stores tail with memory_order_release.
//   Consumer: loads head with memory_order_acquire before reading element,
//             stores head with memory_order_release after consuming.
//
// Since E2 inc2a is cooperative (single control thread), the acquire/release
// are NO-OPS at runtime on x86/aarch64 (release == seq_cst store is at most a
// StoreStore fence on aarch64 which is implied by the sequential order anyway).
// However, they are REQUIRED by the C++ memory model: removing them would be UB
// once inc2b makes workers real threads.  We pay zero cost now and get correctness
// for free.
//
// Capacity: must be a power of two (asserted at construction).  The mask trick
// avoids expensive modulo on every index advance.
//
// Cache-line padding: the head and tail counters are placed on separate cache
// lines to prevent false sharing when inc2b makes the producer and consumer
// run on distinct hardware threads.
//
// Back-pressure: tryEnqueue returns false (never drops, never overwrites).
// The caller must retry on the next duty cycle.
//
// No allocation on the hot path: all storage is pre-allocated in the ring array.
//
// T must be:
//   - Default-constructible (ring storage is value-initialised)
//   - Move-assignable (stored by move on enqueue)

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>       // std::hardware_destructive_interference_size
#include <utility>   // std::move
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// SpscRing<T>
// ---------------------------------------------------------------------------
template <typename T>
class SpscRing {
public:
    // Construct a ring with the given capacity.
    // capacity must be a power of two and >= 2.
    explicit SpscRing(std::size_t capacity)
        : capacity_(capacity)
        , mask_(capacity - 1)
        , buf_(capacity)
    {
        assert(capacity >= 2 && "SpscRing: capacity must be >= 2");
        assert((capacity & mask_) == 0 && "SpscRing: capacity must be a power of two");
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    // Non-copyable, non-movable (atomic members + stable address for buf_).
    SpscRing(const SpscRing&)            = delete;
    SpscRing& operator=(const SpscRing&) = delete;
    SpscRing(SpscRing&&)                 = delete;
    SpscRing& operator=(SpscRing&&)      = delete;

    // -----------------------------------------------------------------------
    // tryEnqueue — producer side (control thread).
    //
    // Attempts to move-store `item` into the next available slot.
    // Returns true on success.  Returns false (back-pressure) if the ring is
    // full — the item is NOT modified.  Never blocks, never drops.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool tryEnqueue(T item) noexcept(noexcept(T(std::move(item)))) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t head = head_.load(std::memory_order_acquire);
        if (tail - head >= capacity_) {
            // Ring full — back-pressure.
            return false;
        }
        buf_[tail & mask_] = std::move(item);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // -----------------------------------------------------------------------
    // drain — consumer side (worker doWork).
    //
    // For each available element (in order), calls fn(T&&) and then advances
    // the head pointer.  Returns the number of elements consumed.
    // fn must not call tryEnqueue (non-reentrant); fn may call tryReturnEnqueue.
    // -----------------------------------------------------------------------
    template <typename Fn>
    std::size_t drain(Fn&& fn) noexcept(noexcept(fn(std::declval<T&&>()))) {
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        std::size_t head       = head_.load(std::memory_order_relaxed);
        std::size_t consumed   = 0;
        while (head != tail) {
            fn(std::move(buf_[head & mask_]));
            ++head;
            ++consumed;
        }
        if (consumed > 0) {
            head_.store(head, std::memory_order_release);
        }
        return consumed;
    }

    // -----------------------------------------------------------------------
    // size — approximate number of elements currently in the ring.
    // Approximate because the producer and consumer may be on different threads
    // in inc2b.  Safe on the single-thread cooperative path (exact).
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t size() const noexcept {
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        const std::size_t head = head_.load(std::memory_order_acquire);
        return tail - head;
    }

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

private:
    static constexpr std::size_t kCacheLineSize =
#ifdef __cpp_lib_hardware_interference_size
        std::hardware_destructive_interference_size;
#else
        64u;
#endif

    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<T>    buf_;  // pre-allocated ring storage

    // Producer-owned: written by producer, read by consumer.
    // Aligned to its own cache line to prevent false sharing.
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};

    // Consumer-owned: written by consumer, read by producer.
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
};

} // namespace rune::node
