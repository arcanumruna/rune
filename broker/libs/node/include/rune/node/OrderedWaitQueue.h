#pragma once
// OrderedWaitQueue.h — F15 S5 fix rounds 1-2 (review I-1, re-review I-2): the
// parked-waiter container of a worker (acks=all waiters, Strict durable waiters).
//
// Requirements:
//   * answers leave in PARK order (pipelined batches of one partition are
//     answered in sequence order — re-review 2 minor);
//   * firing the FRONT is O(1): waiters overwhelmingly fire from the front
//     (the HW is monotone, fsyncs complete in submit order), so a pass that
//     fires k of n waiters must not move the n - k survivors (the
//     order-preserving vector erase did: ~200 B moved per survivor per pass);
//   * the per-pass cost is bounded by LIVE work (re-review I-2): a stalled
//     front waiter (a lagging follower, a slow fsync) must not make every pass
//     walk the tombstones the other partitions leave behind it;
//   * no allocation on the hot path (reserve()d; cold growth only past it).
//
// Structure: a power-of-two ring of payload slots plus a DENSE parallel byte
// array of live flags (skipping a tombstone touches one byte, never a ~200 B
// payload line), between head_ and tail_ (monotone counters; index =
// counter & mask).  sweep(take) visits the LIVE entries in order; an entry
// `take` claims is marked dead in place (no move) and dead slots are popped
// off the front — O(1) per fired front entry.  A dead slot in the middle
// (fired behind a still-waiting older entry) stays a tombstone until the
// front reaches it OR until the window span exceeds 2 × live + kSlack: then
// the window is compacted in order (eager compaction).  A compaction costs
// O(span) = O(live + kSlack) and runs only after more than live + kSlack
// removals since the last one — amortised O(1) per removal, and every pass
// walks at most 2 × live + kSlack slots (spanForTest()).  push_back on a full
// ring compacts first; only if still full does it grow (cold, counted).
//
// Single owner thread.  No atomics, no virtual calls, no std::function
// (template visitors).  T must be default-constructible and move-assignable.
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

namespace rune::node {

template <typename T>
class OrderedWaitQueue {
public:
    /// Span slack above 2 × live before an eager compaction (bounds the
    /// compaction rate for tiny queues).
    static constexpr std::size_t kSlack = 64;

    /// Reserve room for `n` LIVE entries (ring = pow2 >= 2n + kSlack).  Cold.
    void reserve(std::size_t n) {
        const std::size_t want = pow2_(n * 2 + kSlack);
        if (want <= slots_.size()) return;
        regrow_(want);
    }

    [[nodiscard]] std::size_t size() const noexcept { return live_; }
    [[nodiscard]] bool empty() const noexcept { return live_ == 0; }
    [[nodiscard]] std::size_t capacitySlots() const noexcept { return slots_.size(); }
    [[nodiscard]] uint64_t growths() const noexcept { return growths_; }
    [[nodiscard]] uint64_t compactions() const noexcept { return compactions_; }
    /// Slots between head and tail (live + tombstones) — what a pass walks.
    [[nodiscard]] std::size_t spanForTest() const noexcept { return static_cast<std::size_t>(tail_ - head_); }
    /// Test seam (N-2): the head counter (where the window starts in the ring).
    [[nodiscard]] uint64_t headForTest() const noexcept { return head_; }

    void push_back(T&& v) {
        assert(!sweeping_ && "OrderedWaitQueue: a sweep visitor pushed into the queue it sweeps");
        if (tail_ - head_ == slots_.size()) [[unlikely]] makeRoom_();
        const std::size_t i = idx_(tail_);
        slots_[i] = std::move(v);
        live_flags_[i] = 1;
        ++tail_;
        ++live_;
    }

    /// Visit live entries in park order; `take(T&)` returning true removes the
    /// entry (the visitor has fired / failed it).  Returns the number removed.
    /// The visitor must not push into THIS queue (debug-asserted, N-3).
    template <typename Take>
    std::size_t sweep(Take&& take) {
        assert(!sweeping_ && "OrderedWaitQueue: re-entrant sweep");
#ifndef NDEBUG
        sweeping_ = true;
#endif
        std::size_t removed = 0;
        for (uint64_t c = head_; c < tail_; ++c) {
            const std::size_t i = idx_(c);
            if (live_flags_[i] == 0) continue;
            if (!take(slots_[i])) continue;
            live_flags_[i] = 0;
            slots_[i] = T{};        // release payload resources now (no allocation)
            --live_;
            ++removed;
        }
#ifndef NDEBUG
        sweeping_ = false;
#endif
        if (removed != 0) {
            popDeadFront_();
            if (tail_ - head_ > 2 * live_ + kSlack) [[unlikely]] compact_();   // I-2: span bounded by live
        }
        return removed;
    }

    /// Visit live entries in park order (read / in-place update, no removal).
    template <typename Fn>
    void forEach(Fn&& fn) {
        for (uint64_t c = head_; c < tail_; ++c) {
            const std::size_t i = idx_(c);
            if (live_flags_[i] != 0) fn(slots_[i]);
        }
    }
    template <typename Fn>
    void forEach(Fn&& fn) const {
        for (uint64_t c = head_; c < tail_; ++c) {
            const std::size_t i = idx_(c);
            if (live_flags_[i] != 0) fn(slots_[i]);
        }
    }

    /// The k-th live entry in park order (tests; O(n)).
    [[nodiscard]] const T& at(std::size_t k) const {
        for (uint64_t c = head_; c < tail_; ++c) {
            const std::size_t i = idx_(c);
            if (live_flags_[i] != 0 && k-- == 0) return slots_[i];
        }
        throw std::out_of_range("OrderedWaitQueue::at");
    }

private:
    [[nodiscard]] std::size_t idx_(uint64_t c) const noexcept { return static_cast<std::size_t>(c) & mask_; }
    static std::size_t pow2_(std::size_t n) noexcept {
        std::size_t p = 16;
        while (p < n) p <<= 1;
        return p;
    }
    void popDeadFront_() noexcept {
        while (head_ < tail_ && live_flags_[idx_(head_)] == 0) ++head_;
        if (head_ == tail_) head_ = tail_ = 0;   // empty: restart at slot 0 (cache locality)
    }
    void makeRoom_() {
        if (slots_.empty()) { regrow_(16); return; }
        if (live_ < slots_.size()) { compact_(); if (tail_ - head_ < slots_.size()) return; }
        ++growths_;
        regrow_(slots_.size() * 2);   // cold: only past the reserved capacity
    }
    // Close the tombstone gaps in place, keeping order: O(span).  The survivors
    // slide toward the head (write cursor <= read cursor in counter space, so a
    // physically wrapped window is handled by the masked index).
    void compact_() {
        ++compactions_;
        uint64_t w = head_;
        for (uint64_t r = head_; r < tail_; ++r) {
            const std::size_t ri = idx_(r);
            if (live_flags_[ri] == 0) continue;
            if (w != r) {
                const std::size_t wi = idx_(w);
                slots_[wi] = std::move(slots_[ri]);
                live_flags_[wi] = 1;
                slots_[ri] = T{};
                live_flags_[ri] = 0;
            }
            ++w;
        }
        tail_ = w;
        if (head_ == tail_) head_ = tail_ = 0;
    }
    void regrow_(std::size_t n) {
        n = pow2_(n);
        std::vector<T> next(n);
        std::vector<uint8_t> nextLive(n, 0);
        std::size_t k = 0;
        for (uint64_t c = head_; c < tail_; ++c) {
            const std::size_t i = idx_(c);
            if (live_flags_[i] == 0) continue;
            next[k] = std::move(slots_[i]);
            nextLive[k] = 1;
            ++k;
        }
        slots_ = std::move(next);
        live_flags_ = std::move(nextLive);
        mask_ = slots_.size() - 1;
        head_ = 0;
        tail_ = k;
    }

    std::vector<T>       slots_;
    std::vector<uint8_t> live_flags_;   ///< dense: 1 byte per slot (tombstone skip never touches a payload)
    std::size_t mask_ = 0;              ///< slots_.size() - 1 (power of two)
    uint64_t head_ = 0;
    uint64_t tail_ = 0;
    std::size_t live_ = 0;
    uint64_t growths_ = 0;
    uint64_t compactions_ = 0;
#ifndef NDEBUG
    bool sweeping_ = false;
#endif
};

} // namespace rune::node
