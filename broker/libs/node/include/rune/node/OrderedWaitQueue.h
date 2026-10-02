#pragma once
// OrderedWaitQueue.h — F15 S5 fix round 1 (review I-1): the parked-waiter
// container of a worker (acks=all waiters, Strict durable waiters).
//
// Requirements:
//   * answers leave in PARK order (pipelined batches of one partition are
//     answered in sequence order — re-review 2 minor);
//   * firing the FRONT is O(1): waiters overwhelmingly fire from the front
//     (the HW is monotone, fsyncs complete in submit order), so a pass that
//     fires k of n waiters must not move the n - k survivors (the
//     order-preserving vector erase did: ~200 B moved per survivor per pass);
//   * no allocation on the hot path (reserve()d, cold growth only past it).
//
// Structure: a ring of {value, live} slots between head_ and tail_ (monotone
// counters, power-of-two ring: index = counter & mask).  sweep(take) visits the LIVE entries in order; an entry `take`
// claims is marked dead in place (no move) and dead slots are popped off the
// front — O(1) per fired front entry.  A dead slot in the middle (an entry
// behind a still-waiting older one fired first, e.g. another partition) stays
// a tombstone until the front reaches it.  push_back on a full ring first
// compacts the tombstones out (O(n), amortised O(1): the ring holds twice the
// owed cap, so a compaction frees at least half of it); only if still full
// does it grow (cold, counted).
//
// Single owner thread.  No atomics, no virtual calls, no std::function
// (template visitors).  T must be default-constructible and move-assignable.
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

namespace rune::node {

template <typename T>
class OrderedWaitQueue {
public:
    /// Reserve room for `n` LIVE entries (ring = 2n slots).  Cold.
    void reserve(std::size_t n) {
        const std::size_t want = pow2_(n * 2);
        if (want <= slots_.size()) return;
        regrow_(want);
    }

    [[nodiscard]] std::size_t size() const noexcept { return live_; }
    [[nodiscard]] bool empty() const noexcept { return live_ == 0; }
    [[nodiscard]] std::size_t capacitySlots() const noexcept { return slots_.size(); }
    [[nodiscard]] uint64_t growths() const noexcept { return growths_; }
    [[nodiscard]] uint64_t compactions() const noexcept { return compactions_; }

    void push_back(T&& v) {
        if (tail_ - head_ == slots_.size()) [[unlikely]] makeRoom_();
        Slot& s = slots_[idx_(tail_)];
        s.v = std::move(v);
        s.live = true;
        ++tail_;
        ++live_;
    }

    /// Visit live entries in park order; `take(T&)` returning true removes the
    /// entry (the visitor has fired / failed it).  Returns the number removed.
    /// The visitor must not push into THIS queue.
    template <typename Take>
    std::size_t sweep(Take&& take) {
        std::size_t removed = 0;
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (!s.live) continue;
            if (!take(s.v)) continue;
            s.live = false;
            s.v = T{};          // release payload resources now (cold for POD-ish T)
            --live_;
            ++removed;
        }
        popDeadFront_();
        return removed;
    }

    /// Visit live entries in park order (read / in-place update, no removal).
    template <typename Fn>
    void forEach(Fn&& fn) {
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (s.live) fn(s.v);
        }
    }
    template <typename Fn>
    void forEach(Fn&& fn) const {
        for (uint64_t i = head_; i < tail_; ++i) {
            const Slot& s = slots_[idx_(i)];
            if (s.live) fn(s.v);
        }
    }

    /// The k-th live entry in park order (tests; O(n)).
    [[nodiscard]] const T& at(std::size_t k) const {
        for (uint64_t i = head_; i < tail_; ++i) {
            const Slot& s = slots_[idx_(i)];
            if (s.live && k-- == 0) return s.v;
        }
        throw std::out_of_range("OrderedWaitQueue::at");
    }

private:
    struct Slot {
        T    v{};
        bool live = false;
    };
    [[nodiscard]] std::size_t idx_(uint64_t i) const noexcept { return static_cast<std::size_t>(i) & mask_; }
    static std::size_t pow2_(std::size_t n) noexcept {
        std::size_t p = 16;
        while (p < n) p <<= 1;
        return p;
    }
    void popDeadFront_() noexcept {
        while (head_ < tail_ && !slots_[idx_(head_)].live) ++head_;
        if (head_ == tail_) head_ = tail_ = 0;   // empty: restart at slot 0 (cache locality)
    }
    void makeRoom_() {
        if (slots_.empty()) { regrow_(16); return; }
        if (live_ < slots_.size()) { compact_(); if (tail_ - head_ < slots_.size()) return; }
        ++growths_;
        regrow_(slots_.size() * 2);   // cold: only past the reserved capacity
    }
    // Close the tombstone gaps in place, keeping order (O(n), cold/amortised).
    void compact_() {
        ++compactions_;
        uint64_t w = head_;
        for (uint64_t r = head_; r < tail_; ++r) {
            Slot& s = slots_[idx_(r)];
            if (!s.live) continue;
            if (w != r) {
                Slot& d = slots_[idx_(w)];
                d.v = std::move(s.v);
                d.live = true;
                s.v = T{};
                s.live = false;
            }
            ++w;
        }
        tail_ = w;
    }
    void regrow_(std::size_t n) {
        n = pow2_(n);
        std::vector<Slot> next(n);
        std::size_t k = 0;
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (!s.live) continue;
            next[k].v = std::move(s.v);
            next[k].live = true;
            ++k;
        }
        slots_ = std::move(next);
        mask_ = slots_.size() - 1;
        head_ = 0;
        tail_ = k;
    }

    std::vector<Slot> slots_;
    std::size_t mask_ = 0;   ///< slots_.size() - 1 (power of two)
    uint64_t head_ = 0;
    uint64_t tail_ = 0;
    std::size_t live_ = 0;
    uint64_t growths_ = 0;
    uint64_t compactions_ = 0;
};

} // namespace rune::node
