#pragma once
// SessionCryptoTable.h - a worker's per-session crypto state, slot-indexed (authentication Task 13, fix-on-sight).
//
// Replaces Worker::findSessionCrypto_'s linear scan over every encrypted session, which ran on EVERY produce frame
// (O(sessions) on the hot path).  Same structure as SessionAckSink's session table: a fixed power-of-two open-addressing
// table keyed by session id, linear probing, backward-shift deletion, so a lookup is one hash and a short probe chain.
// Worker-thread owned (shared-nothing).  The two arrays are allocated ONCE, on the first insert (cold), so a plaintext
// deployment pays nothing; lookups never allocate.
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace rune::node {

template <class Entry, std::size_t Slots = 8192>
class SessionCryptoTable {
    static_assert(std::has_single_bit(Slots), "Slots must be a power of two");
public:
    static constexpr std::size_t kSlots = Slots;

    [[nodiscard]] std::size_t size() const noexcept { return count_; }
    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }

    /// The hot lookup: one multiply-shift hash + the probe chain.  nullptr = unknown (plaintext) session.
    [[nodiscard]] Entry* find(int64_t sid) noexcept {
        const std::size_t i = findIndex_(sid);
        return i == kNone ? nullptr : &vals_[i];
    }
    [[nodiscard]] const Entry* find(int64_t sid) const noexcept {
        const std::size_t i = findIndex_(sid);
        return i == kNone ? nullptr : &vals_[i];
    }

    /// The entry for `sid`, created (default-constructed) when absent.  nullptr only when the table is full (the
    /// runtime caps sessions below kSlots, so the caller treats it as an invariant violation).
    [[nodiscard]] Entry* findOrInsert(int64_t sid) {
        if (Entry* e = find(sid)) return e;
        if (count_ + 1 >= Slots) return nullptr;                 // one empty slot ends every probe
        if (!keys_) { keys_ = std::make_unique<int64_t[]>(Slots); vals_ = std::make_unique<Entry[]>(Slots); }
        std::size_t i = hash_(sid);
        while (keys_[i] != 0) i = (i + 1) & kMask;
        keys_[i] = sid;
        vals_[i] = Entry{};
        ++count_;
        return &vals_[i];
    }

    void erase(int64_t sid) noexcept {
        std::size_t i = findIndex_(sid);
        if (i == kNone) return;
        keys_[i] = 0;
        vals_[i] = Entry{};
        --count_;
        std::size_t j = i;
        for (;;) {                                               // backward-shift deletion
            j = (j + 1) & kMask;
            if (keys_[j] == 0) return;
            const std::size_t home = hash_(keys_[j]);
            const bool stays = (i <= j) ? (i < home && home <= j) : (i < home || home <= j);
            if (stays) continue;
            keys_[i] = keys_[j];
            vals_[i] = std::move(vals_[j]);
            keys_[j] = 0;
            vals_[j] = Entry{};
            i = j;
        }
    }

    /// Test seam: slots examined by the last find() (1 = direct hit).
    [[nodiscard]] std::size_t lastProbesForTest() const noexcept { return lastProbes_; }

private:
    static constexpr std::size_t kNone = static_cast<std::size_t>(-1);
    static constexpr std::size_t kMask = Slots - 1;
    static constexpr int         kBits = std::countr_zero(Slots);
    static std::size_t hash_(int64_t sid) noexcept {
        return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - kBits));
    }
    [[nodiscard]] std::size_t findIndex_(int64_t sid) const noexcept {
        if (count_ == 0) { lastProbes_ = 0; return kNone; }
        std::size_t i = hash_(sid);
        std::size_t probes = 1;
        for (std::size_t n = 0; n < Slots; ++n, i = (i + 1) & kMask, ++probes) {
            if (keys_[i] == sid) { lastProbes_ = probes; return i; }
            if (keys_[i] == 0)   { lastProbes_ = probes; return kNone; }
        }
        lastProbes_ = probes;
        return kNone;
    }

    std::unique_ptr<int64_t[]> keys_;      // 0 = empty
    std::unique_ptr<Entry[]>   vals_;
    std::size_t                count_ = 0;
    mutable std::size_t        lastProbes_ = 0;
};

} // namespace rune::node
