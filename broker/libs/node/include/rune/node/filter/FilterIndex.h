#pragma once
// FilterIndex — Phase 2c: index value -> members registered under it.  Open addressing (linear probing,
// backward-shift delete), capped at a configured number of distinct values.  Owned by one worker thread
// (shared-nothing; no locks, no atomics).  Cold: init / add / remove.  Hot: forEach — no allocation.
#include "rune/codec/FilterWire.h"

#include <cstdint>
#include <vector>

namespace rune::node::filter {

inline constexpr uint32_t kNil = UINT32_MAX;

class FilterIndex {
public:
    /// Cold: slots = next power of two >= 2 * maxValues; entries reserved to `reserveEntries`.
    void init(uint32_t maxValues, uint32_t reserveEntries) {
        maxValues_ = maxValues;
        uint32_t n = 2;
        while (n < 2 * maxValues) n <<= 1;
        slots_.assign(n, Slot{});
        mask_ = n - 1;
        size_ = 0;
        entries_.clear();
        entries_.reserve(reserveEntries);
        freeEntry_ = kNil;
    }

    /// Cold (Subscribe).  false = `maxValues` distinct values are already registered and k is new (nothing changed).
    [[nodiscard]] bool add(rune::codec::FilterKey k, uint32_t member) {
        uint32_t i = hash_(k);
        while (slots_[i].refs != 0 && slots_[i].key != k) i = (i + 1) & mask_;
        Slot& s = slots_[i];
        if (s.refs == 0 && size_ >= maxValues_) return false;
        uint32_t e;
        if (freeEntry_ != kNil) {
            e          = freeEntry_;
            freeEntry_ = entries_[e].next;
        } else {
            e = static_cast<uint32_t>(entries_.size());
            entries_.push_back(Entry{});   // may grow past the reserve: cold path only
        }
        if (s.refs == 0) {
            s.key  = k;
            s.head = kNil;
            ++size_;
        }
        entries_[e] = Entry{member, s.head};
        s.head      = e;
        ++s.refs;
        return true;
    }

    /// Cold (Unsubscribe).  Removes one (k, member) registration; the last one frees the value (backward-shift delete).
    void remove(rune::codec::FilterKey k, uint32_t member) noexcept {
        if (slots_.empty()) return;
        uint32_t i = hash_(k);
        while (slots_[i].refs != 0 && slots_[i].key != k) i = (i + 1) & mask_;
        Slot& s = slots_[i];
        if (s.refs == 0) return;
        uint32_t prev = kNil, e = s.head;
        while (e != kNil && entries_[e].member != member) { prev = e; e = entries_[e].next; }
        if (e == kNil) return;
        if (prev == kNil) s.head = entries_[e].next; else entries_[prev].next = entries_[e].next;
        entries_[e].next = freeEntry_;
        freeEntry_       = e;
        if (--s.refs != 0) return;
        --size_;
        // Backward-shift: pull later cluster members into the hole when their home lies at or before it.
        uint32_t hole = i, j = i;
        for (;;) {
            j = (j + 1) & mask_;
            if (slots_[j].refs == 0) break;
            const uint32_t home = hash_(slots_[j].key);
            if (((j - home) & mask_) >= ((j - hole) & mask_)) {
                slots_[hole] = slots_[j];
                hole         = j;
            }
        }
        slots_[hole] = Slot{};
    }

    /// Hot: fn(uint32_t member) for every member registered under k.  No allocation; one probe sequence.
    template <class Fn> void forEach(rune::codec::FilterKey k, Fn&& fn) const noexcept {
        if (slots_.empty()) return;
        uint32_t i = hash_(k);
        while (slots_[i].refs != 0) {
            if (slots_[i].key == k) {
                for (uint32_t e = slots_[i].head; e != kNil; e = entries_[e].next) fn(entries_[e].member);
                return;
            }
            i = (i + 1) & mask_;
        }
    }

    [[nodiscard]] uint32_t distinctValues() const noexcept { return size_; }
    [[nodiscard]] bool     initialized() const noexcept { return !slots_.empty(); }

private:
    struct Slot  { rune::codec::FilterKey key{}; uint32_t head = kNil; uint32_t refs = 0; };   // refs 0 = empty slot
    struct Entry { uint32_t member = kNil; uint32_t next = kNil; };
    [[nodiscard]] uint32_t hash_(rune::codec::FilterKey k) const noexcept {
        const uint64_t h = (k.lo * 0x9E3779B97F4A7C15ull) ^ (k.hi * 0xC2B2AE3D27D4EB4Full);
        return static_cast<uint32_t>(h >> 32) & mask_;
    }
    std::vector<Slot>  slots_;
    std::vector<Entry> entries_;   // reserved at init; a free list reuses removed entries
    uint32_t freeEntry_ = kNil, mask_ = 0, size_ = 0, maxValues_ = 0;
};

}  // namespace rune::node::filter
