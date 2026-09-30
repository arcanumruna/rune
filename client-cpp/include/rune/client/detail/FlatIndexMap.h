// include/rune/client/detail/FlatIndexMap.h
//
// Open-addressing int64 -> uint32 map for hot-path lookups (spec §1a.5: no
// node-based containers on per-record / per-batch / per-duty-cycle paths).
// Linear probing over a power-of-two slot array kept at load factor <= 1/2.
// find() never allocates; insert() allocates only when it grows (first sight of
// a key = cold path).  erase() uses backward-shift deletion (no tombstones, so
// probe runs never degrade).  Single-threaded: owned by one agent/shard.
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::client::detail {

class FlatIndexMap {
public:
    static constexpr uint32_t kNone = 0xFFFFFFFFu;

    explicit FlatIndexMap(uint32_t initialCapacityPow2 = 16)
        : slots_(roundUpPow2(initialCapacityPow2 < 4 ? 4 : initialCapacityPow2)),
          mask_(static_cast<uint32_t>(slots_.size() - 1)) {}

    [[nodiscard]] uint32_t find(int64_t key) const noexcept {
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            const Slot& s = slots_[i];
            if (!s.used) return kNone;
            if (s.key == key) return s.value;
            i = (i + 1) & mask_;
        }
    }

    /// Inserts or overwrites.  Returns true when the key was not present.
    bool insert(int64_t key, uint32_t value) {
        if ((static_cast<std::size_t>(size_) + 1) * 2 > slots_.size()) grow();
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            Slot& s = slots_[i];
            if (!s.used) {
                s = Slot{key, value, true};
                ++size_;
                return true;
            }
            if (s.key == key) {
                s.value = value;
                return false;
            }
            i = (i + 1) & mask_;
        }
    }

    bool erase(int64_t key) noexcept {
        uint32_t i = static_cast<uint32_t>(mix(key)) & mask_;
        for (;;) {
            if (!slots_[i].used) return false;
            if (slots_[i].key == key) break;
            i = (i + 1) & mask_;
        }
        uint32_t hole = i;
        uint32_t j    = i;
        for (;;) {
            j = (j + 1) & mask_;
            if (!slots_[j].used) break;
            const uint32_t home = static_cast<uint32_t>(mix(slots_[j].key)) & mask_;
            // Slot j stays iff its home lies cyclically in (hole, j].
            const bool stays = (hole <= j) ? (home > hole && home <= j)
                                           : (home > hole || home <= j);
            if (stays) continue;
            slots_[hole] = slots_[j];
            hole = j;
        }
        slots_[hole].used = false;
        --size_;
        return true;
    }

    [[nodiscard]] uint32_t size() const noexcept { return size_; }
    [[nodiscard]] uint32_t capacity() const noexcept {
        return static_cast<uint32_t>(slots_.size());
    }

    template <class F>
    void forEach(F&& f) const {
        for (const Slot& s : slots_) {
            if (s.used) f(s.key, s.value);
        }
    }

private:
    struct Slot {
        int64_t  key   = 0;
        uint32_t value = 0;
        bool     used  = false;
    };

    static uint64_t mix(int64_t k) noexcept {  // murmur3 fmix64
        uint64_t x = static_cast<uint64_t>(k);
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }

    static std::size_t roundUpPow2(uint32_t v) noexcept {
        std::size_t p = 1;
        while (p < v) p <<= 1;
        return p;
    }

    void grow() {
        std::vector<Slot> old;
        old.swap(slots_);
        slots_.assign(old.size() * 2, Slot{});
        mask_ = static_cast<uint32_t>(slots_.size() - 1);
        size_ = 0;
        for (const Slot& s : old) {
            if (s.used) insert(s.key, s.value);
        }
    }

    std::vector<Slot> slots_;
    uint32_t          mask_ = 0;
    uint32_t          size_ = 0;
};

} // namespace rune::client::detail
