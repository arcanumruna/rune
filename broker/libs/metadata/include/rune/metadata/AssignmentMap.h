// AssignmentMap.h — compact, cache-friendly open-addressing hash map.
//
// Key   = (topicId: int32, partition: int32) packed as uint64.
// Value = PartitionAssignment.
//
// Design mirrors Aeron/Agrona collection style:
//   - Power-of-2 capacity with bit-mask addressing.
//   - Linear probing.
//   - Sentinel key (EMPTY_KEY = 0xFFFFFFFF'FFFFFFFF) — no per-op heap allocation
//     during steady-state get/put after the initial table allocation.
//   - Auto-rehash at load factor >= 0.6.
//
// NOT thread-safe.

#pragma once

#include "rune/metadata/PartitionAssignment.h"

#include <cstdint>
#include <cstring>
#include <cassert>
#include <functional>
#include <optional>
#include <vector>

namespace rune::metadata {

class AssignmentMap {
public:
    static constexpr uint64_t EMPTY_KEY   = ~uint64_t{0};   // 0xFFFFFFFFFFFFFFFF
    static constexpr uint64_t DELETED_KEY = ~uint64_t{1};   // 0xFFFFFFFFFFFFFFFE

    static constexpr float    LOAD_FACTOR = 0.60f;
    static constexpr int      INITIAL_CAP = 16;             // must be power of 2

    // ── Construction ───────────────────────────────────────────────────────────

    AssignmentMap() { allocate(INITIAL_CAP); }

    AssignmentMap(const AssignmentMap&)            = default;
    AssignmentMap& operator=(const AssignmentMap&) = default;
    AssignmentMap(AssignmentMap&&)                 = default;
    AssignmentMap& operator=(AssignmentMap&&)      = default;

    // ── Core API ──────────────────────────────────────────────────────────────

    /// Look up (topicId, partition).  Returns nullptr if absent.
    const PartitionAssignment* get(int32_t topicId, int32_t partition) const noexcept
    {
        const uint64_t k = makeKey(topicId, partition);
        const int      mask = capacity_ - 1;
        int idx = hashIndex(k, mask);
        for (;;) {
            const uint64_t ek = keys_[idx];
            if (ek == EMPTY_KEY)   return nullptr;
            if (ek == k)           return &values_[idx];
            // DELETED_KEY: keep probing
            idx = (idx + 1) & mask;
        }
    }

    /// Non-const overload.
    PartitionAssignment* get(int32_t topicId, int32_t partition) noexcept
    {
        const uint64_t k = makeKey(topicId, partition);
        const int      mask = capacity_ - 1;
        int idx = hashIndex(k, mask);
        for (;;) {
            const uint64_t ek = keys_[idx];
            if (ek == EMPTY_KEY)   return nullptr;
            if (ek == k)           return &values_[idx];
            idx = (idx + 1) & mask;
        }
    }

    /// Insert or overwrite (topicId, partition) → pa.
    void put(int32_t topicId, int32_t partition, const PartitionAssignment& pa)
    {
        maybeRehash();
        const uint64_t k = makeKey(topicId, partition);
        int firstDeleted = -1;
        const int mask = capacity_ - 1;
        int idx = hashIndex(k, mask);
        for (;;) {
            const uint64_t ek = keys_[idx];
            if (ek == EMPTY_KEY) {
                // Use firstDeleted slot if we passed one
                int slot = (firstDeleted >= 0) ? firstDeleted : idx;
                keys_[slot]   = k;
                values_[slot] = pa;
                ++size_;
                return;
            }
            if (ek == DELETED_KEY) {
                if (firstDeleted < 0) firstDeleted = idx;
                idx = (idx + 1) & mask;
                continue;
            }
            if (ek == k) {
                values_[idx] = pa;  // overwrite
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    /// Remove (topicId, partition).  No-op if absent.
    void remove(int32_t topicId, int32_t partition) noexcept
    {
        const uint64_t k = makeKey(topicId, partition);
        const int      mask = capacity_ - 1;
        int idx = hashIndex(k, mask);
        for (;;) {
            const uint64_t ek = keys_[idx];
            if (ek == EMPTY_KEY) return;           // not found
            if (ek == k) {
                keys_[idx] = DELETED_KEY;
                --size_;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    // ── Iteration ─────────────────────────────────────────────────────────────

    /// Visit every live entry in insertion-independent order.
    /// Fn: void(int32_t topicId, int32_t partition, const PartitionAssignment&)
    template<typename Fn>
    void forEach(Fn&& fn) const
    {
        for (int i = 0; i < capacity_; ++i) {
            const uint64_t k = keys_[i];
            if (k == EMPTY_KEY || k == DELETED_KEY) continue;
            fn(static_cast<int32_t>(k >> 32),
               static_cast<int32_t>(k & 0xFFFFFFFFu),
               values_[i]);
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    int size()     const noexcept { return size_;     }
    int capacity() const noexcept { return capacity_; }
    bool empty()   const noexcept { return size_ == 0; }

    /// Reset to empty (keeps allocated storage).
    void clear() noexcept
    {
        for (int i = 0; i < capacity_; ++i) keys_[i] = EMPTY_KEY;
        size_ = 0;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

private:
    static uint64_t makeKey(int32_t topicId, int32_t partition) noexcept
    {
        // Pack: high 32 = topicId, low 32 = partition.
        // NOTE: the EMPTY_KEY sentinel is 0xFFFFFFFF'FFFFFFFF.
        // topicId = -1 (NO_LEADER) and partition = -1 together would collide.
        // In practice, partition -1 is never a valid partition index, so this
        // is safe.  We assert defensively here.
        assert(topicId   != -1 || partition != -1);
        return (static_cast<uint64_t>(static_cast<uint32_t>(topicId))   << 32)
              |  static_cast<uint64_t>(static_cast<uint32_t>(partition));
    }

    static int hashIndex(uint64_t k, int mask) noexcept
    {
        // Fibonacci (Knuth) multiplicative hash for 64-bit keys, reduced to table size.
        const uint64_t h = k * UINT64_C(11400714819323198485);
        return static_cast<int>((h >> 32) & static_cast<uint64_t>(mask));
    }

    void allocate(int cap)
    {
        assert((cap & (cap - 1)) == 0);  // must be power-of-2
        capacity_ = cap;
        keys_.assign(cap, EMPTY_KEY);
        values_.resize(cap);
        size_ = 0;
    }

    void maybeRehash()
    {
        // Resize when size exceeds load factor threshold
        if (size_ + 1 <= static_cast<int>(capacity_ * LOAD_FACTOR)) return;
        const int newCap = capacity_ * 2;
        std::vector<uint64_t>          oldKeys   = std::move(keys_);
        std::vector<PartitionAssignment> oldValues = std::move(values_);
        const int oldCap = capacity_;
        allocate(newCap);
        for (int i = 0; i < oldCap; ++i) {
            const uint64_t k = oldKeys[i];
            if (k == EMPTY_KEY || k == DELETED_KEY) continue;
            // Re-insert without re-checking load (we already doubled)
            const int mask = capacity_ - 1;
            int idx = hashIndex(k, mask);
            for (;;) {
                if (keys_[idx] == EMPTY_KEY) {
                    keys_[idx]   = k;
                    values_[idx] = std::move(oldValues[i]);
                    ++size_;
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
    }

    std::vector<uint64_t>           keys_;
    std::vector<PartitionAssignment> values_;
    int                              capacity_ = 0;
    int                              size_     = 0;
};

} // namespace rune::metadata
