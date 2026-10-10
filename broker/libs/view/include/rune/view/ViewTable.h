// libs/view/include/rune/view/ViewTable.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>
#include <rune/view/ViewRow.h>

namespace rune::view {

// Key -> row index + single-page row-payload slab for a materialized view's
// in-memory table.
//
// Phase 1 Task 3 scope: ONE preallocated slab (`slab_`, sized
// `initialIndexSlots` rows x `layout.rowLen`) and ONE preallocated open-
// addressing index (`tags_`/`keys_`/`rid_`, `initialIndexSlots` slots).
// Chunked growth across multiple slab pages is Task 4; incremental rehash
// of the index is Task 5 — this type asserts capacity instead of growing.
//
// The index key is the EXACT uint64_t view key (not a hash): `get`/`erase`
// compare `keys_[slot] == key` directly. `mix()` is used only to pick the
// open-addressing probe start slot, and `fullTag()` derives a 1-byte
// Swiss/F14-style tag used as a cheap pre-filter before the exact key
// compare.
//
// Deletes are O(1) and tombstone-free in the row PAYLOAD: `erase` performs
// backward-shift deletion over the index arrays (tags_/keys_/rid_) to keep
// the probe sequence for every other key intact, and returns the freed
// row-id to a free-list. It never moves or clears a row's payload bytes.
//
// Shared-nothing: ViewTable has no cross-thread state, no atomics, no
// locks, no thread_local. It is owned and driven by exactly one worker
// thread; nothing here is safe (or intended) to touch from a second thread.
// Zero heap allocation in put()/get()/erase() steady state: all index and
// slab storage is preallocated by the constructor, and the row-id
// free-list only grows (via erase()) — ops never allocate.
class ViewTable {
public:
    // `initialIndexSlots` must be a power of two (capacity of both the
    // open-addressing index and, for this single-page task, the row slab).
    // `chunkBytes` is accepted but unused until Task 4 (chunked slab
    // growth).
    explicit ViewTable(RowLayout layout, size_t initialIndexSlots = 4096,
                        size_t chunkBytes = 32u << 20);

    // Returns a writable pointer to the row for `key`. Inserts a zeroed row
    // on first use. Idempotent: calling put() again for an existing key
    // returns that same row's pointer without creating a duplicate entry.
    uint8_t* put(uint64_t key);

    // Returns a pointer to the row for `key`, or nullptr if absent.
    const uint8_t* get(uint64_t key) const noexcept;

    // Removes `key`. Returns false if `key` was not present. O(1):
    // backward-shift in the index, the row-id returned to the free-list;
    // the row payload itself is left untouched (no tombstone, no move).
    bool erase(uint64_t key) noexcept;

    size_t size() const noexcept { return sz_; }

private:
    static constexpr uint8_t kEmpty = 0; // tag value for a never-used / vacated slot

    static uint64_t mix(uint64_t x) noexcept;
    static uint8_t  fullTag(uint64_t h) noexcept;

    // Returns the index slot holding `key`, or SIZE_MAX if absent.
    size_t findSlot(uint64_t key) const noexcept;

    uint8_t* rowAt(uint32_t rid) noexcept {
        return slab_.data() + static_cast<size_t>(rid) * layout_.rowLen;
    }
    const uint8_t* rowAt(uint32_t rid) const noexcept {
        return slab_.data() + static_cast<size_t>(rid) * layout_.rowLen;
    }

    RowLayout layout_;

    std::vector<uint8_t>  tags_;   // 1-byte Swiss/F14-style tag per index slot
    std::vector<uint64_t> keys_;   // exact 8-byte key per index slot
    std::vector<uint32_t> rid_;    // dense row-id per index slot
    std::vector<uint8_t>  slab_;   // row payloads: maxRows_ * layout_.rowLen (single page)
    std::vector<uint32_t> free_;   // row-id free-list (LIFO; reused before nextRid_)

    uint32_t nextRid_ = 0;  // next never-used row-id, bumped when free_ is empty
    size_t   sz_      = 0;  // live key count
    size_t   cap_     = 0;  // index slot count (power of two)
    size_t   mask_    = 0;  // cap_ - 1
    size_t   maxRows_ = 0;  // slab row capacity (== cap_ for the Task 3 single page)
};

} // namespace rune::view
