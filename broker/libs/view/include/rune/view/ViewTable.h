// libs/view/include/rune/view/ViewTable.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>
#include <rune/view/ViewRow.h>

namespace rune::view {

// Key -> row index + chunked-arena row-payload storage for a materialized
// view's in-memory table.
//
// Phase 1 Task 4 scope: the row payload store is a chunked arena
// (`chunks_`, a `std::vector` of fixed-size `rowsPerChunk_`-row pages) that
// grows by APPENDING a new chunk when the current one fills — rows never
// move, so a `rid` (and any pointer previously returned for it by `put`/
// `get`) stays valid for the table's entire lifetime. ONE preallocated
// open-addressing index (`tags_`/`keys_`/`rid_`, `initialIndexSlots`
// slots) still backs key lookup; incremental rehash/growth of the index
// itself is Task 5 — this type still asserts index capacity instead of
// growing it.
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
// Zero heap allocation in put()/get()/erase() steady state, EXCEPT the
// deliberate chunk allocation inside appendChunk() when `put` needs a row
// beyond the current chunk capacity — that IS the growth path. get()/
// erase() never allocate. The row-id free-list only grows (via erase())
// so its own push_back() never (re)allocates once reserve()'d.
class ViewTable {
public:
    // `initialIndexSlots` must be a power of two (capacity of the
    // open-addressing index; index growth is Task 5).
    // `chunkBytes` sizes each row-slab chunk: `rowsPerChunk_` is
    // `floor_pow2(chunkBytes / layout.rowLen)`, clamped to a minimum of 1
    // row per chunk (e.g. when a single row is wider than `chunkBytes`).
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

    // Number of row-slab chunks currently allocated. Grows by 1 each time
    // `put` needs a row-id beyond the current total chunk capacity.
    size_t chunkCount() const noexcept { return chunks_.size(); }

private:
    static constexpr uint8_t kEmpty = 0; // tag value for a never-used / vacated slot

    static uint64_t mix(uint64_t x) noexcept;
    static uint8_t  fullTag(uint64_t h) noexcept;

    // Returns the index slot holding `key`, or SIZE_MAX if absent.
    size_t findSlot(uint64_t key) const noexcept;

    // Allocates one more chunk (rowsPerChunk_ * layout_.rowLen bytes,
    // zero-initialized) and appends it. This is the ONE deliberate
    // allocation on the put() growth path; existing rid->address mappings
    // are unaffected because `chunks_` grows by push_back (prior chunk
    // vectors/pointers are never touched or moved — only a new element is
    // added) and rows live inside already-allocated chunks, never inside
    // the chunks_ vector's own backing storage.
    void appendChunk();

    // rid -> row address: shift picks the chunk, mask picks the in-chunk
    // row slot. A chunk is a single contiguous std::vector<uint8_t>, so
    // this is a shift + mask + one pointer-load + multiply-add — no
    // branching, no allocation, a few ns. Rows never move once allocated:
    // growth only ever appends a new chunk (see appendChunk()), so a
    // pointer returned for a given rid stays valid for the table's
    // lifetime.
    uint8_t* rowAddr(uint32_t rid) noexcept {
        auto& chunk = chunks_[static_cast<size_t>(rid) >> chunkShift_];
        const size_t inChunk = static_cast<size_t>(rid) & chunkMask_;
        return chunk.data() + inChunk * layout_.rowLen;
    }
    const uint8_t* rowAddr(uint32_t rid) const noexcept {
        const auto& chunk = chunks_[static_cast<size_t>(rid) >> chunkShift_];
        const size_t inChunk = static_cast<size_t>(rid) & chunkMask_;
        return chunk.data() + inChunk * layout_.rowLen;
    }

    RowLayout layout_;

    std::vector<uint8_t>  tags_;   // 1-byte Swiss/F14-style tag per index slot
    std::vector<uint64_t> keys_;   // exact 8-byte key per index slot
    std::vector<uint32_t> rid_;    // dense row-id per index slot
    std::vector<std::vector<uint8_t>> chunks_; // row-payload chunked arena; rows never move
    std::vector<uint32_t> free_;   // row-id free-list (LIFO; reused before nextRid_)

    uint32_t nextRid_ = 0;  // next never-used row-id, bumped when free_ is empty
    size_t   sz_      = 0;  // live key count
    size_t   cap_     = 0;  // index slot count (power of two)
    size_t   mask_    = 0;  // cap_ - 1

    size_t rowsPerChunk_ = 0;  // power of two: floor_pow2(chunkBytes / rowLen), min 1
    size_t chunkShift_   = 0;  // log2(rowsPerChunk_); rid >> chunkShift_ == chunk index
    size_t chunkMask_    = 0;  // rowsPerChunk_ - 1; rid & chunkMask_ == in-chunk row slot
};

} // namespace rune::view
