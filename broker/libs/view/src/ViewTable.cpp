// libs/view/src/ViewTable.cpp
#include <rune/view/ViewTable.h>
#include <algorithm>
#include <bit>
#include <cassert>
#include <utility>

namespace rune::view {

uint64_t ViewTable::mix(uint64_t x) noexcept {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

uint8_t ViewTable::fullTag(uint64_t h) noexcept {
    uint8_t t = static_cast<uint8_t>((h >> 56) & 0x7f);
    return 0x80 | (t < 2 ? 2 : t); // 0/1 reserved for EMPTY/TOMB-style sentinels
}

ViewTable::ViewTable(RowLayout layout, size_t initialIndexSlots, size_t chunkBytes)
    : layout_(std::move(layout)) {
    assert(initialIndexSlots > 0 && std::has_single_bit(initialIndexSlots) &&
           "ViewTable: initialIndexSlots must be a power of two");

    cap_     = initialIndexSlots;
    mask_    = cap_ - 1;

    // rowsPerChunk_ = floor_pow2(chunkBytes / rowLen), clamped to >= 1 (a
    // single row wider than chunkBytes still gets its own one-row chunk).
    const size_t rowLen = static_cast<size_t>(layout_.rowLen);
    size_t rowsPerChunk = rowLen > 0 ? (chunkBytes / rowLen) : 0;
    rowsPerChunk = std::bit_floor(rowsPerChunk); // 0 stays 0 (bit_floor(0) == 0)
    if (rowsPerChunk < 1) rowsPerChunk = 1;
    rowsPerChunk_ = rowsPerChunk;
    chunkShift_   = static_cast<size_t>(std::countr_zero(rowsPerChunk_));
    chunkMask_    = rowsPerChunk_ - 1;

    tags_.assign(cap_, kEmpty);
    keys_.assign(cap_, 0);
    rid_.assign(cap_, 0);
    // free_ can never hold more than the index's own cap_ row-ids (index
    // capacity bounds live keys until Task 5's index rehash), so reserving
    // up front makes erase()'s push_back() allocation-free for the
    // table's entire lifetime (not just after an amortized-growth warm-up).
    free_.reserve(cap_);

    appendChunk(); // start with one chunk so rid 0 is always addressable
}

void ViewTable::appendChunk() {
    chunks_.emplace_back(rowsPerChunk_ * static_cast<size_t>(layout_.rowLen), uint8_t{0});
}

size_t ViewTable::findSlot(uint64_t key) const noexcept {
    const uint64_t h  = mix(key);
    const uint8_t  ft = fullTag(h);
    size_t i = h & mask_;
    for (size_t probes = 0; probes < cap_; ++probes) {
        const uint8_t t = tags_[i];
        if (t == kEmpty) return static_cast<size_t>(-1);
        if (t == ft && keys_[i] == key) return i;
        i = (i + 1) & mask_;
    }
    return static_cast<size_t>(-1);
}

uint8_t* ViewTable::put(uint64_t key) {
    const uint64_t h  = mix(key);
    const uint8_t  ft = fullTag(h);
    size_t i = h & mask_;
    for (size_t probes = 0; probes < cap_; ++probes) {
        const uint8_t t = tags_[i];
        if (t == ft && keys_[i] == key) {
            return rowAddr(rid_[i]); // idempotent upsert: existing key, no dup
        }
        if (t == kEmpty) {
            uint32_t rid;
            if (!free_.empty()) {
                rid = free_.back();
                free_.pop_back();
            } else {
                // Row-id space is the chunked arena's total capacity so
                // far: append one more chunk (the deliberate growth
                // allocation) whenever the next never-used rid would fall
                // past it. Existing rids/pointers are unaffected — rows
                // never move.
                if (nextRid_ >= chunks_.size() * rowsPerChunk_) {
                    appendChunk();
                }
                rid = nextRid_++;
            }
            tags_[i] = ft;
            keys_[i] = key;
            rid_[i]  = rid;
            uint8_t* row = rowAddr(rid);
            std::fill(row, row + layout_.rowLen, uint8_t{0});
            ++sz_;
            return row;
        }
        i = (i + 1) & mask_;
    }
    assert(false && "ViewTable: index full (Task 3 scope — growth is Task 4/5)");
    return nullptr; // unreachable under the capacity guard above
}

const uint8_t* ViewTable::get(uint64_t key) const noexcept {
    const size_t i = findSlot(key);
    if (i == static_cast<size_t>(-1)) return nullptr;
    return rowAddr(rid_[i]);
}

bool ViewTable::erase(uint64_t key) noexcept {
    const size_t i = findSlot(key);
    if (i == static_cast<size_t>(-1)) return false;

    free_.push_back(rid_[i]);
    tags_[i] = kEmpty;

    // Backward-shift deletion (standard open-addressing linear-probing
    // delete, e.g. Wikipedia "Open addressing" / Knuth 6.4): walk forward
    // from the freed slot `hole`, pulling back any entry at `j` whose home
    // slot does NOT lie in the open-closed cyclic interval (hole, j] — such
    // an entry's probe sequence does not depend on slot `hole` being
    // occupied, so it is safe to move it into the hole, vacating `j` as the
    // new hole. Stops at the first slot that was already EMPTY (that marks
    // the end of this probe run).
    size_t hole = i;
    size_t j    = i;
    for (;;) {
        j = (j + 1) & mask_;
        if (tags_[j] == kEmpty) break;

        const size_t home = mix(keys_[j]) & mask_;
        bool pinned; // entry at j must stay put (for now) relative to hole
        if (hole <= j) {
            pinned = (hole < home) && (home <= j);
        } else {
            pinned = (hole < home) || (home <= j);
        }
        if (pinned) continue;

        tags_[hole] = tags_[j];
        keys_[hole] = keys_[j];
        rid_[hole]  = rid_[j];
        tags_[j]    = kEmpty;
        hole        = j;
    }

    --sz_;
    return true;
}

} // namespace rune::view
