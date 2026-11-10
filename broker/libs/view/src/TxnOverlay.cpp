// libs/view/src/TxnOverlay.cpp
#include <rune/view/TxnOverlay.h>

#include <algorithm>
#include <cstring>
#include <new>

namespace rune::view {

namespace {

uint64_t hashOf(uint8_t member, OverlayTable table, const ViewKey& k) noexcept {
    uint64_t h = ((static_cast<uint64_t>(member) << 8) | static_cast<uint8_t>(table)) * 0x9E3779B97F4A7C15ull ^ k.len;
    for (std::size_t i = 0; i < k.len; i += 8) {
        uint64_t w = 0;
        std::memcpy(&w, k.bytes.data() + i, std::min<std::size_t>(8, k.len - i));
        h = (h ^ w) * 0xFF51AFD7ED558CCDull;
        h ^= h >> 32;
    }
    return h;
}

} // namespace

void TxnOverlay::reserve(std::size_t rows, uint32_t maxRowLen) {
    capacity_ = rows;
    maxRowLen_ = maxRowLen;
    slots_ = 2;
    while (slots_ < 2 * rows) slots_ <<= 1;
    // malloc's large blocks are untouched, so only the index, which must be zero, costs a write per byte (an assign of every overlay
    // at every restart and member change was most of a checked-writes model seed).  A calloc'd arena is zeroed again whenever the
    // allocator hands back a freed block; only the spare row, which no put writes, is zeroed.
    entries_.reset(static_cast<Entry*>(std::malloc((rows + 1) * sizeof(Entry))));
    index_.reset(static_cast<int32_t*>(std::calloc(slots_, sizeof(int32_t))));
    arena_.reset(static_cast<uint8_t*>(std::malloc((rows + 1) * maxRowLen + 1)));
    if (!entries_ || !index_ || !arena_) throw std::bad_alloc();
    std::memset(arena_.get() + rows * maxRowLen, 0, maxRowLen + 1);
    used_ = 0;
    full_ = false;
}

// O(rows held): the index is sized for the largest transaction, and an atomic batch clears its overlay twice.
void TxnOverlay::clear() noexcept {
    for (std::size_t s = 0; s < used_; ++s) index_[entries_[s].pos] = 0;
    used_ = 0;
    full_ = false;
}

int32_t TxnOverlay::find(uint8_t member, OverlayTable table, const ViewKey& key) const noexcept {
    if (used_ == 0) return -1;
    const std::size_t mask = slots_ - 1;
    for (std::size_t pos = hashOf(member, table, key) & mask;; pos = (pos + 1) & mask) {
        const int32_t s = index_[pos];
        if (s == 0) return -1;
        const Entry& e = entries_[static_cast<std::size_t>(s - 1)];
        if (e.member == member && e.table == table && e.key == key) return s - 1;
    }
}

int32_t TxnOverlay::insert(uint8_t member, OverlayTable table, const ViewKey& key) noexcept {
    if (used_ == capacity_) {
        full_ = true;
        return -1;
    }
    const std::size_t mask = slots_ - 1;
    std::size_t pos = hashOf(member, table, key) & mask;
    while (index_[pos] != 0) pos = (pos + 1) & mask;
    const auto slot = static_cast<int32_t>(used_++);
    entries_[static_cast<std::size_t>(slot)] = Entry{key, static_cast<uint32_t>(pos), member, table, false};
    index_[pos] = slot + 1;
    return slot;
}

const uint8_t* TxnOverlay::get(uint8_t member, OverlayTable table, const ViewKey& key, bool& erased) const noexcept {
    const int32_t s = find(member, table, key);
    erased = s >= 0 && entries_[static_cast<std::size_t>(s)].erased;
    return s >= 0 && !erased ? image(s) : nullptr;
}

uint8_t* TxnOverlay::put(uint8_t member, OverlayTable table, const ViewKey& key, const uint8_t* base, uint32_t rowLen) noexcept {
    if (rowLen > maxRowLen_) {
        full_ = true;
        return nullptr;
    }
    int32_t s = find(member, table, key);
    if (s >= 0) {
        Entry& e = entries_[static_cast<std::size_t>(s)];
        if (e.erased) {
            std::memset(image(s), 0, rowLen);
            e.erased = false;
        }
        return image(s);
    }
    s = insert(member, table, key);
    if (s < 0) return nullptr;
    if (base != nullptr) std::memcpy(image(s), base, rowLen);
    else std::memset(image(s), 0, rowLen);
    return image(s);
}

void TxnOverlay::erase(uint8_t member, OverlayTable table, const ViewKey& key) noexcept {
    int32_t s = find(member, table, key);
    if (s < 0) s = insert(member, table, key);
    if (s >= 0) entries_[static_cast<std::size_t>(s)].erased = true;
}

} // namespace rune::view
