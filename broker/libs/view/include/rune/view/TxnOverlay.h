// libs/view/include/rune/view/TxnOverlay.h
//
// Part 11: the chained after-images of one transaction or atomic batch, and the row locks a held overlay puts on later records.
// A chained prepare reads overlay-first and writes its record's effect into the overlay; the tables keep the before-images.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <rune/view/ViewKey.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

enum class OverlayTable : uint8_t { View = 0, Owners = 1, Contributors = 2, Groups = 3, Blocks = 4, GroupIds = 5,
                                    ClaimsIndex = 6, ClaimsExtras = 7, KeyIndex = 8 };   // 0-7 = checkpoint v3 table ids (07, 09)

// A fixed slot arena with an open-addressed index over (member, table, ViewKey).  Reserved once; never allocates afterwards.
class TxnOverlay {
public:
    void reserve(std::size_t rows, uint32_t maxRowLen);   // cold
    void bind(uint64_t sourceKey) noexcept { sourceKey_ = sourceKey; }
    [[nodiscard]] uint64_t sourceKey() const noexcept { return sourceKey_; }
    void clear() noexcept;
    // A put or erase found no free slot since the last clear().
    [[nodiscard]] bool full() const noexcept { return full_; }
    [[nodiscard]] std::size_t rows() const noexcept { return used_; }

    // True for an erased row too: erasing a row locks it as much as writing it does.
    [[nodiscard]] bool contains(uint8_t member, OverlayTable table, const ViewKey& key) const noexcept { return find(member, table, key) >= 0; }
    // The row's image; nullptr with `erased` set when the overlay erased it, nullptr with `erased` clear when it does not hold it.
    [[nodiscard]] const uint8_t* get(uint8_t member, OverlayTable table, const ViewKey& key, bool& erased) const noexcept;
    // The row's writable image: the held one, else a copy of `base` (zeroes when nullptr).  nullptr = full.
    uint8_t* put(uint8_t member, OverlayTable table, const ViewKey& key, const uint8_t* base, uint32_t rowLen) noexcept;
    void erase(uint8_t member, OverlayTable table, const ViewKey& key) noexcept;
    // Scratch row handed out in place of a full put, so a caller that is about to report CapacityExhausted still has memory to write.
    [[nodiscard]] uint8_t* spare() noexcept { return arena_.data() + capacity_ * maxRowLen_; }

private:
    struct Entry {
        ViewKey      key;
        uint8_t      member = 0;
        OverlayTable table  = OverlayTable::View;
        bool         erased = false;
    };
    [[nodiscard]] int32_t find(uint8_t member, OverlayTable table, const ViewKey& key) const noexcept;
    [[nodiscard]] uint8_t* image(int32_t slot) noexcept { return arena_.data() + static_cast<std::size_t>(slot) * maxRowLen_; }
    [[nodiscard]] const uint8_t* image(int32_t slot) const noexcept { return arena_.data() + static_cast<std::size_t>(slot) * maxRowLen_; }
    int32_t insert(uint8_t member, OverlayTable table, const ViewKey& key) noexcept;   // -1 = full

    std::vector<Entry>   entries_;
    std::vector<int32_t> index_;   // slot + 1; 0 = empty
    std::vector<uint8_t> arena_;   // capacity_ rows plus the spare
    std::size_t          capacity_ = 0;
    std::size_t          used_ = 0;
    uint32_t             maxRowLen_ = 0;
    uint64_t             sourceKey_ = 0;
    bool                 full_ = false;
};

inline constexpr std::size_t kTxnOverlaysPerWorker = 8;

// The overlays a worker holds for one source: a record that reads or writes one of their rows waits.
struct TxnLockSet {
    std::array<const TxnOverlay*, kTxnOverlaysPerWorker> locked{};
    uint8_t                                              count = 0;
    // `except` is the overlay being chained: a transaction never locks itself.
    [[nodiscard]] bool contains(uint8_t member, OverlayTable table, const ViewKey& key, const TxnOverlay* except = nullptr) const noexcept {
        for (uint8_t i = 0; i < count; ++i)
            if (locked[i] != except && locked[i]->contains(member, table, key)) return true;
        return false;
    }
};

// What one chained or lock-checked prepare reads through; `locked` is its result.
struct TxnAccess {
    TxnOverlay*       chain  = nullptr;
    const TxnLockSet* locks  = nullptr;
    uint8_t           member = 0;
    bool              locked = false;
};

// A ViewTable seen through a TxnAccess.  With x == nullptr every call is the table's own; t may be nullptr (no stored rows).
// With a chain every read, write and erase leaves an entry (an absent row as an erased one), so the overlay's rows are the transaction's locks.
class TxnTable {
public:
    TxnTable(ViewTable* t, OverlayTable id, TxnAccess* x) noexcept : t_(t), id_(id), x_(x) {}

    [[nodiscard]] ViewTable* table() const noexcept { return t_; }

    [[nodiscard]] const uint8_t* get(const ViewKey& k) const noexcept {
        const uint8_t* r = nullptr;
        if (x_ != nullptr && probe(k, r)) return r;
        r = t_ != nullptr ? t_->get(k) : nullptr;
        if (r == nullptr) noteAbsent(k);
        return r;
    }
    [[nodiscard]] const uint8_t* get(uint64_t k) const noexcept {
        const uint8_t* r = nullptr;
        if (x_ == nullptr) return t_ != nullptr ? t_->get(k) : nullptr;
        const ViewKey vk = ViewKey::fromU64(k);
        if (probe(vk, r)) return r;
        r = t_ != nullptr ? t_->get(k) : nullptr;
        if (r == nullptr) noteAbsent(vk);
        return r;
    }
    uint8_t* put(const ViewKey& k) noexcept {
        if (x_ == nullptr) return t_->put(k);
        touch(k);
        if (x_->chain == nullptr) return t_->put(k);
        return chainPut(k, t_->get(k));
    }
    uint8_t* put(uint64_t k) noexcept {
        if (x_ == nullptr) return t_->put(k);
        const ViewKey vk = ViewKey::fromU64(k);
        touch(vk);
        if (x_->chain == nullptr) return t_->put(k);
        return chainPut(vk, t_->get(k));
    }
    bool erase(const ViewKey& k) noexcept {
        if (x_ == nullptr) return t_->erase(k);
        touch(k);
        if (x_->chain == nullptr) return t_->erase(k);
        return chainErase(k, exists(k, t_ != nullptr && t_->get(k) != nullptr));
    }
    bool erase(uint64_t k) noexcept {
        if (x_ == nullptr) return t_->erase(k);
        const ViewKey vk = ViewKey::fromU64(k);
        touch(vk);
        if (x_->chain == nullptr) return t_->erase(k);
        return chainErase(vk, exists(vk, t_ != nullptr && t_->get(k) != nullptr));
    }

private:
    void touch(const ViewKey& k) const noexcept {
        if (x_->locks != nullptr && x_->locks->contains(x_->member, id_, k, x_->chain)) x_->locked = true;
    }
    // True when the chain decides the row: `r` is its image, or nullptr when the chain erased it.
    bool probe(const ViewKey& k, const uint8_t*& r) const noexcept {
        touch(k);
        if (x_->chain == nullptr) return false;
        bool erased = false;
        r = x_->chain->get(x_->member, id_, k, erased);
        return r != nullptr || erased;
    }
    uint8_t* chainPut(const ViewKey& k, const uint8_t* base) noexcept {
        uint8_t* r = x_->chain->put(x_->member, id_, k, base, t_->rowLen());
        return r != nullptr ? r : x_->chain->spare();
    }
    bool exists(const ViewKey& k, bool inTable) const noexcept {
        const uint8_t* r = nullptr;
        return probe(k, r) ? r != nullptr : inTable;
    }
    // A read that found the row absent still locks it: the chain's erased entry reads back as absent.
    void noteAbsent(const ViewKey& k) const noexcept {
        if (x_ != nullptr && x_->chain != nullptr && t_ != nullptr) x_->chain->erase(x_->member, id_, k);
    }
    bool chainErase(const ViewKey& k, bool existed) noexcept {
        x_->chain->erase(x_->member, id_, k);
        return existed;
    }

    ViewTable*    t_;
    OverlayTable  id_;
    TxnAccess*    x_;
};

} // namespace rune::view
