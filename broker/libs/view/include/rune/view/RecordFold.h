// libs/view/include/rune/view/RecordFold.h
//
// One source record folded in two steps: prepare computes the prospective rows into a fixed per-instance Prospect
// without writing a table, apply commits them.  The judge of a verdict source prepares every verdict view, evaluates,
// and applies all or none.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/view/AggState.h>
#include <rune/view/FoldStats.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

struct SourceRecord {
    std::string_view               key;                   // wire key
    std::string_view               value;                 // empty = tombstone
    int64_t                        offset = 0;
    uint8_t                        intent = 0;            // rune::protocol::record::intentOf(attributes)
    const rune::codec::Descriptor* writer = nullptr;      // materialized writer version; nullptr = the fold resolves it
};

enum class PrepareStatus : uint8_t { Ok, Malformed, CapacityExhausted };
enum class ProspectOp : uint8_t { None, Upsert, Erase, Move };

// Touched aggregate rows of one prepared record, in fixed slots (never allocates after reserve).
struct RowOverlay {
    static constexpr std::size_t kSlotsPerTable = 16;
    static constexpr std::size_t kTables        = 4;
    enum class Table : uint8_t { View, Contributors, Groups, Blocks };
    struct Slot { uint64_t key = 0; bool erased = false; };

    void reserve(const RowLayout& view, const AggLayout& agg, std::size_t slotsPerTable = kSlotsPerTable);   // cold
    void clear() noexcept {
        used_.fill(0);
        full_ = false;
    }
    [[nodiscard]] bool full() const noexcept { return full_; }

    // The slot of `key` in `t`, or nullptr.
    [[nodiscard]] Slot* find(Table t, uint64_t key) noexcept {
        const auto ti = static_cast<std::size_t>(t);
        for (std::size_t i = 0; i < used_[ti]; ++i)
            if (slots_[ti * kSlotsPerTable + i].key == key) return &slots_[ti * kSlotsPerTable + i];
        return nullptr;
    }
    // A new slot for `key` (image not initialised), or the table's spare row with full() set when out of slots.
    [[nodiscard]] Slot* add(Table t, uint64_t key, uint8_t*& image) noexcept {
        const auto ti = static_cast<std::size_t>(t);
        if (used_[ti] >= slotsPerTable_) {
            full_ = true;
            spareSlot_ = Slot{key, false};
            image = images_.data() + imageBase_[ti] + slotsPerTable_ * rowLen_[ti];
            return &spareSlot_;
        }
        Slot& s = slots_[ti * kSlotsPerTable + used_[ti]];
        s = Slot{key, false};
        image = images_.data() + imageBase_[ti] + used_[ti] * rowLen_[ti];
        ++used_[ti];
        return &s;
    }
    [[nodiscard]] uint8_t* imageOf(Table t, const Slot* s) noexcept {
        const auto ti = static_cast<std::size_t>(t);
        if (s == &spareSlot_) return images_.data() + imageBase_[ti] + slotsPerTable_ * rowLen_[ti];
        return images_.data() + imageBase_[ti] + static_cast<std::size_t>(s - &slots_[ti * kSlotsPerTable]) * rowLen_[ti];
    }
    [[nodiscard]] uint32_t rowLen(Table t) const noexcept { return rowLen_[static_cast<std::size_t>(t)]; }
    [[nodiscard]] std::size_t used(Table t) const noexcept { return used_[static_cast<std::size_t>(t)]; }
    [[nodiscard]] const Slot& slot(Table t, std::size_t i) const noexcept { return slots_[static_cast<std::size_t>(t) * kSlotsPerTable + i]; }
    [[nodiscard]] const uint8_t* image(Table t, std::size_t i) const noexcept {
        const auto ti = static_cast<std::size_t>(t);
        return images_.data() + imageBase_[ti] + i * rowLen_[ti];
    }

private:
    std::array<Slot, kTables * kSlotsPerTable> slots_{};
    std::array<std::size_t, kTables>           used_{};
    std::array<uint32_t, kTables>              rowLen_{};
    std::array<std::size_t, kTables>           imageBase_{};
    std::size_t                                slotsPerTable_ = kSlotsPerTable;
    std::vector<uint8_t>                       images_;   // per table: slotsPerTable_ rows plus one spare
    Slot                                       spareSlot_{};
    bool                                       full_ = false;
};

struct ProspectRow {
    ViewKey  key;
    uint8_t* image   = nullptr;   // the prospective view-row image; valid until the Prospect is cleared
    bool     removed = false;     // the row is gone after apply
    bool     existed = false;     // the row exists in the table now
};

// Reserved once per ViewInstance at install; never grows.
struct Prospect {
    ProspectOp                   op       = ProspectOp::None;
    uint8_t                      rowCount = 0;
    std::array<ProspectRow, 2>   rows{};
    std::vector<uint8_t>         scratch;   // projection: 2 * rowLen
    RowOverlay                   overlay;   // aggregate: every touched row of the four tables
    FoldStats                    stats;     // what the fold would have counted for this record
    uint32_t                     rowLen = 0;

    void reserve(const RowLayout& layout, const AggLayout* agg);   // cold
    void clear() noexcept {
        op       = ProspectOp::None;
        rowCount = 0;
        rows     = {};
        stats    = {};
        overlay.clear();
    }
};

inline void RowOverlay::reserve(const RowLayout& view, const AggLayout& agg, std::size_t slotsPerTable) {
    slotsPerTable_ = slotsPerTable < kSlotsPerTable ? slotsPerTable : kSlotsPerTable;
    rowLen_ = {view.rowLen, agg.contributorRow.rowLen, agg.groupRow.rowLen, agg.blockRow.rowLen};
    std::size_t total = 0;
    for (std::size_t t = 0; t < kTables; ++t) {
        imageBase_[t] = total;
        total += (slotsPerTable_ + 1) * rowLen_[t];
    }
    images_.assign(total, 0);
    clear();
}

inline void Prospect::reserve(const RowLayout& layout, const AggLayout* agg) {
    rowLen = layout.rowLen;
    if (agg != nullptr) overlay.reserve(layout, *agg);
    else scratch.assign(2u * layout.rowLen, 0);
}

} // namespace rune::view
