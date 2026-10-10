// libs/view/include/rune/view/ViewRow.h
#pragma once
#include <cstdint>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Record.h>

namespace rune::view {

// Fixed-width row layout for a materialized view (Phase 1: fixed-width
// columns only). Layout is: null bitmap first (one bit per field, bit set =
// NULL), then fixed-width slots in field declaration order.
struct RowLayout {
    uint32_t rowLen      = 0;  // bitmapBytes + sum(slotWidth)
    uint32_t bitmapBytes = 0;  // ceil(nFields / 8)
    uint32_t dataOffset  = 0;  // == bitmapBytes; first slot's byte offset
    std::vector<uint32_t> slotOffset;                    // per field, row-relative byte offset
    std::vector<rune::codec::PrimType> slotType;         // per field, PrimType
    std::vector<uint16_t> slotWidth;                     // per field, byte width in the row
};

// Builds the fixed-width row layout from a codec Descriptor. Phase 1 columns
// are fixed-width only: throws std::invalid_argument if any field is var-len
// (String/Bytes).
RowLayout buildRowLayout(const rune::codec::Descriptor& outDesc);

// Null-bitmap bit ops. `slot` indexes fields in declaration order (same index
// space as RowLayout::slotOffset/slotType/slotWidth).
void setNull(uint8_t* row, const RowLayout& layout, uint16_t slot) noexcept;
bool isNull(const uint8_t* row, const RowLayout& layout, uint16_t slot) noexcept;
void clearNull(uint8_t* row, const RowLayout& layout, uint16_t slot) noexcept;

// Writes `v` into `row` at `slot`. If `!v.present`, sets the null bit and
// leaves the slot bytes zero. Otherwise clears the null bit and encodes the
// width-appropriate representation of `v` per `slotType[slot]`.
void writeSlot(uint8_t* row, const RowLayout& layout, uint16_t slot,
               rune::codec::FieldValue v) noexcept;

// Reads the field at `slot` from `row`. Returns a FieldValue with
// `present=false` when the null bit is set — distinguishable from a real
// zero value.
rune::codec::FieldValue readSlot(const uint8_t* row, const RowLayout& layout,
                                  uint16_t slot) noexcept;

} // namespace rune::view
