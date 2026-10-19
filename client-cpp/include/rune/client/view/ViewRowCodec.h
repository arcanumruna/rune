// include/rune/client/view/ViewRowCodec.h
//
// Task 21 — positional decode of a `GetViewRow` response's `rowBytes`.
//
// Phase 1 view output rows are fixed-width only (no String/Bytes columns —
// rune::view::buildRowLayout throws on a var-len output field). The wire
// layout (rune::view::RowLayout / rune::view::readSlot,
// libs/view/include/rune/view/ViewRow.h, verified byte-for-byte against
// ViewRow.cpp on the view-engine-phase1 branch) is:
//
//   [null bitmap: ceil(nFields/8) bytes, bit=slot index, LSB-first, set=NULL]
//   [fixed-width slots, in output-field declaration order, each slot's width
//    determined by its PrimType, packed with no padding]
//
// Since GetViewRow carries NO output-descriptor wire (Phase 1: the caller
// already knows its own authored output shape — the same list of
// (name, PrimType) it put in the CreateView planBlob's `output` fields, in
// the SAME order), this header rebuilds that exact layout client-side from a
// caller-supplied field-type list and decodes rowBytes against it — the
// "positional accessor" the client API offers in place of a server-sent
// output descriptor. `requestedFingerprint` (pinned from CreateViewResponse)
// is what guards a layout drift: the broker rejects a GetViewRow against the
// wrong output fingerprint with SchemaMismatch before any bytes are decoded.
//
// This is a client-side, independent reimplementation (shared-nothing across
// repos — libs/view is broker/node-internal, not a client dependency), not a
// hot-path type: view reads are a cold control-plane-adjacent path in Phase 1
// (point lookups, not the produce/consume data path).
#pragma once

#include <rune/codec/PrimType.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rune::client::view {

// Fixed-width row layout mirroring rune::view::RowLayout.
struct RowLayout {
    uint32_t rowLen      = 0;
    uint32_t bitmapBytes = 0;
    uint32_t dataOffset  = 0;
    std::vector<uint32_t>              slotOffset;
    std::vector<rune::codec::PrimType> slotType;
    std::vector<uint16_t>              slotWidth;
};

// One decoded field (mirrors rune::codec::FieldValue's shape).
struct FieldValue {
    rune::codec::PrimType type    = rune::codec::PrimType::Int32;
    bool                  present = false;
    int64_t               i       = 0;
    uint64_t              u       = 0;
    double                f       = 0;
    bool                  b       = false;
    std::string_view      bytes;   // FixedStr only; valid only while row outlives this value
};

// Builds the fixed-width row layout from the output fields' PrimTypes, IN
// THE SAME ORDER the CreateView planBlob declared them (ViewPlan::output
// order == RowLayout slot order; see PlanCompiler.cpp). Throws
// std::invalid_argument on a var-len type (String/Bytes) — Phase 1 columns
// are fixed-width only.
inline RowLayout buildRowLayout(std::span<const rune::codec::PrimType> fieldTypesInOrder,
                                std::span<const uint16_t> fixedStrWidths = {}) {
    for (const auto t : fieldTypesInOrder) {
        if (rune::codec::isVarLen(t)) {
            throw std::invalid_argument(
                "rune::client::view::buildRowLayout: var-len column not supported in Phase 1 "
                "(fixed-width columns only)");
        }
    }
    RowLayout layout;
    const auto n = fieldTypesInOrder.size();
    layout.bitmapBytes = static_cast<uint32_t>((n + 7) / 8);
    layout.dataOffset  = layout.bitmapBytes;

    layout.slotOffset.reserve(n);
    layout.slotType.reserve(n);
    layout.slotWidth.reserve(n);

    uint32_t offset = layout.dataOffset;
    for (std::size_t idx = 0; idx < n; ++idx) {
        const auto t = fieldTypesInOrder[idx];
        uint32_t width = rune::codec::fixedSize(t);
        if (t == rune::codec::PrimType::FixedStr) {
            width = (idx < fixedStrWidths.size()) ? fixedStrWidths[idx] : 0;
        }
        layout.slotOffset.push_back(offset);
        layout.slotType.push_back(t);
        layout.slotWidth.push_back(static_cast<uint16_t>(width));
        offset += width;
    }
    layout.rowLen = offset;
    return layout;
}

inline bool isNullSlot(std::span<const uint8_t> row, uint16_t slot) noexcept {
    return (row[slot >> 3] & static_cast<uint8_t>(1u << (slot & 7))) != 0;
}

namespace detail {
inline uint64_t readU(std::span<const uint8_t> row, uint32_t off, uint32_t n) noexcept {
    uint64_t v = 0;
    for (uint32_t k = 0; k < n; ++k) v |= uint64_t(row[off + k]) << (8 * k);
    return v;
}
} // namespace detail

// Reads the field at `slot` out of `row` (a GetViewRow response's rowBytes,
// span length >= layout.rowLen). Returns FieldValue{present=false} for a
// NULL slot (merged-away / never-set field), distinguishable from a real
// zero value.
inline FieldValue readSlot(std::span<const uint8_t> row, const RowLayout& layout, uint16_t slot) noexcept {
    using rune::codec::PrimType;
    FieldValue fv;
    fv.type = layout.slotType[slot];
    if (isNullSlot(row, slot)) {
        fv.present = false;
        return fv;
    }
    fv.present = true;
    const uint32_t off   = layout.slotOffset[slot];
    const uint16_t width = layout.slotWidth[slot];
    switch (fv.type) {
    case PrimType::Int32:   fv.i = static_cast<int32_t>(static_cast<uint32_t>(detail::readU(row, off, 4))); break;
    case PrimType::Int64:   fv.i = static_cast<int64_t>(detail::readU(row, off, 8)); break;
    case PrimType::UInt16:  fv.u = detail::readU(row, off, 2); break;
    case PrimType::UInt32:
    case PrimType::Enum:    fv.u = detail::readU(row, off, 4); break;
    case PrimType::UInt64:  fv.u = detail::readU(row, off, 8); break;
    case PrimType::Float64: {
        uint64_t raw = detail::readU(row, off, 8);
        std::memcpy(&fv.f, &raw, 8);
        break;
    }
    case PrimType::Bool:    fv.b = row[off] != 0; fv.i = row[off]; break;
    case PrimType::FixedStr:
        fv.bytes = std::string_view(reinterpret_cast<const char*>(row.data() + off), width);
        break;
    case PrimType::String:
    case PrimType::Bytes:
        // var-len: rejected by buildRowLayout; unreachable for a valid RowLayout.
        break;
    }
    return fv;
}

} // namespace rune::client::view
