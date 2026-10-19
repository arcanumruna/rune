// libs/view/src/ViewRow.cpp
#include <rune/view/ViewRow.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace rune::view {

using rune::codec::Descriptor;
using rune::codec::FieldValue;
using rune::codec::PrimType;

RowLayout buildRowLayout(const Descriptor& outDesc) {
    const auto& fields  = outDesc.fields;
    const size_t nFields = fields.size();

    for (const auto& f : fields) {
        if (rune::codec::isVarLen(f.type)) {
            throw std::invalid_argument(
                "rune::view::buildRowLayout: var-len column '" + f.name +
                "' not supported in Phase 1 (fixed-width columns only)");
        }
    }

    RowLayout layout;
    layout.bitmapBytes = static_cast<uint32_t>((nFields + 7) / 8);
    layout.dataOffset  = layout.bitmapBytes;

    layout.slotOffset.reserve(nFields);
    layout.slotType.reserve(nFields);
    layout.slotWidth.reserve(nFields);

    uint32_t offset = layout.dataOffset;
    for (const auto& f : fields) {
        const uint32_t width = rune::codec::fieldWidth(f);
        layout.slotOffset.push_back(offset);
        layout.slotType.push_back(f.type);
        layout.slotWidth.push_back(static_cast<uint16_t>(width));
        offset += width;
    }
    layout.rowLen = offset;
    return layout;
}

void setNull(uint8_t* row, const RowLayout&, uint16_t slot) noexcept {
    row[slot >> 3] |= static_cast<uint8_t>(1u << (slot & 7));
}

bool isNull(const uint8_t* row, const RowLayout&, uint16_t slot) noexcept {
    return (row[slot >> 3] & static_cast<uint8_t>(1u << (slot & 7))) != 0;
}

void clearNull(uint8_t* row, const RowLayout&, uint16_t slot) noexcept {
    row[slot >> 3] &= static_cast<uint8_t>(~(1u << (slot & 7)));
}

namespace {

void writeU(uint8_t* row, uint32_t off, uint64_t v, uint32_t n) noexcept {
    for (uint32_t k = 0; k < n; ++k) {
        row[off + k] = static_cast<uint8_t>((v >> (8 * k)) & 0xFFu);
    }
}

uint64_t readU(const uint8_t* row, uint32_t off, uint32_t n) noexcept {
    uint64_t v = 0;
    for (uint32_t k = 0; k < n; ++k) {
        v |= uint64_t(row[off + k]) << (8 * k);
    }
    return v;
}

} // namespace

void writeSlot(uint8_t* row, const RowLayout& layout, uint16_t slot, FieldValue v) noexcept {
    if (!v.present) {
        setNull(row, layout, slot);
        return;
    }
    clearNull(row, layout, slot);

    const uint32_t off   = layout.slotOffset[slot];
    const uint16_t width = layout.slotWidth[slot];
    const PrimType type  = layout.slotType[slot];

    switch (type) {
        case PrimType::Int32:
            writeU(row, off, static_cast<uint32_t>(static_cast<int32_t>(v.i)), 4);
            break;
        case PrimType::Int64:
            writeU(row, off, static_cast<uint64_t>(v.i), 8);
            break;
        case PrimType::UInt16:
            writeU(row, off, v.u, 2);
            break;
        case PrimType::UInt32:
        case PrimType::Enum:
            writeU(row, off, v.u, 4);
            break;
        case PrimType::UInt64:
            writeU(row, off, v.u, 8);
            break;
        case PrimType::Float64: {
            uint64_t raw = 0;
            std::memcpy(&raw, &v.f, 8);
            writeU(row, off, raw, 8);
            break;
        }
        case PrimType::Bool:
            row[off] = v.b ? 1 : 0;
            break;
        case PrimType::FixedStr: {
            const size_t n = std::min(static_cast<size_t>(width), v.bytes.size());
            std::memcpy(row + off, v.bytes.data(), n);
            if (n < width) {
                std::memset(row + off + n, 0, width - n);
            }
            break;
        }
        case PrimType::String:
        case PrimType::Bytes:
            // var-len: rejected by buildRowLayout; unreachable for a valid RowLayout.
            break;
    }
}

FieldValue readSlot(const uint8_t* row, const RowLayout& layout, uint16_t slot) noexcept {
    if (isNull(row, layout, slot)) {
        FieldValue fv{};
        fv.type    = layout.slotType[slot];
        fv.present = false;
        return fv;
    }

    const uint32_t off   = layout.slotOffset[slot];
    const uint16_t width = layout.slotWidth[slot];
    const PrimType type  = layout.slotType[slot];

    FieldValue fv{};
    fv.type    = type;
    fv.present = true;

    switch (type) {
        case PrimType::Int32:
            fv.i = static_cast<int32_t>(static_cast<uint32_t>(readU(row, off, 4)));
            break;
        case PrimType::Int64:
            fv.i = static_cast<int64_t>(readU(row, off, 8));
            break;
        case PrimType::UInt16:
            fv.u = readU(row, off, 2);
            break;
        case PrimType::UInt32:
        case PrimType::Enum:
            fv.u = readU(row, off, 4);
            break;
        case PrimType::UInt64:
            fv.u = readU(row, off, 8);
            break;
        case PrimType::Float64: {
            uint64_t raw = readU(row, off, 8);
            std::memcpy(&fv.f, &raw, 8);
            break;
        }
        case PrimType::Bool:
            fv.b = row[off] != 0;
            fv.i = row[off];
            break;
        case PrimType::FixedStr:
            fv.bytes = std::string_view(reinterpret_cast<const char*>(row + off), width);
            break;
        case PrimType::String:
        case PrimType::Bytes:
            // var-len: rejected by buildRowLayout; unreachable for a valid RowLayout.
            break;
    }
    return fv;
}

} // namespace rune::view
