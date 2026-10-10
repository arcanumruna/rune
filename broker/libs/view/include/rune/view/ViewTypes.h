// libs/view/include/rune/view/ViewTypes.h
#pragma once
#include <cstdint>
#include <rune/codec/PrimType.h>
namespace rune::view {
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last };
inline bool isNumeric(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64: case P::UInt16:
        case P::UInt32: case P::UInt64: case P::Float64: return true;
        default: return false; // Enum is categorical; Bool/String/Bytes/FixedStr not numeric
    }
}
inline bool isFixedWidthColumn(rune::codec::PrimType t) noexcept {
    return !rune::codec::isVarLen(t); // String/Bytes are the only var-len types
}
inline bool isIntegerKeyType(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64: case P::UInt16:
        case P::UInt32: case P::UInt64: return true;   // Phase 1 keys fold to uint64_t
        default: return false;                          // Float64/Bool/Enum/FixedStr/String/Bytes deferred
    }
}
// Whole-branch review Bug B: ClientGateway::handleGetViewRow canonicalizes a
// probe key into a FIXED 8-byte little-endian buffer and partitions it via
// FNV-1a64 over that exact 8-byte span. The PRODUCER, however, partitions a
// committed row by the key field's NATIVE width (decodeWireKey enforces exact
// width — Int32/UInt32 = 4 bytes, UInt16 = 2 bytes). FNV over an 8-byte-padded
// key never equals FNV over the native-width key, so a narrow (<8-byte)
// integer VIEW KEY on a multi-partition view routes GetViewRow to the WRONG
// partition and a real row silently reads back AbsentKey.
//
// Phase 1 fix (no wire change): restrict VIEW KEY fields to 8-byte integer
// types only (Int64/UInt64), so handleGetViewRow's 8-byte canonicalization is
// always correct. Narrow-key (Int32/UInt32/UInt16) VIEW KEY support is
// DEFERRED — it requires the GetViewRow wire to carry the key's native width
// so the gateway can canonicalize/hash exactly as the producer did.
//
// Scoped to the KEY-field validation path ONLY: isIntegerKeyType above is left
// unchanged (and unused elsewhere today) so any future non-key integer check
// is unaffected. Non-key integer OUTPUT columns and Float64/etc. output
// columns are unaffected by this restriction — only the KEY field is gated.
inline bool isEightByteKeyType(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    return t == P::Int64 || t == P::UInt64;
}
} // namespace rune::view
