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
} // namespace rune::view
