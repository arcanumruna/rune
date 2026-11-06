// include/rune/codec/ViewKeyEncoding.h
#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <rune/codec/PrimType.h>
#include <rune/codec/Record.h>
#include <rune/codec/UnicodeLower.h>

namespace rune::codec {

inline constexpr uint8_t  kKeyFunctionNone = 0, kKeyFunctionLower = 1, kKeyFunctionCasefold = 2;   // 2 reserved
inline constexpr uint32_t kMaxViewKeyBytes = 128;

[[nodiscard]] constexpr bool isViewKeyType(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32: case PrimType::Int64: case PrimType::UInt16: case PrimType::UInt32:
        case PrimType::UInt64: case PrimType::Enum: case PrimType::Bool: case PrimType::FixedStr: return true;
        default: return false;
    }
}

/// Bytes a view-key component occupies; 0 = not a valid component (wrong type, lower on a non-FixedStr, casefold or an unknown function).
[[nodiscard]] inline uint32_t viewKeyComponentWidth(PrimType t, uint16_t fixedLen, uint8_t keyFunction) noexcept {
    if (!isViewKeyType(t)) return 0;
    if (keyFunction == kKeyFunctionNone) return t == PrimType::FixedStr ? fixedLen : fixedSize(t);
    if (keyFunction == kKeyFunctionLower && t == PrimType::FixedStr) return maxLoweredWidth(fixedLen);
    return 0;
}

namespace detail {

inline void putBigEndian(uint64_t v, std::span<uint8_t> out) noexcept {
    for (std::size_t k = out.size(); k-- > 0; v >>= 8) out[k] = uint8_t(v);
}

inline uint64_t getBigEndian(std::span<const uint8_t> in) noexcept {
    uint64_t v = 0;
    for (uint8_t b : in) v = (v << 8) | b;
    return v;
}

} // namespace detail

/// Order-preserving encoding of one view-key component into exactly `out.size()` (= the component width) bytes.
/// False if `v` is absent, of another type, or (lower) invalid UTF-8 or too wide for `out`.
[[nodiscard]] inline bool encodeKeyComponent(PrimType t, uint8_t keyFunction, const FieldValue& v, std::span<uint8_t> out) noexcept {
    if (!v.present || v.type != t || !isViewKeyType(t)) return false;
    if (keyFunction == kKeyFunctionLower) {
        if (t != PrimType::FixedStr) return false;
        const std::span<const uint8_t> in(reinterpret_cast<const uint8_t*>(v.bytes.data()), v.bytes.size());
        return lowerUtf8(in, out).ok;
    }
    if (keyFunction != kKeyFunctionNone) return false;
    switch (t) {
        case PrimType::Int32:  detail::putBigEndian(uint32_t(int32_t(v.i)) ^ 0x8000'0000u, out); return true;
        case PrimType::Int64:  detail::putBigEndian(uint64_t(v.i) ^ 0x8000'0000'0000'0000ull, out); return true;
        case PrimType::UInt16: case PrimType::UInt32: case PrimType::UInt64: case PrimType::Enum:
            detail::putBigEndian(v.u, out); return true;
        case PrimType::Bool:   detail::putBigEndian(v.b ? 1 : 0, out); return true;
        case PrimType::FixedStr:
            if (v.bytes.size() > out.size()) return false;
            std::memcpy(out.data(), v.bytes.data(), v.bytes.size());
            std::fill(out.begin() + std::ptrdiff_t(v.bytes.size()), out.end(), uint8_t{0});
            return true;
        default: return false;
    }
}

/// The inverse of encodeKeyComponent, for writing a group key's columns into an aggregate's output row: integers, Enum and Bool
/// decode to their value; FixedStr and lower(FixedStr) return a FixedStr view of `in` (a lowered column holds the lowered bytes).
[[nodiscard]] inline FieldValue decodeKeyComponent(PrimType t, uint8_t, std::span<const uint8_t> in) noexcept {
    FieldValue fv;
    fv.type    = t;
    fv.present = true;
    const uint64_t raw = detail::getBigEndian(in);
    switch (t) {
        case PrimType::Int32:  fv.i = int32_t(uint32_t(raw) ^ 0x8000'0000u); break;
        case PrimType::Int64:  fv.i = int64_t(raw ^ 0x8000'0000'0000'0000ull); break;
        case PrimType::Bool:   fv.b = raw != 0; fv.i = raw; break;
        case PrimType::FixedStr: fv.bytes = std::string_view(reinterpret_cast<const char*>(in.data()), in.size()); break;
        default:               fv.u = raw; break;
    }
    return fv;
}

} // namespace rune::codec
