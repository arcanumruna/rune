#pragma once
// FilterWire.h — Phase 2c: the subscription-filter wire format constants and helpers (spec §5).
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace rune::codec {

inline constexpr uint8_t     kFilterFormatVer       = 1;
inline constexpr std::size_t kMaxFilterTerms        = 8;
inline constexpr std::size_t kMaxFilterInValues     = 64;
inline constexpr std::size_t kMaxFilterFixedStrLen  = 16;
inline constexpr std::size_t kFilterHeaderBytes     = 2;
inline constexpr std::size_t kFilterTermHeaderBytes = 5;
inline constexpr std::size_t kMaxFilterBytes =
    kFilterHeaderBytes + kMaxFilterTerms * (kFilterTermHeaderBytes + kMaxFilterInValues * kMaxFilterFixedStrLen);
static_assert(kMaxFilterBytes == 8234, "spec §5");

enum class FilterOp : uint8_t { Eq = 1, In = 2 };   // wire values, append-only

[[nodiscard]] constexpr bool isFilterableType(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32: case PrimType::Int64: case PrimType::UInt16: case PrimType::UInt32:
        case PrimType::UInt64: case PrimType::Enum: case PrimType::Bool: case PrimType::FixedStr: return true;
        default: return false;
    }
}

[[nodiscard]] inline bool isFilterable(const FieldDescriptor& f) noexcept {
    if (!isFilterableType(f.type)) return false;
    return f.type != PrimType::FixedStr || (f.fixedLen >= 1 && f.fixedLen <= kMaxFilterFixedStrLen);
}

/// Byte width of one filter value; 0 = not filterable.
[[nodiscard]] constexpr uint8_t filterValueWidth(PrimType t, uint16_t fixedLen) noexcept {
    switch (t) {
        case PrimType::Int32: case PrimType::UInt32: case PrimType::Enum: return 4;
        case PrimType::Int64: case PrimType::UInt64: return 8;
        case PrimType::UInt16: return 2;
        case PrimType::Bool: return 1;
        case PrimType::FixedStr:
            return (fixedLen >= 1 && fixedLen <= kMaxFilterFixedStrLen) ? static_cast<uint8_t>(fixedLen) : uint8_t{0};
        default: return 0;
    }
}

/// A filter value as a 128-bit little-endian key (zero-extended); compares hi first, then lo.
struct FilterKey {
    uint64_t hi = 0;
    uint64_t lo = 0;
    friend constexpr auto operator<=>(const FilterKey&, const FilterKey&) = default;
};

[[nodiscard]] constexpr FilterKey loadFilterKey(const uint8_t* p, uint8_t width) noexcept {
    FilterKey k;
    for (uint8_t i = 0; i < width && i < 16; ++i) {
        if (i < 8) k.lo |= static_cast<uint64_t>(p[i]) << (8 * i);
        else       k.hi |= static_cast<uint64_t>(p[i]) << (8 * (i - 8));
    }
    return k;
}

struct FilterBytes {
    std::array<uint8_t, kMaxFilterBytes> bytes{};
    uint16_t len = 0;   // 0 = no filter
    [[nodiscard]] constexpr std::span<const uint8_t> span() const noexcept { return {bytes.data(), len}; }
    [[nodiscard]] constexpr bool empty() const noexcept { return len == 0; }
    friend constexpr bool operator==(const FilterBytes& a, const FilterBytes& b) noexcept {
        if (a.len != b.len) return false;
        for (std::size_t i = 0; i < a.len; ++i) if (a.bytes[i] != b.bytes[i]) return false;
        return true;
    }
};

enum class FilterErrorKind : uint8_t {
    Malformed, BadFormatVersion, TermCount, UnknownOp, ValueCount, NotCanonical,
    TooManyTerms, TooManyValues, ValueOutOfRange, UnknownTag, TypeMismatch,
    VariableLength, FloatField, FixedStrTooLong, UntypedTopic, Capacity, BufferTooSmall, UnknownField };

struct FilterError {
    FilterErrorKind kind      = FilterErrorKind::Malformed;
    uint8_t         termIndex = 0xFF;
    uint16_t        tag       = 0;
};

/// Cold path: the human-readable detail text (spec §9).
[[nodiscard]] inline std::string describe(const FilterError& e) {
    const bool has = e.termIndex != 0xFF;
    const std::string i = std::to_string(e.termIndex);
    const std::string term = "filter term " + i;
    const std::string termTag = term + " (tag " + std::to_string(e.tag) + "): ";
    switch (e.kind) {
        case FilterErrorKind::Malformed:        return has ? term + ": malformed" : "filter: malformed";
        case FilterErrorKind::BadFormatVersion: return "filter: unknown format version";
        case FilterErrorKind::TermCount:        return "filter: term count must be 1..8";
        case FilterErrorKind::UnknownOp:        return term + ": unknown op";
        case FilterErrorKind::ValueCount:       return term + ": value count must be 1 for Eq and 1..64 for In";
        case FilterErrorKind::NotCanonical:     return term + ": not canonical (terms by tag, In values sorted and unique)";
        case FilterErrorKind::TooManyTerms:     return "filter: more than 8 terms";
        case FilterErrorKind::TooManyValues:    return term + ": more than 64 values";
        case FilterErrorKind::ValueOutOfRange:  return termTag + "value out of the field's range";
        case FilterErrorKind::UnknownTag:       return termTag + "unknown tag";
        case FilterErrorKind::TypeMismatch:     return termTag + "type mismatch";
        case FilterErrorKind::VariableLength:   return termTag + "variable-length field";
        case FilterErrorKind::FloatField:       return termTag + "float field";
        case FilterErrorKind::FixedStrTooLong:  return termTag + "string(N) longer than 16";
        case FilterErrorKind::UntypedTopic:     return "filter needs a typed topic";
        case FilterErrorKind::Capacity:         return "filter capacity";
        case FilterErrorKind::BufferTooSmall:   return "filter: output buffer too small";
        case FilterErrorKind::UnknownField:     return term + ": unknown field";
    }
    return "filter: malformed";
}

}  // namespace rune::codec
