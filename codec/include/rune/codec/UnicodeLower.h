// include/rune/codec/UnicodeLower.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::codec {

/// The Unicode version the lowercase table is generated from (data/UnicodeData-16.0.0.txt).
inline constexpr uint32_t    kUnicodeVersion          = (16u << 16) | (0u << 8) | 0u;
inline constexpr std::size_t kSimpleLowercaseMappings = 1460;

[[nodiscard]] constexpr uint32_t packUnicodeVersion(uint8_t major, uint8_t minor, uint8_t update) noexcept {
    return (uint32_t{major} << 16) | (uint32_t{minor} << 8) | uint32_t{update};
}
[[nodiscard]] constexpr bool unicodeVersionSupported(uint32_t packed) noexcept { return packed == kUnicodeVersion; }

/// Simple_Lowercase_Mapping of `cp`, or `cp` itself when it has none.  No locale rules, no normalization.
[[nodiscard]] char32_t lowerCodePoint(char32_t cp) noexcept;

struct LowerResult {
    bool     ok  = false;   // false: invalid UTF-8, or `out` too small
    uint32_t len = 0;       // lowered bytes written (before the zero fill)
};

/// Lowers the UTF-8 in `in` up to its first 0x00 byte (string(N) padding) into `out`, then zero-fills `out` to its end.
[[nodiscard]] LowerResult lowerUtf8(std::span<const uint8_t> in, std::span<uint8_t> out) noexcept;

/// The most bytes `n` source bytes can lower to.
[[nodiscard]] uint32_t maxLoweredWidth(uint32_t n) noexcept;

} // namespace rune::codec
