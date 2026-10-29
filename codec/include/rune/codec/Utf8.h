// include/rune/codec/Utf8.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <type_traits>

namespace rune::codec {

enum class Utf8Error : uint8_t { None, InvalidLead, StrayContinuation, Truncated, Overlong, Surrogate, AboveMax };

struct Utf8Result {
    Utf8Error   error  = Utf8Error::None;
    std::size_t offset = 0;   // start of the first invalid sequence
    [[nodiscard]] constexpr bool ok() const noexcept { return error == Utf8Error::None; }
};

/// Well-formedness per Unicode Table 3-7.  Allocation-free; 8 ASCII bytes are skipped per step.
[[nodiscard]] constexpr Utf8Result validateUtf8(std::span<const uint8_t> bytes) noexcept {
    const std::size_t n = bytes.size();
    std::size_t i = 0;
    while (i < n) {
        if (!std::is_constant_evaluated()) {
            while (i + 8 <= n) {
                uint64_t w;
                std::memcpy(&w, bytes.data() + i, 8);
                if ((w & 0x8080808080808080ull) != 0) break;
                i += 8;
            }
            if (i >= n) break;
        }
        const uint8_t b = bytes[i];
        if (b < 0x80) { ++i; continue; }
        if (b < 0xC2) {
            if (b >= 0xC0) return {Utf8Error::Overlong, i};
            return {Utf8Error::StrayContinuation, i};
        }
        if (b > 0xF4) return {Utf8Error::InvalidLead, i};
        const std::size_t len = b < 0xE0 ? 2 : b < 0xF0 ? 3 : 4;
        // The second byte has a lead-specific range; the rest are plain continuations.
        uint8_t lo = 0x80, hi = 0xBF;
        if (b == 0xE0) lo = 0xA0;
        else if (b == 0xED) hi = 0x9F;
        else if (b == 0xF0) lo = 0x90;
        else if (b == 0xF4) hi = 0x8F;
        for (std::size_t k = 1; k < len; ++k) {
            if (i + k >= n) return {Utf8Error::Truncated, i};
            const uint8_t c = bytes[i + k];
            if (k == 1) {
                if (c < lo || c > hi) {
                    if (c < 0x80 || c > 0xBF) return {Utf8Error::Truncated, i};
                    if (b == 0xE0 || b == 0xF0) return {Utf8Error::Overlong, i};
                    if (b == 0xED) return {Utf8Error::Surrogate, i};
                    return {Utf8Error::AboveMax, i};
                }
            } else if (c < 0x80 || c > 0xBF) {
                return {Utf8Error::Truncated, i};
            }
        }
        i += len;
    }
    return {};
}

} // namespace rune::codec
