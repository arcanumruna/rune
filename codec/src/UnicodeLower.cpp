// src/UnicodeLower.cpp
#include "rune/codec/UnicodeLower.h"
#include "rune/codec/Utf8.h"
#include <algorithm>
#include <array>

namespace rune::codec {
namespace {
#include "UnicodeLowerTable.inc"

static_assert(2 * kMaxLoweredLenBySrcLen[1] <= kMaxLoweredLenBySrcLen[2]
              && kMaxLoweredLenBySrcLen[3] <= kMaxLoweredLenBySrcLen[2] + kMaxLoweredLenBySrcLen[1]
              && kMaxLoweredLenBySrcLen[4] <= 2 * kMaxLoweredLenBySrcLen[2],
              "maxLoweredWidth assumes 2-byte sources are the best growers");

uint32_t encodeUtf8(char32_t cp, uint8_t* p) noexcept {
    if (cp < 0x80) { p[0] = uint8_t(cp); return 1; }
    if (cp < 0x800) { p[0] = uint8_t(0xC0 | (cp >> 6)); p[1] = uint8_t(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) {
        p[0] = uint8_t(0xE0 | (cp >> 12)); p[1] = uint8_t(0x80 | ((cp >> 6) & 0x3F)); p[2] = uint8_t(0x80 | (cp & 0x3F));
        return 3;
    }
    p[0] = uint8_t(0xF0 | (cp >> 18)); p[1] = uint8_t(0x80 | ((cp >> 12) & 0x3F));
    p[2] = uint8_t(0x80 | ((cp >> 6) & 0x3F)); p[3] = uint8_t(0x80 | (cp & 0x3F));
    return 4;
}
} // namespace

char32_t lowerCodePoint(char32_t cp) noexcept {
    if (cp > 0x10FFFF) return cp;
    return char32_t(int32_t(cp) + kLowerStage2[std::size_t{kLowerStage1[cp >> 7]} * 128 + (cp & 127)]);
}

LowerResult lowerUtf8(std::span<const uint8_t> in, std::span<uint8_t> out) noexcept {
    std::size_t i = 0, o = 0;
    const std::size_t n = in.size(), cap = out.size();
    while (i < n && in[i] != 0) {
        const uint8_t b = in[i];
        if (b < 0x80) {
            if (o >= cap) return {};
            out[o++] = (b >= 'A' && b <= 'Z') ? uint8_t(b + 0x20) : b;
            ++i;
            continue;
        }
        const std::size_t len = b < 0xE0 ? 2 : b < 0xF0 ? 3 : 4;
        if (i + len > n) return {};
        if (!validateUtf8(in.subspan(i, len)).ok()) return {};
        char32_t cp;
        if (len == 2) cp = ((b & 0x1Fu) << 6) | (in[i + 1] & 0x3Fu);
        else if (len == 3) cp = ((b & 0x0Fu) << 12) | ((in[i + 1] & 0x3Fu) << 6) | (in[i + 2] & 0x3Fu);
        else cp = ((b & 0x07u) << 18) | ((in[i + 1] & 0x3Fu) << 12) | ((in[i + 2] & 0x3Fu) << 6) | (in[i + 3] & 0x3Fu);
        std::array<uint8_t, 4> enc;
        const uint32_t elen = encodeUtf8(lowerCodePoint(cp), enc.data());
        if (o + elen > cap) return {};
        std::copy_n(enc.data(), elen, out.data() + o);
        o += elen;
        i += len;
    }
    std::fill(out.begin() + std::ptrdiff_t(o), out.end(), uint8_t{0});
    return {true, uint32_t(o)};
}

uint32_t maxLoweredWidth(uint32_t n) noexcept {
    return (n / 2) * kMaxLoweredLenBySrcLen[2] + (n % 2) * kMaxLoweredLenBySrcLen[1];
}

} // namespace rune::codec
