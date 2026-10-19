#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::protocol {

inline size_t writeVarlong(int64_t value, uint8_t* out) noexcept {
    uint64_t v = (static_cast<uint64_t>(value) << 1) ^ (value >> 63); // zigzag
    size_t i = 0;
    while (v >= 0x80) { out[i++] = static_cast<uint8_t>(v) | 0x80; v >>= 7; }
    out[i++] = static_cast<uint8_t>(v);
    return i;
}

inline int64_t readVarlong(const uint8_t* in, size_t& pos) noexcept {
    uint64_t v = 0; int shift = 0; uint8_t b;
    do { b = in[pos++]; v |= static_cast<uint64_t>(b & 0x7F) << shift; shift += 7; } while (b & 0x80);
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); // un-zigzag
}

inline size_t writeVarint(int32_t value, uint8_t* out) noexcept {
    return writeVarlong(static_cast<int64_t>(value), out);
}
inline int32_t readVarint(const uint8_t* in, size_t& pos) noexcept {
    return static_cast<int32_t>(readVarlong(in, pos));
}

} // namespace rune::protocol
