#pragma once
#include <cstddef>
#include <cstdint>
#include <array>

namespace rune::protocol {

namespace detail {
inline constexpr std::array<uint32_t, 256> makeTable() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k)
            c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        t[i] = c;
    }
    return t;
}
inline constexpr auto kTable = makeTable();
} // namespace detail

// Incremental: continue from a previous CRC value (already ~complemented internally).
inline uint32_t crc32c(uint32_t seed, const uint8_t* data, size_t len) noexcept {
    uint32_t crc = ~seed;
    for (size_t i = 0; i < len; ++i)
        crc = detail::kTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

inline uint32_t crc32c(const uint8_t* data, size_t len) noexcept {
    return crc32c(0u, data, len);
}

} // namespace rune::protocol
