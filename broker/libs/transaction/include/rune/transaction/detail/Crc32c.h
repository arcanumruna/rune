#pragma once
// Crc32c.h — software CRC-32C (Castagnoli, reflected poly 0x82F63B78) with a
// COMPILE-TIME table.  F15 S1 (design note §6 minor cleanups): replaces the three
// lazily-filled tables guarded by std::call_once (a synchronisation primitive —
// the shared-nothing law allows none outside the cleaner and ring internals).
// A constexpr table needs no initialisation, so it is race-free by construction
// on every thread.

#include <array>
#include <cstddef>
#include <cstdint>

namespace rune::transaction::detail {

inline constexpr std::array<std::uint32_t, 256> kCrc32cTable = [] {
    std::array<std::uint32_t, 256> t{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (0x82F63B78u & (0u - (c & 1u)));
        t[i] = c;
    }
    return t;
}();

/// Feed `len` bytes into a running (pre-inverted) CRC state; returns the
/// final (post-inverted) CRC-32C.  `crc` defaults to the standard init.
[[nodiscard]] constexpr std::uint32_t crc32c(const std::uint8_t* data, std::size_t len,
                                             std::uint32_t crc = 0xFFFFFFFFu) noexcept {
    for (std::size_t i = 0; i < len; ++i) crc = (crc >> 8) ^ kCrc32cTable[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

// Known-answer check ("123456789" -> 0xE3069283, RFC 3720 B.4), at compile time.
static_assert([] {
    constexpr std::uint8_t kCheck[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    return crc32c(kCheck, sizeof(kCheck)) == 0xE3069283u;
}(), "CRC-32C known answer");

} // namespace rune::transaction::detail
