// libs/view/include/rune/view/ViewKey.h
//
// A view row's key as bytes.  Part 07 widens the tables; until then a key of at most 8 bytes maps to the uint64_t API.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace rune::view {

inline constexpr std::size_t kMaxViewKeyBytes = 128;

struct ViewKey {
    std::array<uint8_t, kMaxViewKeyBytes> bytes{};
    uint8_t                               len = 0;

    [[nodiscard]] static ViewKey fromU64(uint64_t v) noexcept {
        ViewKey k;
        std::memcpy(k.bytes.data(), &v, 8);   // little-endian host, like every row slot
        k.len = 8;
        return k;
    }
    [[nodiscard]] std::span<const uint8_t> span() const noexcept { return {bytes.data(), len}; }
    // Requires len <= 8: the zero-extended little-endian load of bytes[0, len).
    [[nodiscard]] uint64_t asU64() const noexcept {
        uint64_t v = 0;
        std::memcpy(&v, bytes.data(), len < 8 ? len : 8);
        return v;
    }
};

} // namespace rune::view
