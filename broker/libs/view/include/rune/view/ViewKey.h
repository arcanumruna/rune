// libs/view/include/rune/view/ViewKey.h
//
// A view row's key as bytes.  Part 07 widens the tables; until then a key of at most 8 bytes maps to the uint64_t API.
#pragma once
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <cassert>
#include <rune/codec/ViewKeyEncoding.h>

namespace rune::view {

inline constexpr std::size_t kMaxViewKeyBytes = rune::codec::kMaxViewKeyBytes;

static_assert(std::endian::native == std::endian::little, "narrow integer keys are copied into key bytes in memory order");

struct ViewKey {
    std::array<uint8_t, kMaxViewKeyBytes> bytes{};
    uint8_t                               len = 0;

    [[nodiscard]] static ViewKey fromU64(uint64_t v) noexcept {
        ViewKey k;
        std::memcpy(k.bytes.data(), &v, 8);   // little-endian host, like every row slot
        k.len = 8;
        return k;
    }
    [[nodiscard]] static ViewKey fromBytes(std::span<const uint8_t> b) noexcept {
        assert(b.size() <= kMaxViewKeyBytes);
        ViewKey k;
        std::memcpy(k.bytes.data(), b.data(), b.size());
        k.len = static_cast<uint8_t>(b.size());
        return k;
    }
    [[nodiscard]] bool operator==(const ViewKey& o) const noexcept {
        return len == o.len && std::memcmp(bytes.data(), o.bytes.data(), len) == 0;
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
