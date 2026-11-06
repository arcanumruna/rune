// libs/view/include/rune/view/ViewKeyHash.h
//
// The seeded hash of a wide view key (K > 8): wyhash v4, the one SkimpyOffsetMap uses.
#pragma once
#include <cstdint>
#include <span>
#include <rune/storage/WyHash.h>

namespace rune::view {

[[nodiscard]] inline uint64_t viewKeyHash(std::span<const uint8_t> bytes, uint64_t seed) noexcept {
    return rune::storage::detail::wyhash64(bytes.data(), bytes.size(), seed);
}

} // namespace rune::view
