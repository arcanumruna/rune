#pragma once
// LeaseKey.h - lease topics Task 13a / 13b: the wire form of a legacy GetViewRow u64 key on a lease view.
//
// A lease topic's key is its key field's own width (spec 3): 8 bytes for a (U)Int64 key, but 2 / 4 bytes for a UInt16 / UInt32 / Int32 key
// and up to 128 for a FixedStr.  The legacy GetViewRow carries the key as a u64, so the u64 is encoded to the key's wire form before it is
// hashed to a partition (the gateway) or looked up (the worker).  ONE function for both, so the two can never disagree about which
// partition owns a key.
#include <rune/codec/PrimType.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node::lease {

/// Writes the key bytes of the legacy u64 `v` for a lease topic whose key is `keyType` of `keyWidth` bytes (0 = unknown: 8 raw bytes) into
/// `out` and returns their count (the width), or 0 when `v` does not fit the key's type.  A key of 8 bytes or more takes the u64's 8
/// little-endian bytes as they are (a wider key is then refused by the caller's width check).  A narrower integer key is the u64 encoded to
/// that width: an Int32 reads the u64 as its sign-extension (so [INT32_MIN, INT32_MAX]), a UInt16 / UInt32 must have no higher bits; any other
/// narrow type has no u64 form.
[[nodiscard]] inline std::size_t leaseKeyFromLegacyU64(uint64_t v, rune::codec::PrimType keyType, std::size_t keyWidth,
                                                       std::array<uint8_t, 8>& out) noexcept {
    std::size_t width = 8;
    if (keyWidth != 0 && keyWidth < 8) {
        bool fits = false;
        if (keyType == rune::codec::PrimType::Int32)
            fits = static_cast<int64_t>(v) >= INT32_MIN && static_cast<int64_t>(v) <= INT32_MAX;
        else if (keyType == rune::codec::PrimType::UInt16 || keyType == rune::codec::PrimType::UInt32)
            fits = (v >> (8 * keyWidth)) == 0;
        if (!fits) return 0;
        width = keyWidth;
    }
    std::memcpy(out.data(), &v, 8);
    return width;
}

}  // namespace rune::node::lease
