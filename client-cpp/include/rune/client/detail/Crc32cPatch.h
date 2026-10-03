// include/rune/client/detail/Crc32cPatch.h — incremental CRC-32C update after an
// in-place patch of a few bytes (backlog B2 / LAT-1 i).
//
// CRC-32C (init ~0, xorout ~0) is affine over GF(2): for two messages A, B of the
// SAME length, crc(A) ^ crc(B) = raw(A ^ B), where raw is the bare polynomial
// remainder (init 0, no xorout).  A ^ B is zero except over the patched bytes
// [off, off + n), and leading zeros do not change raw, so
//
//     crc(B) = crc(A) ^ raw(delta) * x^(8 * (len - off - n))   (mod P)
//
// raw(delta) is an n-byte table CRC; the shift by the trailing zero bytes is
// zlib's crc32_combine operator (x^(8k) mod P by squaring, O(log k) GF(2)
// multiplies).  Cost is independent of the frame length — no rescan.
//
// Bit-exact with a full recompute (randomized property tests:
// tests/detail/Crc32cPatchTest.cpp).  No allocation, no branches on data.
#pragma once

#include <rune/protocol/Crc32c.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace rune::client::detail {

namespace crc32c_patch {

inline constexpr uint32_t kPoly = 0x82F63B78u;  // CRC-32C (Castagnoli), reflected

/// a * b mod P (reflected GF(2) polynomials; bit 31 = x^0).
constexpr uint32_t multModP(uint32_t a, uint32_t b) noexcept {
    uint32_t m = 1u << 31;
    uint32_t p = 0;
    for (;;) {
        if ((a & m) != 0) {
            p ^= b;
            if ((a & (m - 1)) == 0) break;
        }
        m >>= 1;
        b = (b & 1u) != 0 ? (b >> 1) ^ kPoly : b >> 1;
        if (m == 0) break;
    }
    return p;
}

/// kX2n[k] = x^(2^k) mod P.
inline constexpr std::array<uint32_t, 32> kX2n = [] {
    std::array<uint32_t, 32> t{};
    uint32_t p = 1u << 30;  // x^1
    t[0] = p;
    for (std::size_t n = 1; n < 32; ++n) t[n] = p = multModP(p, p);
    return t;
}();

/// x^(8 * bytes) mod P — the operator that appends `bytes` zero bytes.
constexpr uint32_t x8nModP(uint64_t bytes) noexcept {
    uint32_t p = 1u << 31;  // x^0
    unsigned k = 3;         // 8 * bytes = bytes << 3
    while (bytes != 0) {
        if ((bytes & 1u) != 0) p = multModP(kX2n[k & 31u], p);
        bytes >>= 1;
        ++k;
    }
    return p;
}

}  // namespace crc32c_patch

/// The CRC-32C of a `regionLen`-byte region after `n` bytes at `off` changed
/// from `oldBytes` to `newBytes`, given its CRC before the patch.  Requires
/// off + n <= regionLen.  Equal bytes contribute nothing (an unchanged patch
/// returns `crc` unchanged).
inline uint32_t crc32cPatch(uint32_t crc, std::size_t regionLen, std::size_t off,
                            const uint8_t* oldBytes, const uint8_t* newBytes,
                            std::size_t n) noexcept {
    const auto& t = ::rune::protocol::detail::kTable;
    uint32_t raw = 0;
    for (std::size_t i = 0; i < n; ++i) {
        raw = t[(raw ^ static_cast<uint32_t>(oldBytes[i] ^ newBytes[i])) & 0xFFu] ^ (raw >> 8);
    }
    if (raw == 0) return crc;
    return crc ^ crc32c_patch::multModP(crc32c_patch::x8nModP(regionLen - off - n), raw);
}

}  // namespace rune::client::detail
