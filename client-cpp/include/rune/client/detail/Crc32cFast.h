// include/rune/client/detail/Crc32cFast.h — 3-way interleaved CRC-32C
// (backlog B3 / LAT-1 j).
//
// The hardware CRC32C instruction (x86 SSE4.2 crc32q, ARMv8 crc32cx) has a
// latency of ~3 cycles but a throughput of 1 per cycle, so ONE dependency chain
// (rune::protocol::crc32c) runs at ~1/3 of the unit's rate.  Here a block of
// 3*B bytes is split into three independent B-byte chains that the CPU overlaps;
// the three partial registers are then combined with the linearity of the raw
// CRC:
//
//     raw(c, A|B|C) = raw(c, A) * x^(8*2B)  ^  raw(0, B) * x^(8*B)  ^  raw(0, C)
//
// Multiplying by a FIXED power of x mod P is linear over GF(2), so each shift is
// four 256-entry table lookups (tables built once at static initialisation from
// the zlib-style operator in Crc32cPatch.h).  No PCLMUL needed, same code on
// x86-64 and AArch64.  Bit-exact with rune::protocol::crc32c (property tests:
// tests/detail/Crc32cFastTest.cpp).  No allocation, no locks; the tables are
// immutable after static init.
#pragma once

#include <rune/client/detail/Crc32cPatch.h>
#include <rune/protocol/Crc32c.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__aarch64__)
#  include <arm_acle.h>
#elif defined(__x86_64__) || defined(_M_X64)
#  include <nmmintrin.h>
#endif

namespace rune::client::detail {

namespace crc32c_fast {

/// Multiply-by-a-constant (x^(8*bytes) mod P) as 4 byte-indexed tables.
struct ShiftTable {
    std::array<std::array<uint32_t, 256>, 4> t{};
    explicit constexpr ShiftTable(uint64_t bytes) noexcept {
        const uint32_t k = crc32c_patch::x8nModP(bytes);
        for (uint32_t i = 0; i < 4; ++i) {
            for (uint32_t b = 0; b < 256; ++b) t[i][b] = crc32c_patch::multModP(k, b << (8 * i));
        }
    }
    [[nodiscard]] uint32_t operator()(uint32_t c) const noexcept {
        return t[0][c & 0xFFu] ^ t[1][(c >> 8) & 0xFFu] ^ t[2][(c >> 16) & 0xFFu] ^ t[3][c >> 24];
    }
};

inline constexpr std::size_t kBigBlock   = 8192;  // 3 x 8 KiB chains (24 KiB step)
inline constexpr std::size_t kSmallBlock = 512;   // 3 x 512 B chains (1.5 KiB step)

struct Tables {
    ShiftTable big1{kBigBlock}, big2{2 * kBigBlock};
    ShiftTable small1{kSmallBlock}, small2{2 * kSmallBlock};
};
/// Computed at COMPILE time (constant-initialised): never observed zeroed, even from another
/// translation unit's static initialiser (B review).
inline constexpr Tables kTables{};

inline uint64_t load64(const uint8_t* p) noexcept {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

#if defined(__aarch64__)
inline uint32_t step64(uint32_t c, uint64_t v) noexcept { return __crc32cd(c, v); }
inline uint32_t step8(uint32_t c, uint8_t v) noexcept { return __crc32cb(c, v); }
#  define RUNE_CRC_TARGET
#elif defined(__x86_64__) || defined(_M_X64)
#  define RUNE_CRC_TARGET __attribute__((target("sse4.2")))
RUNE_CRC_TARGET inline uint32_t step64(uint32_t c, uint64_t v) noexcept {
    return static_cast<uint32_t>(_mm_crc32_u64(c, v));
}
RUNE_CRC_TARGET inline uint32_t step8(uint32_t c, uint8_t v) noexcept { return _mm_crc32_u8(c, v); }
#endif

#if defined(__aarch64__) || defined(__x86_64__) || defined(_M_X64)
/// Raw (no pre/post inversion) CRC of `n` bytes, three chains per block.
template <std::size_t B>
RUNE_CRC_TARGET inline uint32_t threeWay(uint32_t c, const uint8_t*& p, std::size_t& n, const ShiftTable& s1,
                                         const ShiftTable& s2) noexcept {
    static_assert(B % 8 == 0);
    while (n >= 3 * B) {
        uint32_t a = c, b = 0, d = 0;
        const uint8_t* p1 = p + B;
        const uint8_t* p2 = p + 2 * B;
        for (std::size_t i = 0; i < B; i += 8) {
            a = step64(a, load64(p + i));
            b = step64(b, load64(p1 + i));
            d = step64(d, load64(p2 + i));
        }
        c = s2(a) ^ s1(b) ^ d;
        p += 3 * B;
        n -= 3 * B;
    }
    return c;
}

RUNE_CRC_TARGET inline uint32_t rawHw(uint32_t c, const uint8_t* p, std::size_t n) noexcept {
    const Tables& t = kTables;
    c = threeWay<kBigBlock>(c, p, n, t.big1, t.big2);
    c = threeWay<kSmallBlock>(c, p, n, t.small1, t.small2);
    while (n >= 8) {
        c = step64(c, load64(p));
        p += 8;
        n -= 8;
    }
    while (n-- != 0) c = step8(c, *p++);
    return c;
}
#endif

}  // namespace crc32c_fast

#undef RUNE_CRC_TARGET

/// CRC-32C of data[0, len) — the value rune::protocol::crc32c returns, computed
/// with three interleaved hardware chains for long inputs.
inline uint32_t crc32cFast(const uint8_t* data, std::size_t len) noexcept {
#if defined(__aarch64__)
    return ~crc32c_fast::rawHw(~0u, data, len);
#elif defined(__x86_64__) || defined(_M_X64)
    if (::rune::protocol::detail::crc32c_hw_available()) return ~crc32c_fast::rawHw(~0u, data, len);
    return ::rune::protocol::crc32c(data, len);
#else
    return ::rune::protocol::crc32c(data, len);
#endif
}

}  // namespace rune::client::detail
