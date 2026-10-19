#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <array>

#if defined(__aarch64__)
#  include <arm_acle.h>
#elif defined(__x86_64__) || defined(_M_X64)
#  include <nmmintrin.h>   // SSE4.2 _mm_crc32_*
#endif

namespace rune::protocol {

namespace detail {

// Software (Sarwate table) CRC32C — the portable fallback and the reference the
// hardware path must match bit-for-bit.  Castagnoli polynomial 0x82F63B78.
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

// Raw CRC step over `len` bytes (no pre/post complement) — software.
inline uint32_t crc32c_sw(uint32_t crc, const uint8_t* data, size_t len) noexcept {
    for (size_t i = 0; i < len; ++i)
        crc = kTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc;
}

#if defined(__x86_64__) || defined(_M_X64)
// SSE4.2 hardware CRC32C.  Attribute-targeted so it compiles without a global
// -msse4.2; guarded at the call site by a runtime CPU check.  The _mm_crc32_*
// intrinsics compute exactly the reflected CRC32C step, so the result is
// identical to crc32c_sw for the same input.
__attribute__((target("sse4.2")))
inline uint32_t crc32c_hw(uint32_t crc, const uint8_t* data, size_t len) noexcept {
    while (len >= 8) {
        uint64_t v; std::memcpy(&v, data, 8);
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
        data += 8; len -= 8;
    }
    while (len--) crc = _mm_crc32_u8(crc, *data++);
    return crc;
}
inline bool crc32c_hw_available() noexcept {
    return __builtin_cpu_supports("sse4.2");
}
#endif

#if defined(__aarch64__)
// Linux arm64 (GCC/clang without -march=...+crc): the ACLE CRC intrinsics are always_inline and refuse to inline
// into a function compiled without the CRC extension — enable it for this function only.
#  if defined(__ARM_FEATURE_CRC32)
#    define RUNE_PROTOCOL_CRC_TARGET
#  elif defined(__clang__)
#    define RUNE_PROTOCOL_CRC_TARGET __attribute__((target("crc")))
#  else
#    define RUNE_PROTOCOL_CRC_TARGET __attribute__((target("+crc")))
#  endif
RUNE_PROTOCOL_CRC_TARGET inline uint32_t crc32c_arm(uint32_t crc, const uint8_t* data, size_t len) noexcept {
    while (len >= 8) {
        uint64_t v; std::memcpy(&v, data, 8);
        crc = __crc32cd(crc, v);
        data += 8; len -= 8;
    }
    while (len--) crc = __crc32cb(crc, *data++);
    return crc;
}
#  undef RUNE_PROTOCOL_CRC_TARGET
#endif

} // namespace detail

// Incremental CRC32C: continue from a previous value.  Uses ARMv8 / SSE4.2
// hardware CRC where available (multi-GB/s), falling back to the software table.
inline uint32_t crc32c(uint32_t seed, const uint8_t* data, size_t len) noexcept {
    uint32_t crc = ~seed;
#if defined(__aarch64__)
    // ARMv8 CRC (present on Apple Silicon + all server ARM we target).
    crc = detail::crc32c_arm(crc, data, len);
#elif defined(__x86_64__) || defined(_M_X64)
    if (detail::crc32c_hw_available())
        crc = detail::crc32c_hw(crc, data, len);
    else
        crc = detail::crc32c_sw(crc, data, len);
#else
    crc = detail::crc32c_sw(crc, data, len);
#endif
    return ~crc;
}

inline uint32_t crc32c(const uint8_t* data, size_t len) noexcept {
    return crc32c(0u, data, len);
}

} // namespace rune::protocol
