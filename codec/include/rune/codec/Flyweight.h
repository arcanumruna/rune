// include/rune/codec/Flyweight.h — Phase 2b Part B: load/store helpers for the
// generated zero-copy flyweights (XEncoder / XDecoder).
//
// Records are packed little-endian and a claimed buffer is not aligned (the
// producer ring's value offset is 38), so every access is a memcpy — never a
// reinterpret_cast load/store (no aliasing / alignment UB).  On a
// little-endian host the byteswap branches compile away.
#pragma once
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace rune::codec::fly {

/// Store `v` at `p` as packed little-endian (bool: one byte 0/1; double: its
/// IEEE-754 bit pattern).
template <class T>
inline void store(uint8_t* p, T v) noexcept {
    if constexpr (std::is_same_v<T, bool>) {
        *p = v ? uint8_t{1} : uint8_t{0};
    } else if constexpr (std::is_same_v<T, double>) {
        store<uint64_t>(p, std::bit_cast<uint64_t>(v));
    } else {
        static_assert(std::is_integral_v<T>, "fly::store: integral, bool or double");
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) v = std::byteswap(v);
        std::memcpy(p, &v, sizeof v);
    }
}

/// Load a packed little-endian T from `p` (bool: any non-zero byte is true).
template <class T>
[[nodiscard]] inline T load(const uint8_t* p) noexcept {
    if constexpr (std::is_same_v<T, bool>) {
        return *p != 0;
    } else if constexpr (std::is_same_v<T, double>) {
        return std::bit_cast<double>(load<uint64_t>(p));
    } else {
        static_assert(std::is_integral_v<T>, "fly::load: integral, bool or double");
        T v;
        std::memcpy(&v, p, sizeof v);
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) v = std::byteswap(v);
        return v;
    }
}

/// A string(N) field: the first min(N, s.size()) bytes of `s`, zero-padded to N.
inline void storeFixedStr(uint8_t* p, std::string_view s, std::size_t n) noexcept {
    const std::size_t k = s.size() < n ? s.size() : n;
    if (k != 0) std::memcpy(p, s.data(), k);
    if (k < n) std::memset(p + k, 0, n - k);
}

/// A string(N) field's N raw bytes, zero padding included — exactly the bytes
/// the generated struct's decode() copies into its std::array<char, N>.
[[nodiscard]] inline std::string_view loadFixedStr(const uint8_t* p, std::size_t n) noexcept {
    return {reinterpret_cast<const char*>(p), n};
}

}  // namespace rune::codec::fly
