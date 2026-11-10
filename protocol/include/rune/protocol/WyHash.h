#pragma once
// Shared by the broker (storage/WyHash.h forwards here) and the client (the lease watch computes the broker's scan hash).
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::protocol {

// ---------------------------------------------------------------------------
// wyhash — inline, header-only implementation.
// Based on wyhash v4 (public domain, Wang Yi).
// Reference: https://github.com/wangyi-fudan/wyhash
// This is the fast 64-bit non-crypto hash used in place of Kafka's MD5.
//
// Key fix vs naive adaption: the 1-7 byte path mixes the start, middle, and
// end bytes into two 64-bit accumulators AND mixes the seed in separately so
// that keys of the same length with different characters produce distinct hashes.
// ---------------------------------------------------------------------------
namespace detail {

inline uint64_t wyread8(const void* p) {
    uint64_t v; std::memcpy(&v, p, sizeof(v)); return v;
}
inline uint64_t wyread4(const void* p) {
    uint32_t v; std::memcpy(&v, p, sizeof(v)); return static_cast<uint64_t>(v);
}
inline uint64_t wyread3(const uint8_t* p, size_t k) {
    return (static_cast<uint64_t>(p[0]) << 16) |
           (static_cast<uint64_t>(p[k >> 1]) << 8) |
            static_cast<uint64_t>(p[k - 1]);
}
// 128-bit multiply then XOR-fold (one MULQ/UMULH instruction on x86-64/aarch64).
inline uint64_t wymix(uint64_t a, uint64_t b) {
    __uint128_t r = static_cast<__uint128_t>(a) * b;
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
}

// wyhash v4 secret constants (random primes).
static constexpr uint64_t kSecret0 = 0xa0761d6478bd642fULL;
static constexpr uint64_t kSecret1 = 0xe7037ed1a0b428dbULL;
static constexpr uint64_t kSecret2 = 0x8ebc6af09c88c6e3ULL;
static constexpr uint64_t kSecret3 = 0x589965cc75374cc3ULL;

inline uint64_t wyhash64(const void* key, size_t len,
                          uint64_t seed = 0x517cc1b727220a95ULL) {
    const uint8_t* p = static_cast<const uint8_t*>(key);
    seed ^= wymix(seed ^ kSecret0, kSecret1);
    uint64_t a, b;
    if (len <= 16) {
        if (len >= 4) {
            a = (wyread4(p) << 32) | wyread4(p + ((len >> 3) << 2));
            b = (wyread4(p + len - 4) << 32) | wyread4(p + len - 4 - ((len >> 3) << 2));
        } else if (len > 0) {
            a = wyread3(p, len);
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len;
        if (i > 48) {
            uint64_t see1 = seed, see2 = seed;
            do {
                seed  = wymix(wyread8(p)     ^ kSecret1, wyread8(p + 8)  ^ seed);
                see1  = wymix(wyread8(p + 16)^ kSecret2, wyread8(p + 24) ^ see1);
                see2  = wymix(wyread8(p + 32)^ kSecret3, wyread8(p + 40) ^ see2);
                p += 48; i -= 48;
            } while (i > 48);
            seed ^= see1 ^ see2;
        }
        while (i > 16) {
            seed = wymix(wyread8(p) ^ kSecret1, wyread8(p + 8) ^ seed);
            i -= 16; p += 16;
        }
        a = wyread8(p + i - 16);
        b = wyread8(p + i - 8);
    }
    return wymix(kSecret1 ^ static_cast<uint64_t>(len), wymix(a ^ kSecret1, b ^ seed));
}

} // namespace detail

} // namespace rune::protocol
