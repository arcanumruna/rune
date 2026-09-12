#pragma once
// PartitionHash.h — Canonical key→partition hash shared by broker and client.
//
// Algorithm: FNV-1a 64-bit (Fowler–Noll–Vo)
//   - Public domain, stable, endianness-independent (operates on bytes only).
//   - No hardware intrinsics, zero allocation, constexpr-friendly.
//   - Broker (GB-1) and client (CB-2) MUST include THIS header — never diverge.
//
// API:
//   fnv1a64(data, len) → uint64_t   — raw hash of arbitrary bytes
//   partitionForKey(keyBytes, partitionCount) → int32_t   — [0, partitionCount)
//
// Stability guarantee: the FNV-1a offset basis and prime are fixed constants;
// the result is fully determined by the byte content of the key — no platform-
// specific byte-order or SIMD instruction is involved.

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::protocol {

// ---------------------------------------------------------------------------
// FNV-1a 64-bit parameters (never change — shared by broker + client)
// ---------------------------------------------------------------------------
inline constexpr uint64_t kFnv1aOffset64 = 14695981039346656037ULL;
inline constexpr uint64_t kFnv1aPrime64  = 1099511628211ULL;

// ---------------------------------------------------------------------------
// fnv1a64 — raw hash over a byte range.
// Operates purely on byte values: endianness-independent.
// constexpr when key contents are constant.
// ---------------------------------------------------------------------------
[[nodiscard]] inline constexpr uint64_t
fnv1a64(const uint8_t* data, std::size_t len) noexcept
{
    uint64_t h = kFnv1aOffset64;
    for (std::size_t i = 0; i < len; ++i) {
        h ^= static_cast<uint64_t>(data[i]);
        h *= kFnv1aPrime64;
    }
    return h;
}

// ---------------------------------------------------------------------------
// partitionForKey — map an opaque key to a partition index in [0, partitionCount).
//
// The formula is:   fnv1a64(key) % partitionCount
// Precondition:     partitionCount >= 1  (caller must validate from TopicSpec)
// ---------------------------------------------------------------------------
[[nodiscard]] inline constexpr int32_t
partitionForKey(std::span<const uint8_t> key, int32_t partitionCount) noexcept
{
    const uint64_t h = fnv1a64(key.data(), key.size());
    return static_cast<int32_t>(h % static_cast<uint64_t>(partitionCount));
}

} // namespace rune::protocol
