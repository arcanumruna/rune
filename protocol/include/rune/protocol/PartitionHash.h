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
//   partitionForKey(keyBytes, partitionCount) → int32_t   — [0, partitionCount);
//                                                          empty key → 0
//   partitionFor(keyBytes, partitionCount, partitioner) → int32_t — the topic's
//                                                          partitioner (Fnv1a | Modulo)
//
// Stability guarantee: the FNV-1a offset basis and prime are fixed constants;
// the result is fully determined by the byte content of the key — no platform-
// specific byte-order or SIMD instruction is involved.

#include <rune/protocol/CatalogTypes.h>

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
//                   — except an EMPTY key (or partitionCount <= 1) is
//                   partition 0 (Phase 2b Part C: Key{} = "no key" lands on
//                   partition 0 for broker and client alike).
// Precondition:     partitionCount >= 1  (caller must validate from TopicSpec)
//
// This is the ONE routing rule broker (TopicRegistry::partitionForKey) and
// client (rune::client::Key::partition) share — never special-case it on one
// side only.
// ---------------------------------------------------------------------------
[[nodiscard]] inline constexpr int32_t
partitionForKey(std::span<const uint8_t> key, int32_t partitionCount) noexcept
{
    if (key.empty() || partitionCount <= 1) return 0;
    const uint64_t h = fnv1a64(key.data(), key.size());
    return static_cast<int32_t>(h % static_cast<uint64_t>(partitionCount));
}

// ---------------------------------------------------------------------------
// partitionFor — the key -> partition function of a topic's Partitioner.
//   Fnv1a:  partitionForKey.
//   Modulo: an autoincrement id routes to (id mod P), the id being the key read as a little-endian
//           unsigned integer of 1-8 bytes.  An empty key, a key wider than 8 bytes or P <= 1 is 0.
// The ONE routing rule for the client, TopicRegistry::partitionForKey and the view-read path.
// ---------------------------------------------------------------------------
[[nodiscard]] inline constexpr int32_t
partitionFor(std::span<const uint8_t> key, int32_t partitionCount, Partitioner partitioner) noexcept
{
    if (partitioner == Partitioner::Fnv1a) return partitionForKey(key, partitionCount);
    if (key.empty() || key.size() > 8 || partitionCount <= 1) return 0;
    uint64_t id = 0;
    for (std::size_t i = 0; i < key.size(); ++i) id |= static_cast<uint64_t>(key[i]) << (8 * i);
    return static_cast<int32_t>(id % static_cast<uint64_t>(partitionCount));
}

} // namespace rune::protocol
