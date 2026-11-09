#pragma once
// LeaseHash.h - lease topics Task 13b (spec 8.3): the ORDER of a ScanLeases scan.
//
// Rows are paged in ascending order of scanHash(key, sourceTopicId), a 64-bit hash of the key bytes seeded with the SOURCE topic's id.
// The hash is a pure function of (key bytes, topic id): the same on every node and for every table size, never the ViewTable's per-node
// probe seed - so a client that resumes after `lastHash` on ANOTHER broker (a leader change) or after the table grew sees every key it has
// not seen yet exactly once.  It is only ever computed by brokers (the client treats lastHash as an opaque cursor), so any deterministic
// seedable function would do; the repo's wyhash is used (no new dependency).  The golden values are pinned by
// ScanLeasesRegression.ScanHashGoldenRegression: changing this function is a wire (version-skew) change.
#include <rune/storage/WyHash.h>

#include <cstdint>
#include <span>

namespace rune::node::lease {

[[nodiscard]] inline uint64_t scanHash(std::span<const uint8_t> key, int32_t sourceTopicId) noexcept {
    return rune::storage::detail::wyhash64(key.data(), key.size(), static_cast<uint64_t>(static_cast<int64_t>(sourceTopicId)));
}

}  // namespace rune::node::lease
