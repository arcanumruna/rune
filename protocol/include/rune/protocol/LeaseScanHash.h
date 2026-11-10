#pragma once
// LeaseScanHash.h - the ORDER of a ScanLeases scan (spec 8.3), shared by the broker (node/lease/LeaseHash.h forwards here) and the client
// (the lease watch decides, per key, which scan page covers it).
//
// Rows are paged in ascending order of scanHash(key, sourceTopicId): wyhash64 of the key bytes seeded with the SOURCE topic's id.  A pure
// function of (key bytes, topic id).  Pinned by ScanLeasesRegression.ScanHashGoldenRegression (broker) and LeaseScanHashGoldenRegression
// (client): changing it is a wire (version-skew) change.
#include <rune/protocol/WyHash.h>

#include <cstdint>
#include <span>

namespace rune::protocol {

[[nodiscard]] inline uint64_t scanHash(std::span<const uint8_t> key, int32_t sourceTopicId) noexcept {
    return detail::wyhash64(key.data(), key.size(), static_cast<uint64_t>(static_cast<int64_t>(sourceTopicId)));
}

}  // namespace rune::protocol
