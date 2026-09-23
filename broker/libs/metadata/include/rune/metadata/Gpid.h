// Gpid.h — single source of truth for the global partition id (GPID) scheme.
//
// GPID SCHEME
// ──────────
// A global partition id (gpid) is a compact integer that uniquely identifies
// a (topicId, partition) pair across the cluster:
//
//   gpid = topicId * kMaxGpidPartitions + partition
//
// Constraints:
//   - topicId  ≥ 0
//   - 0 ≤ partition < kMaxGpidPartitions
//   - kMaxGpidPartitions = 1024  (power of two; never change without a
//     protocol-version bump — all routing images and tests assume it)
//
// Usage:
//   int32_t gpid = rune::metadata::gpidOf(topicId, partition);
//
// This header is intentionally small and header-only.  It has no includes
// beyond <cstdint> so it can be included from any layer without pulling in
// the full RoutingImage machinery.
//
// All production call sites (ClientGateway, NodeRuntimeAgent) and all tests
// (MultiLeaderProduceTest, MetadataRequestTest, GatewayDataBench) must import
// THIS header rather than defining kMaxGpidPartitions locally.

#pragma once

#include <cstdint>

namespace rune::metadata {

/// Maximum number of partitions per topic for GPID encoding.
/// gpid = topicId * kMaxGpidPartitions + partition
/// MUST be a power of two.  Value 1024 is part of the routing protocol.
inline constexpr int32_t kMaxGpidPartitions = 1024;

/// Compute the global partition id for a (topicId, partition) pair.
///
/// Preconditions (checked by assert in debug builds):
///   topicId  >= 0
///   partition >= 0 && partition < kMaxGpidPartitions
///
/// Hot-path safe: no allocation, constexpr-friendly, inlined.
[[nodiscard]]
inline constexpr int32_t gpidOf(int32_t topicId, int32_t partition) noexcept
{
    return topicId * kMaxGpidPartitions + partition;
}

} // namespace rune::metadata
