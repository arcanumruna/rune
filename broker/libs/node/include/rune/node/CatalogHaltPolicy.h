#pragma once
// CatalogHaltPolicy.h — catalog history Task 11 fix round 1 (review M1): how
// long a catalog-halted controller holds its candidacy after a stand-down.
//
// The first stand-down holds `baseNs`.  A halted node that leads the consensus
// AGAIN (its earlier stand-down did not produce a lasting healthy leader — e.g.
// a "live" peer that cannot win an election) doubles its hold on each repeat,
// capped at 2^kMaxShift × base: every stand-down is a new consensus term and a
// controller change for the data plane, so a repeat is never immediate and
// never unbounded.  Pure, allocation-free.
#include <algorithm>
#include <cstdint>

namespace rune::node {

inline constexpr unsigned kHaltedStandDownMaxShift = 5;   // up to 32 × base

[[nodiscard]] constexpr int64_t haltedStandDownHoldNs(int64_t baseNs, uint64_t priorStandDowns) noexcept {
    const auto shift = static_cast<unsigned>(std::min<uint64_t>(priorStandDowns, kHaltedStandDownMaxShift));
    return baseNs << shift;
}

/// How long a halted controller waits, after it first sees itself halted while
/// leading, before it decides whether a healthy peer exists: one bad event halts
/// every applier within a replication round-trip, and the peers' halt marks
/// (node-control registers) need about as long to arrive — deciding earlier sees
/// a halting peer as healthy and stands down needlessly (fix round 1, M1/M5).
[[nodiscard]] constexpr int64_t haltedStandDownSettleNs(int64_t leaderHeartbeatTimeoutNs) noexcept {
    return 2 * leaderHeartbeatTimeoutNs;
}

} // namespace rune::node
