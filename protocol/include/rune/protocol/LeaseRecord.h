#pragma once
// LeaseRecord.h — lease-topic record attributes and the tombstone / control predicates.
//
// Record attribute bits 2-3 carry the lease op ONLY on lease topics; on every other topic they stay reserved and
// refused (kAttrReservedMask).  Intent bits 0-1 are unchanged (L21): a grant is Put|insert(1), an update is
// Put|update(2), a revoke is Revoke|update(2); records the broker writes itself carry intent 0.
//
// A lease topic's value is the full row: the 4-byte schema-version prefix, then the four u64 system columns
// (version, ttl, holder, remaining), then the user columns.
#include <cstddef>
#include <cstdint>

#include "RecordBatch.h"

namespace rune::protocol::record {

enum class LeaseOp : uint8_t { Put = 0, Revoke = 1, Expire = 2, Checkpoint = 3 };

inline constexpr uint8_t kAttrLeaseOpShift = 2;
inline constexpr uint8_t kAttrLeaseOpMask  = 0x0C;

[[nodiscard]] constexpr LeaseOp leaseOpOf(uint8_t attrs) noexcept {
    return static_cast<LeaseOp>((attrs & kAttrLeaseOpMask) >> kAttrLeaseOpShift);
}

/// attrs with bits 2-3 replaced by `op`; every other bit is untouched.
[[nodiscard]] constexpr uint8_t withLeaseOp(uint8_t attrs, LeaseOp op) noexcept {
    return static_cast<uint8_t>((attrs & ~kAttrLeaseOpMask) | (static_cast<uint8_t>(op) << kAttrLeaseOpShift));
}

// Offsets of the four u64 system columns inside a lease row's value (after the 4-byte schema-version prefix).
inline constexpr std::size_t kLeaseVersionValueOffset   = 4;
inline constexpr std::size_t kLeaseTtlValueOffset       = 12;
inline constexpr std::size_t kLeaseHolderValueOffset    = 20;
inline constexpr std::size_t kLeaseRemainingValueOffset = 28;
inline constexpr std::size_t kLeaseSystemBytes          = 32;

struct TopicTraits {
    bool lease = false;
};

/// The one tombstone predicate.  Lease topic: Revoke or Expire (the value is the full row, its length is irrelevant).
/// Any other topic: an empty value (attrs are ignored).
[[nodiscard]] constexpr bool isTombstone(uint8_t attrs, std::size_t valueLen, TopicTraits t) noexcept {
    if (t.lease) {
        const LeaseOp op = leaseOpOf(attrs);
        return op == LeaseOp::Revoke || op == LeaseOp::Expire;
    }
    return valueLen == 0;
}

/// True only for a Checkpoint op on a lease topic; always false on any other topic.
[[nodiscard]] constexpr bool isLeaseControl(uint8_t attrs, TopicTraits t) noexcept {
    return t.lease && leaseOpOf(attrs) == LeaseOp::Checkpoint;
}

} // namespace rune::protocol::record
