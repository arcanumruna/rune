#pragma once
// LeaseRecord.h — lease-topic record attributes and the tombstone / control predicates.
//
// Record attribute bits 2-3 carry the lease op ONLY on lease topics; on every other topic they stay reserved and
// refused (kAttrReservedMask).  Intent bits 0-1 are unchanged (L21): a grant is Put|insert(1), an update is
// Put|update(2), a revoke is Revoke|update(2); records the broker writes itself carry intent 0:
// a Carry is Put with intent 0 and the fence is Checkpoint with version -1 (transient lease topics, spec A.3/A.6).
//
// A lease topic's value is the full row: the 4-byte schema-version prefix, then the four u64 system columns
// (version, ttl, holder, remaining), then the user columns.
#include <cstddef>
#include <cstdint>
#include <cstring>

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

/// Most records in one lease-topic batch.  The write hook refuses larger lease-topic batches (Task 5); this bound keeps
/// the largest ProduceResponse (verdict + lease trailers) inside one Aeron message (term / 8 = 64 KiB).
inline constexpr std::size_t kMaxLeaseBatchRecords = 2048;

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

/// Spec A.3: a Carry is a broker re-append of a live row (Put, intent 0).  Clients cannot send it (a client upsert on a
/// lease topic is refused).  Always false on a non-lease topic.
[[nodiscard]] constexpr bool isLeaseCarry(uint8_t attrs, TopicTraits t) noexcept {
    return t.lease && leaseOpOf(attrs) == LeaseOp::Put && intentOf(attrs) == 0;
}

/// The lease_version a fence record carries.
inline constexpr int64_t kLeaseFenceVersion = -1;

/// Spec A.6: the partition's fence record (a Checkpoint whose version is -1), never a row.  False on a non-lease topic.
[[nodiscard]] inline bool isLeaseFence(uint8_t attrs, const uint8_t* value, std::size_t valueLen, TopicTraits t) noexcept {
    if (!t.lease || leaseOpOf(attrs) != LeaseOp::Checkpoint || valueLen < kLeaseVersionValueOffset + 8) return false;
    int64_t v = 0;
    std::memcpy(&v, value + kLeaseVersionValueOffset, 8);
    return v == kLeaseFenceVersion;
}

} // namespace rune::protocol::record
