#pragma once
// LeaseFoldEvents.h -- lease topics Task 6: what the LeaseIndex fold hands to the lease wheels (Tasks 9-12).
//
// The judge of a lease topic pushes one event per ACCEPTED record, in log order.  The buffer is worker-owned (it lives in
// PartitionWriteState next to the verdict state), reserved once to kLeaseFoldEventCap when the partition becomes a lease
// judge, and never reallocates on the steady path: a judge pass that would overflow it ends early instead (the consumer
// drains it and the next duty cycle resumes).  Only a single batch larger than the whole cap, judged into an empty buffer,
// grows it (counted in VerdictSourceStats::leaseEventOverflows).  Replay and rebuild push nothing and clear the buffer:
// row ids change on a rebuild.
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::codec { struct Descriptor; }

namespace rune::node {

inline constexpr std::size_t kLeaseFoldEventCap = 8192;

enum class LeaseEventKind : uint8_t { Granted, Updated, Ended, Checkpointed };

struct LeaseEvent {
    LeaseEventKind kind = LeaseEventKind::Granted;
    uint8_t        endOp = 0;        ///< Ended: the rune::protocol::record::LeaseOp (Revoke or Expire); else 0
    uint32_t       rid = 0;          ///< the lease row's id in the lease index (for Ended, the erased row's id)
    int64_t        version = 0;      ///< the row's lease_version after the record (for Ended, before the erase)
    int64_t        offset = 0;       ///< the record's log offset
    uint64_t       ttlMs = 0;        ///< lease_ttl_ms of the row
    uint64_t       remainingMs = 0;  ///< lease_remaining_ms of the row after the record
};

struct LeaseFoldEvents {
    std::vector<LeaseEvent> buf;
    void reserveOnce() {
        if (buf.capacity() < kLeaseFoldEventCap) buf.reserve(kLeaseFoldEventCap);
    }
};

/// Per writer version: the descriptor already proven to have every field id in the layout.  Lives in the lease ViewInstance, so a
/// rebuilt layout starts empty.  A version past the array scans (correct, just not cached).
inline constexpr uint16_t kLeaseCoverCacheVersions = 32;
struct LeaseCoverCache {
    std::array<const rune::codec::Descriptor*, kLeaseCoverCacheVersions> proven{};
};

}  // namespace rune::node
