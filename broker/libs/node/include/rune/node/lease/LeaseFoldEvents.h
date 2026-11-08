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
    [[nodiscard]] std::size_t reservedBytes() const noexcept { return buf.capacity() * sizeof(LeaseEvent); }
    /// Cold: gives the storage back (the partition stopped leading); reserveOnce() takes it again on the next activation.
    void release() noexcept { std::vector<LeaseEvent>().swap(buf); }
};

/// Per writer version, keyed on the spec's per-install sequence number (SpecVersion::installSeq: a reinstalled version, even one with
/// an identical descriptor, is a new entry): the writer was proven to have every field id in the layout and type-compatible, and
/// `slotOf[k]` is the layout slot of the writer's k-th field.  Built once (cold) when the entry is filled, so projecting a record
/// and re-encoding a row are O(fields), not O(fields^2).  Lives in the lease ViewInstance, so a rebuilt layout starts empty.  A
/// version past the array scans (correct, just not cached).
inline constexpr uint16_t kLeaseCoverCacheVersions = 32;
struct LeaseCoverEntry {
    uint32_t              installSeq = 0;   ///< 0 = empty
    std::vector<uint16_t> slotOf;
};
struct LeaseCoverCache {
    std::array<LeaseCoverEntry, kLeaseCoverCacheVersions> entries{};
    uint64_t fills = 0;   ///< entries built (observability / tests)
    [[nodiscard]] uint64_t coverCacheFillsForTest() const noexcept { return fills; }
};

}  // namespace rune::node
