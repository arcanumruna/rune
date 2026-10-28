#pragma once
// NsRingMessages.h - the namespace slot messages that ride the existing worker rings (namespaces Phase 2 Task 8, D22).
//
// Control thread -> every worker, on the worker ingress ring (the same ring and FIFO as TopicDropped / FilterSchema):
//   NsSlotAssign{slot, nsId, limits}  - put nsId in slot, or refresh the limits of the slot's owner (SetLimits);
//   NsSlotRelease{slot, nsId}         - free the slot (the namespace was erased).
//   NsThrottle{slot, nsId, kind, untilNs}  - Task 14 (D28): the aggregator's throttle deadline of one kind (0 = clear);
//   NsViewMemOver{slot, nsId, on}          - Task 14 (D26): the namespace's views are over its memory limit (or back under).
// NsState is declared here for Task 3 (its transport is added with its consumer).
//
// Wire (no new SBE field): WorkerOp::Type::NsSlotAssign / NsSlotRelease carry slot in `topicId`, nsId in `nextOffset`,
// and (Assign) the 256-byte encoded limits as the DESCRIPTOR_BLOB tail (schemaUpdateBlob).
// NsThrottle: slot in `topicId`, nsId in `nextOffset`, kind in `partition`, untilNs in `hwBound`.
// NsViewMemOver: slot in `topicId`, nsId in `nextOffset`, on in `partition` (0 / 1).
// Cold: once per namespace create / limit change / erase.

#include <rune/node/WorkerOp.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node {

struct NsSlotAssign  { uint16_t slot; uint32_t nsId; rune::protocol::NamespaceLimits limits; };
struct NsSlotRelease { uint16_t slot; uint32_t nsId; };
struct NsState       { uint16_t slot; uint32_t nsId; uint8_t tag; };

/// The rates and gauges a namespace is limited by (D28).  Disk: untilNs = INT64_MAX while the node is over the limit, 0
/// to clear.  A rate's untilNs is a deadline on the injected monotonic clock the receiver compares with.
enum class ThrottleKind : uint8_t { ProduceBytes = 0, FetchBytes = 1, RequestOps = 2, Disk = 3, ProduceRecords = 4,
                                    QueryCycles = 5, kCount = 6 };
inline constexpr std::size_t kThrottleKinds = static_cast<std::size_t>(ThrottleKind::kCount);
struct Throttle      { uint16_t slot; uint32_t nsId; ThrottleKind kind; int64_t untilNs; };
struct ViewMemOver   { uint16_t slot; uint32_t nsId; bool on; };

[[nodiscard]] inline WorkerOp makeNsSlotAssignOp(const NsSlotAssign& m) {
    WorkerOp op;
    op.type      = WorkerOp::Type::NsSlotAssign;
    op.topicId   = static_cast<int32_t>(m.slot);
    op.nextOffset = static_cast<int64_t>(m.nsId);
    op.schemaUpdateBlob.resize(rune::protocol::kNamespaceLimitsBytes);
    rune::protocol::encodeLimits(m.limits, op.schemaUpdateBlob.data());
    return op;
}

[[nodiscard]] inline WorkerOp makeNsSlotReleaseOp(const NsSlotRelease& m) {
    WorkerOp op;
    op.type       = WorkerOp::Type::NsSlotRelease;
    op.topicId    = static_cast<int32_t>(m.slot);
    op.nextOffset = static_cast<int64_t>(m.nsId);
    return op;
}

[[nodiscard]] inline WorkerOp makeThrottleOp(const Throttle& m) {
    WorkerOp op;
    op.type       = WorkerOp::Type::NsThrottle;
    op.topicId    = static_cast<int32_t>(m.slot);
    op.nextOffset = static_cast<int64_t>(m.nsId);
    op.partition  = static_cast<int32_t>(m.kind);
    op.hwBound    = m.untilNs;
    return op;
}

[[nodiscard]] inline WorkerOp makeViewMemOverOp(const ViewMemOver& m) {
    WorkerOp op;
    op.type       = WorkerOp::Type::NsViewMemOver;
    op.topicId    = static_cast<int32_t>(m.slot);
    op.nextOffset = static_cast<int64_t>(m.nsId);
    op.partition  = m.on ? 1 : 0;
    return op;
}

}  // namespace rune::node
