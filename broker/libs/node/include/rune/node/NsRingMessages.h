#pragma once
// NsRingMessages.h - the namespace slot messages that ride the existing worker rings (namespaces Phase 2 Task 8, D22).
//
// Control thread -> every worker, on the worker ingress ring (the same ring and FIFO as TopicDropped / FilterSchema):
//   NsSlotAssign{slot, nsId, limits}  - put nsId in slot, or refresh the limits of the slot's owner (SetLimits);
//   NsSlotRelease{slot, nsId}         - free the slot (the namespace was erased).
// NsState / Throttle are declared here for Tasks 3 / 14 (their transport is added with their consumers).
//
// Wire (no new SBE field): WorkerOp::Type::NsSlotAssign / NsSlotRelease carry slot in `topicId`, nsId in `nextOffset`,
// and (Assign) the 256-byte encoded limits as the DESCRIPTOR_BLOB tail (schemaUpdateBlob).
// Cold: once per namespace create / limit change / erase.

#include <rune/node/WorkerOp.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstdint>
#include <cstring>

namespace rune::node {

struct NsSlotAssign  { uint16_t slot; uint32_t nsId; rune::protocol::NamespaceLimits limits; };
struct NsSlotRelease { uint16_t slot; uint32_t nsId; };
struct NsState       { uint16_t slot; uint32_t nsId; uint8_t tag; };
struct Throttle      { uint16_t slot; uint32_t nsId; uint8_t kind; int64_t untilNs; };

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

}  // namespace rune::node
