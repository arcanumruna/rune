#pragma once
// WorkerWire.h — flat byte codec for WorkerOp and WorkerReturn over a
// OneToOneByteRing (cross-thread-ring refactor, R1).
//
// DESIGN
// ======
// Each ring message is a contiguous byte frame:
//
//   [WorkerOpWire header][optional tail bytes]
//
// WorkerOpWire is a trivially-copyable POD that holds EVERY scalar field of
// WorkerOp.  In-process pointers (replica*, logPtr) are carried as uintptr_t
// (same address space — safe for cross-thread but NOT cross-process).
//
// The ONE variable field per op type is serialised as a flat byte tail:
//   Produce          → recordBatchBytes  (raw bytes)
//   ApplyReplicated  → replicatedPayload.records (raw bytes)
//   BecomeLeader     → isr as NodeId[] (int32 array, little-endian native)
//   SetOwnership     → ownedSet as uintptr_t[] (pointer array)
//   PushRead / BecomeFollower → no tail (tailLen = 0, tailKind = None)
//
// WorkerReturnWire is symmetric:
//   PushReadResult → chunk (raw bytes) as tail
//   ProduceResult  → no tail
//
// Encode: caller supplies a scratch uint8_t[] of sufficient capacity.
//         encodeWorkerOp() returns total byte count (header + tail).
//         Scratch must be at least sizeof(WorkerOpWire) + tailLen bytes.
//
// Decode: zero-copy — decodeWorkerOpHeader() returns a pointer into the frame.
//         workerOpTail() returns pointer to the first tail byte (frame + sizeof header).
//
// msgTypeId for the ring = static_cast<int32_t>(op.type) + 1
// (agrona requires typeId >= 1; 0 is reserved for padding).
//
// LAYOUT NOTE: #pragma pack(push,1) is used to guarantee a deterministic
// in-memory layout with no padding gaps.  Both structs are in-process only
// (not persisted to disk or sent over a network) so endianness is native.

#include <rune/node/WorkerOp.h>

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <cassert>
#include <stdexcept>

namespace rune::node {

// ---------------------------------------------------------------------------
// TailKind — which variable field is in the tail (one per op type).
// ---------------------------------------------------------------------------
enum class TailKind : uint8_t {
    None              = 0,   ///< no tail (BecomeFollower, PushRead, ProduceResult)
    RecordBatch       = 1,   ///< Produce: recordBatchBytes
    ReplicatedRecords = 2,   ///< ApplyReplicated: replicatedPayload.records
    IsrList           = 3,   ///< BecomeLeader: isr as NodeId[]
    OwnedSet          = 4,   ///< SetOwnership: ownedSet as uintptr_t[]
    Chunk             = 5,   ///< PushReadResult: chunk
};

// ---------------------------------------------------------------------------
// WorkerOpWire — packed POD for all WorkerOp scalar fields.
// ---------------------------------------------------------------------------
#pragma pack(push, 1)
struct WorkerOpWire {
    // --- common fields ---
    uint8_t  type;          ///< WorkerOp::Type cast to uint8_t
    int32_t  topicId;
    int32_t  partition;
    uintptr_t replica;      ///< reinterpret_cast<uintptr_t>(op.replica)
    uintptr_t logPtr;       ///< reinterpret_cast<uintptr_t>(op.logPtr)

    // --- Produce fields ---
    int64_t  sessionId;
    int64_t  correlationId;
    int16_t  acks;
    int64_t  producerId;
    int16_t  producerEpoch;
    int32_t  baseSequence;
    int32_t  lastOffsetDelta;
    int32_t  recordCount;
    uint16_t batchAttrs;
    uint8_t  deferAck;      ///< bool as uint8_t
    uint8_t  durabilityTier;
    int64_t  enqueuedAtNs;

    // --- PushRead fields ---
    int64_t  subscriptionId;
    int64_t  nextOffset;
    int64_t  hwBound;
    uint8_t  isolation;
    uint8_t  isGroup;              ///< bool as uint8_t
    uint8_t  groupCarrierReceived; ///< bool as uint8_t

    // --- BecomeLeader / BecomeFollower fields ---
    int64_t  roleEpoch;      ///< rune::replication::Epoch = int64_t
    int32_t  roleLeaderId;   ///< rune::replication::NodeId = int32_t

    // --- ApplyReplicated (ReplicatedPayload scalars) ---
    // Also reused by ReconcileFetchResp (same FetchResponseMsg fields).
    int32_t  replLeaderEpoch;
    int32_t  replPartition;
    int64_t  replHighWatermark;
    int64_t  replLogStartOffset;
    int16_t  replErrorCode;
    int32_t  replTopicId;

    // --- E5.1.2c: FetchRequest (leader-side) fields ---
    int64_t  frLeaderEpoch;   ///< FetchRequestPayload::leaderEpoch
    int32_t  frFollowerId;    ///< FetchRequestPayload::followerId
    int64_t  frFetchOffset;   ///< FetchRequestPayload::fetchOffset
    int32_t  frMaxBytes;      ///< FetchRequestPayload::maxBytes
    int64_t  frNowNs;         ///< FetchRequestPayload::nowNs

    // --- E5.1.2c: OffsetsForLeaderEpochReq fields ---
    int64_t  oleCurrentEpoch;   ///< OffsetsForLeaderEpochReqPayload::currentEpoch
    int32_t  oleFollowerId;     ///< OffsetsForLeaderEpochReqPayload::followerId
    int64_t  oleRequestedEpoch; ///< OffsetsForLeaderEpochReqPayload::requestedEpoch
    int64_t  oleFollowerLeo;    ///< OffsetsForLeaderEpochReqPayload::followerLeo

    // --- variable tail descriptor ---
    uint8_t  tailKind;  ///< TailKind cast to uint8_t
    uint32_t tailLen;   ///< byte length of the tail following this header
};
#pragma pack(pop)

static_assert(std::is_trivially_copyable_v<WorkerOpWire>,
              "WorkerOpWire must be trivially copyable for memcpy ring use");

// ---------------------------------------------------------------------------
// WorkerReturnWire — packed POD for all WorkerReturn scalar fields.
// ---------------------------------------------------------------------------
#pragma pack(push, 1)
struct WorkerReturnWire {
    // --- common fields ---
    uint8_t  type;          ///< WorkerReturn::Type cast to uint8_t

    // --- ProduceResult fields ---
    int64_t  sessionId;
    int64_t  correlationId;
    int32_t  partition;
    int64_t  baseOffset;
    int64_t  lastOffset;
    int16_t  errorCode;
    uint8_t  deferAck;      ///< bool as uint8_t
    uintptr_t replica;      ///< reinterpret_cast<uintptr_t>(ret.replica)
    int64_t  producerId;
    int16_t  producerEpoch;
    int32_t  baseSequence;
    int32_t  recordCount;
    int32_t  topicId;
    uint16_t batchAttrs;
    int64_t  enqueuedAtNs;

    // --- PushReadResult fields ---
    int64_t  subscriptionId;
    int32_t  readTopicId;
    int32_t  readPartition;
    int64_t  newNextOffset;
    int64_t  hwBound;
    uint8_t  isolation;
    uint8_t  isGroup;              ///< bool as uint8_t
    uint8_t  groupCarrierReceived; ///< bool as uint8_t

    // --- variable tail descriptor ---
    uint8_t  tailKind;  ///< TailKind cast to uint8_t
    uint32_t tailLen;   ///< byte length of the tail following this header
};
#pragma pack(pop)

static_assert(std::is_trivially_copyable_v<WorkerReturnWire>,
              "WorkerReturnWire must be trivially copyable for memcpy ring use");

// ---------------------------------------------------------------------------
// msgTypeId helpers
// ---------------------------------------------------------------------------

/// Ring msgTypeId for a WorkerOp (agrona requires >= 1).
inline int32_t workerOpMsgTypeId(const WorkerOp& op) {
    return static_cast<int32_t>(op.type) + 1;
}

/// Ring msgTypeId for a WorkerReturn (agrona requires >= 1).
inline int32_t workerReturnMsgTypeId(const WorkerReturn& ret) {
    return static_cast<int32_t>(ret.type) + 1;
}

// ---------------------------------------------------------------------------
// encodedWorkerOpSize — total encoded byte size (header + variable tail) of an
// op, WITHOUT encoding.  Lets a producer tryClaim() the exact ring slot and then
// encode in place (zero-copy).  MUST match encodeWorkerOp's tail selection below.
// ---------------------------------------------------------------------------
inline std::size_t encodedWorkerOpSize(const WorkerOp& op) {
    std::size_t tail = 0;
    switch (op.type) {
    case WorkerOp::Type::Produce:
    case WorkerOp::Type::AppendControlRecord:
        tail = op.recordBatchBytes.size();
        break;
    case WorkerOp::Type::ApplyReplicated:
    case WorkerOp::Type::ReconcileFetchResp:
        tail = op.replicatedPayload.records.size();
        break;
    case WorkerOp::Type::BecomeLeader:
        tail = op.isr.size() * sizeof(rune::replication::NodeId);
        break;
    case WorkerOp::Type::SetOwnership:
        tail = op.ownedSet.size() * sizeof(uintptr_t);
        break;
    case WorkerOp::Type::PushRead:
    case WorkerOp::Type::BecomeFollower:
    case WorkerOp::Type::FetchRequest:
    case WorkerOp::Type::OffsetsForLeaderEpochReq:
    case WorkerOp::Type::Subscribe:
    case WorkerOp::Type::Unsubscribe:
        tail = 0;
        break;
    }
    return sizeof(WorkerOpWire) + tail;
}

/// encodedWorkerReturnSize — total encoded size of a return (header + optional
/// chunk tail), WITHOUT encoding.  MUST match encodeWorkerReturn's tail selection.
inline std::size_t encodedWorkerReturnSize(const WorkerReturn& ret) {
    const std::size_t tail =
        (ret.type == WorkerReturn::Type::PushReadResult && !ret.chunk.empty())
            ? ret.chunk.size() : 0;
    return sizeof(WorkerReturnWire) + tail;
}

// ---------------------------------------------------------------------------
// encodeWorkerOp — fill scratch with [WorkerOpWire][tail] and return total size.
//
// scratch must be at least sizeof(WorkerOpWire) + tailBytes large.
// Throws std::length_error if scratchCap is insufficient.
// ---------------------------------------------------------------------------
inline std::size_t encodeWorkerOp(const WorkerOp& op,
                                  uint8_t*        scratch,
                                  std::size_t     scratchCap)
{
    WorkerOpWire hdr{};
    hdr.type             = static_cast<uint8_t>(op.type);
    hdr.topicId          = op.topicId;
    hdr.partition        = op.partition;
    hdr.replica          = reinterpret_cast<uintptr_t>(op.replica);
    hdr.logPtr           = reinterpret_cast<uintptr_t>(op.logPtr);
    hdr.sessionId        = op.sessionId;
    hdr.correlationId    = op.correlationId;
    hdr.acks             = op.acks;
    hdr.producerId       = op.producerId;
    hdr.producerEpoch    = op.producerEpoch;
    hdr.baseSequence     = op.baseSequence;
    hdr.lastOffsetDelta  = op.lastOffsetDelta;
    hdr.recordCount      = op.recordCount;
    hdr.batchAttrs       = op.batchAttrs;
    hdr.deferAck         = op.deferAck ? uint8_t{1} : uint8_t{0};
    hdr.durabilityTier   = op.durabilityTier;
    hdr.enqueuedAtNs     = op.enqueuedAtNs;
    hdr.subscriptionId   = op.subscriptionId;
    hdr.nextOffset       = op.nextOffset;
    hdr.hwBound          = op.hwBound;
    hdr.isolation        = op.isolation;
    hdr.isGroup          = op.isGroup          ? uint8_t{1} : uint8_t{0};
    hdr.groupCarrierReceived = op.groupCarrierReceived ? uint8_t{1} : uint8_t{0};
    hdr.roleEpoch        = static_cast<int64_t>(op.roleEpoch);
    hdr.roleLeaderId     = static_cast<int32_t>(op.roleLeaderId);
    hdr.replLeaderEpoch  = op.replicatedPayload.leaderEpoch;
    hdr.replPartition    = op.replicatedPayload.partition;
    hdr.replHighWatermark  = op.replicatedPayload.highWatermark;
    hdr.replLogStartOffset = op.replicatedPayload.logStartOffset;
    hdr.replErrorCode    = op.replicatedPayload.errorCode;
    hdr.replTopicId      = op.replicatedPayload.topicId;

    // E5.1.2c: FetchRequest leader-side payload
    hdr.frLeaderEpoch  = static_cast<int64_t>(op.fetchRequestPayload.leaderEpoch);
    hdr.frFollowerId   = static_cast<int32_t>(op.fetchRequestPayload.followerId);
    hdr.frFetchOffset  = static_cast<int64_t>(op.fetchRequestPayload.fetchOffset);
    hdr.frMaxBytes     = op.fetchRequestPayload.maxBytes;
    hdr.frNowNs        = op.fetchRequestPayload.nowNs;

    // E5.1.2c: OffsetsForLeaderEpochReq payload
    hdr.oleCurrentEpoch   = static_cast<int64_t>(op.offsEpochReqPayload.currentEpoch);
    hdr.oleFollowerId     = static_cast<int32_t>(op.offsEpochReqPayload.followerId);
    hdr.oleRequestedEpoch = static_cast<int64_t>(op.offsEpochReqPayload.requestedEpoch);
    hdr.oleFollowerLeo    = static_cast<int64_t>(op.offsEpochReqPayload.followerLeo);

    // Determine tail
    const uint8_t* tailSrc  = nullptr;
    std::size_t    tailBytes = 0;
    TailKind       kind      = TailKind::None;

    switch (op.type) {
    case WorkerOp::Type::Produce:
        kind      = TailKind::RecordBatch;
        tailSrc   = op.recordBatchBytes.data();
        tailBytes = op.recordBatchBytes.size();
        break;
    case WorkerOp::Type::ApplyReplicated:
    case WorkerOp::Type::ReconcileFetchResp:
        // Both use replicatedPayload.records as the tail.
        kind      = TailKind::ReplicatedRecords;
        tailSrc   = op.replicatedPayload.records.data();
        tailBytes = op.replicatedPayload.records.size();
        break;
    case WorkerOp::Type::BecomeLeader:
        kind      = TailKind::IsrList;
        // isr is vector<NodeId> (int32_t[]); tail = raw bytes of that array
        tailSrc   = reinterpret_cast<const uint8_t*>(op.isr.data());
        tailBytes = op.isr.size() * sizeof(rune::replication::NodeId);
        break;
    case WorkerOp::Type::SetOwnership:
        kind      = TailKind::OwnedSet;
        // ownedSet as uintptr_t[] — encode pointer values
        tailBytes = op.ownedSet.size() * sizeof(uintptr_t);
        break;
    case WorkerOp::Type::PushRead:
    case WorkerOp::Type::BecomeFollower:
    case WorkerOp::Type::FetchRequest:
    case WorkerOp::Type::OffsetsForLeaderEpochReq:
    // read-rework T3/T4: Subscribe/Unsubscribe reuse the scalar wire fields
    // (subscriptionId/sessionId/topicId/partition/nextOffset/isolation/isGroup/
    // groupCarrierReceived/logPtr/replica) — no variable tail.
    case WorkerOp::Type::Subscribe:
    case WorkerOp::Type::Unsubscribe:
        kind      = TailKind::None;
        tailBytes = 0;
        break;
    // Task #13: AppendControlRecord — marker bytes carried as RecordBatch tail
    // (reuses the same TailKind as Produce — raw bytes, no extra framing).
    case WorkerOp::Type::AppendControlRecord:
        kind      = TailKind::RecordBatch;
        tailSrc   = op.recordBatchBytes.data();
        tailBytes = op.recordBatchBytes.size();
        break;
    }

    hdr.tailKind = static_cast<uint8_t>(kind);
    hdr.tailLen  = static_cast<uint32_t>(tailBytes);

    const std::size_t total = sizeof(WorkerOpWire) + tailBytes;
    if (total > scratchCap) {
        throw std::length_error("encodeWorkerOp: scratch buffer too small");
    }

    std::memcpy(scratch, &hdr, sizeof(WorkerOpWire));

    if (tailBytes > 0) {
        if (op.type == WorkerOp::Type::SetOwnership) {
            // Encode each pointer as uintptr_t
            uint8_t* dst = scratch + sizeof(WorkerOpWire);
            for (auto* p : op.ownedSet) {
                uintptr_t pval = reinterpret_cast<uintptr_t>(p);
                std::memcpy(dst, &pval, sizeof(uintptr_t));
                dst += sizeof(uintptr_t);
            }
        } else {
            std::memcpy(scratch + sizeof(WorkerOpWire), tailSrc, tailBytes);
        }
    }

    return total;
}

// ---------------------------------------------------------------------------
// Decode helpers for WorkerOp — zero-copy views into a ring frame.
// ---------------------------------------------------------------------------

/// Return a typed pointer to the WorkerOpWire header at the start of `frame`.
/// frame must point to the first byte of the encoded message.
inline const WorkerOpWire* decodeWorkerOpHeader(const uint8_t* frame) {
    return reinterpret_cast<const WorkerOpWire*>(frame);
}

/// Return a pointer to the first byte of the variable tail in `frame`.
/// Returns nullptr if tailLen == 0.
inline const uint8_t* workerOpTail(const uint8_t* frame) {
    const auto* hdr = decodeWorkerOpHeader(frame);
    if (hdr->tailLen == 0) return nullptr;
    return frame + sizeof(WorkerOpWire);
}

// ---------------------------------------------------------------------------
// encodeWorkerReturn — fill scratch with [WorkerReturnWire][tail] and return total size.
// ---------------------------------------------------------------------------
inline std::size_t encodeWorkerReturn(const WorkerReturn& ret,
                                      uint8_t*            scratch,
                                      std::size_t         scratchCap)
{
    WorkerReturnWire hdr{};
    hdr.type                 = static_cast<uint8_t>(ret.type);
    hdr.sessionId            = ret.sessionId;
    hdr.correlationId        = ret.correlationId;
    hdr.partition            = ret.partition;
    hdr.baseOffset           = ret.baseOffset;
    hdr.lastOffset           = ret.lastOffset;
    hdr.errorCode            = ret.errorCode;
    hdr.deferAck             = ret.deferAck ? uint8_t{1} : uint8_t{0};
    hdr.replica              = reinterpret_cast<uintptr_t>(ret.replica);
    hdr.producerId           = ret.producerId;
    hdr.producerEpoch        = ret.producerEpoch;
    hdr.baseSequence         = ret.baseSequence;
    hdr.recordCount          = ret.recordCount;
    hdr.topicId              = ret.topicId;
    hdr.batchAttrs           = ret.batchAttrs;
    hdr.enqueuedAtNs         = ret.enqueuedAtNs;
    hdr.subscriptionId       = ret.subscriptionId;
    hdr.readTopicId          = ret.readTopicId;
    hdr.readPartition        = ret.readPartition;
    hdr.newNextOffset        = ret.newNextOffset;
    hdr.hwBound              = ret.hwBound;
    hdr.isolation            = ret.isolation;
    hdr.isGroup              = ret.isGroup              ? uint8_t{1} : uint8_t{0};
    hdr.groupCarrierReceived = ret.groupCarrierReceived ? uint8_t{1} : uint8_t{0};

    // Tail: only PushReadResult carries a chunk
    TailKind      kind      = TailKind::None;
    const uint8_t* tailSrc  = nullptr;
    std::size_t    tailBytes = 0;

    if (ret.type == WorkerReturn::Type::PushReadResult && !ret.chunk.empty()) {
        kind      = TailKind::Chunk;
        tailSrc   = ret.chunk.data();
        tailBytes = ret.chunk.size();
    }

    hdr.tailKind = static_cast<uint8_t>(kind);
    hdr.tailLen  = static_cast<uint32_t>(tailBytes);

    const std::size_t total = sizeof(WorkerReturnWire) + tailBytes;
    if (total > scratchCap) {
        throw std::length_error("encodeWorkerReturn: scratch buffer too small");
    }

    std::memcpy(scratch, &hdr, sizeof(WorkerReturnWire));
    if (tailBytes > 0) {
        std::memcpy(scratch + sizeof(WorkerReturnWire), tailSrc, tailBytes);
    }

    return total;
}

// ---------------------------------------------------------------------------
// Decode helpers for WorkerReturn — zero-copy views into a ring frame.
// ---------------------------------------------------------------------------

/// Return a typed pointer to the WorkerReturnWire header at the start of `frame`.
inline const WorkerReturnWire* decodeWorkerReturnHeader(const uint8_t* frame) {
    return reinterpret_cast<const WorkerReturnWire*>(frame);
}

/// Return a pointer to the first byte of the variable tail in `frame`.
/// Returns nullptr if tailLen == 0.
inline const uint8_t* workerReturnTail(const uint8_t* frame) {
    const auto* hdr = decodeWorkerReturnHeader(frame);
    if (hdr->tailLen == 0) return nullptr;
    return frame + sizeof(WorkerReturnWire);
}

} // namespace rune::node
