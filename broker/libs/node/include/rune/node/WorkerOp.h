#pragma once
// WorkerOp.h — E2 inc2a: op-envelope types for the per-worker SPSC ingress ring.
//
// DESIGN
// ======
// WorkerOp carries exactly the data needed to EXECUTE one log operation on the
// owning worker AND to respond to the originator (gateway / control thread).
//
// Five op types:
//   Produce         — gateway → worker; execute appendNoSync + onLeaderLogAppended
//                     on the owning partition's Log+Replica; return ProduceReturn.
//   PushRead        — gateway → worker; execute Log::read() for a subscription
//                     push; return PushReadReturn with the data chunk.
//   ApplyReplicated — transport → worker; execute PartitionReplica::applyReplicated;
//                     no return (fire-and-forget — the follower has no response to
//                     send back to the gateway).
//   BecomeLeader    — bridge → worker; execute PartitionReplica::becomeLeader on
//                     the owning worker thread (shared-nothing E2 role-change fix).
//   BecomeFollower  — bridge → worker; execute PartitionReplica::becomeFollower on
//                     the owning worker thread (shared-nothing E2 role-change fix).
//
// Buffer lifetime (correctness-first):
//   Aeron fragment buffers (FetchResponseMsg::records span) are ONLY valid inside
//   the poll callback.  We COPY the relevant bytes into a std::vector<uint8_t>
//   owned by the WorkerOp at enqueue time.  This is the "copy into a worker-owned
//   buffer" approach mandated by the brief.  A pooled allocator can replace the
//   vector later (inc2b+).
//
// WorkerReturn carries the result back to the gateway.  Two subtypes:
//   ProduceReturn   — contains the info needed to send a ProduceResponse (or
//                     enqueue a pending ack).
//   PushReadReturn  — contains the data chunk + subscription context for
//                     pushSubscriptions to encode + send Records.
//
// All fields are plain data (no pointers except non-owning replica*) or owned
// vectors.  Move-semantics for the vectors ensure no copies on the hot path.

#include <rune/replication/ReplicationMessages.h>  // FetchResponseMsg
#include <rune/storage/Log.h>

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {

// ---------------------------------------------------------------------------
// WorkerOp — ingress op sent from the control thread to a Worker.
// ---------------------------------------------------------------------------
struct WorkerOp {
    enum class Type : uint8_t {
        Produce,          ///< gateway produce-append
        PushRead,         ///< gateway pushSubscriptions log-read
        ApplyReplicated,  ///< transport applyReplicated
        BecomeLeader,     ///< bridge → worker: promote replica to leader
        BecomeFollower,   ///< bridge → worker: demote replica to follower
    };

    Type    type    = Type::Produce;
    int32_t topicId = 0;
    int32_t partition = 0;

    // -----------------------------------------------------------------------
    // Non-owning pointer to the PartitionReplica for this (topicId, partition).
    // Null if no replica wired (passthrough / single-node).
    // Lifetime invariant: the replica registry outlives the ring.
    // -----------------------------------------------------------------------
    rune::replication::PartitionReplica* replica = nullptr;

    // -----------------------------------------------------------------------
    // Non-owning Log* — set by the gateway at enqueue time from the
    // TopicRegistry lookup that already happened inline.  Avoids a registry
    // lookup inside the worker's hot path.  Lifetime: TopicRegistry owns the
    // Log; TopicRegistry outlives the ring (owned by BrokerNode).
    // -----------------------------------------------------------------------
    rune::storage::Log* logPtr = nullptr;

    // -----------------------------------------------------------------------
    // Produce-specific fields
    // -----------------------------------------------------------------------

    // Session + correlation identity so the gateway can send the response.
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    // Produce flags / producer-state fields
    int16_t acks          = 0;    ///< 0=none, 1=all (same as msg.acks)
    int64_t producerId    = -1;   ///< -1 = legacy (no idempotent tracking)
    int16_t producerEpoch = 0;
    int32_t baseSequence  = 0;

    // The restamped RecordBatch bytes (copied from restampScratch_ at enqueue).
    // Sized to the exact batch length.  Worker owns this buffer.
    std::vector<uint8_t> recordBatchBytes;

    // lastOffsetDelta from the RecordBatch header (kLastOffsetDeltaOffset=23, LE32).
    // The worker computes lastOffset = worker_base + lastOffsetDelta.
    int32_t lastOffsetDelta = 0;

    // recordCount = lastOffsetDelta + 1 (validated by gateway before enqueue).
    int32_t recordCount = 0;

    // Batch attributes (LE16 at kAttributesOffset=21) captured from the batch
    // before enqueueing.  Used by the gateway's drainWorkerReturns_ to feed
    // the LsoTracker (TRANSACTIONAL bit = 0x10) after a successful append.
    // Forwarded in WorkerReturn.batchAttrs.
    uint16_t batchAttrs = 0;

    // deferAck: true when the gateway determined (before enqueueing) that the
    // response should be deferred (acks=All + replica + committedMetadataReady).
    // Forwarded in WorkerReturn so the gateway knows to enqueue PendingAck.
    bool deferAck = false;

    // enqueuedAtNs: timestamp at enqueue time (for pending-ack timeout).
    int64_t enqueuedAtNs = 0;

    // -----------------------------------------------------------------------
    // PushRead-specific fields
    // -----------------------------------------------------------------------

    // Subscription identity (subscriptionId, sessionId, topicId, partition
    // are already in the common fields above; nextOffset lives here).
    int64_t subscriptionId = 0;
    int64_t nextOffset     = 0;      ///< starting offset for log->read()
    int64_t hwBound        = 0;      ///< HW-bound for this read cycle
    uint8_t isolation      = 0;      ///< 0=read_uncommitted, 1=read_committed
    bool    isGroup        = false;
    bool    groupCarrierReceived = false;

    // -----------------------------------------------------------------------
    // BecomeLeader / BecomeFollower-specific fields
    //
    // Bridge enqueues these when routing replica role changes to the owning
    // worker (Part 1 of the E2 shared-nothing fix).
    //
    // BecomeLeader: roleEpoch + isr carries the per-partition epoch and ISR.
    // BecomeFollower: roleEpoch + roleLeaderId carries the new leader.
    // -----------------------------------------------------------------------

    // Epoch for BecomeLeader / BecomeFollower (per-partition or cluster epoch).
    rune::replication::Epoch roleEpoch = 0;

    // ISR for BecomeLeader (copied by value — bridge owns nothing beyond enqueue).
    std::vector<rune::replication::NodeId> isr;

    // Leader node for BecomeFollower (-1 = no leader / NO_LEADER).
    rune::replication::NodeId roleLeaderId = -1;

    // -----------------------------------------------------------------------
    // ApplyReplicated-specific fields
    // -----------------------------------------------------------------------

    // The FetchResponse payload, fully copied from the Aeron term buffer.
    // Includes all fields needed to reconstruct a FetchResponseMsg.
    struct ReplicatedPayload {
        int32_t  leaderEpoch    = 0;
        int32_t  partition      = 0;
        int64_t  highWatermark  = 0;
        int64_t  logStartOffset = 0;
        int16_t  errorCode      = 0;
        int32_t  topicId        = 0;
        std::vector<uint8_t> records;   ///< copy of the records span
    } replicatedPayload;
};

// ---------------------------------------------------------------------------
// WorkerReturn — result sent from Worker back to the control thread.
// ---------------------------------------------------------------------------
struct WorkerReturn {
    enum class Type : uint8_t {
        ProduceResult,   ///< result of a Produce op
        PushReadResult,  ///< result of a PushRead op
    };

    Type    type = Type::ProduceResult;

    // -----------------------------------------------------------------------
    // ProduceResult fields (gateway needs these to send ProduceResponse or
    // enqueue a PendingAck).
    // -----------------------------------------------------------------------
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int32_t partition     = 0;
    int64_t baseOffset    = 0;
    int64_t lastOffset    = 0;

    // Result code: 0 = success, non-zero = error (mirrors pc::ErrorCode).
    int16_t errorCode     = 0;

    // Whether the gateway should enqueue a deferred ack (acks=All) instead of
    // sending immediately.  True only when the produce was appended successfully
    // AND acks=All AND replica is set AND committedMetadataReady is true.
    bool    deferAck      = false;

    // Non-owning pointer to the replica (for the pending-ack path).
    rune::replication::PartitionReplica* replica = nullptr;

    // Producer-state update fields (forwarded from WorkerOp for PSM update).
    // Gateway calls producerStateManager_.update() after receiving the result.
    int64_t producerId    = -1;
    int16_t producerEpoch = 0;
    int32_t baseSequence  = 0;
    int32_t recordCount   = 0;   ///< forwarded from WorkerOp.recordCount
    // topicId forwarded back so the PSM update can key correctly.
    int32_t topicId       = 0;

    // Batch attributes forwarded from WorkerOp for LSO tracking.
    uint16_t batchAttrs   = 0;

    // enqueuedAtNs forwarded from WorkerOp for pending-ack timeout tracking.
    int64_t  enqueuedAtNs = 0;

    // -----------------------------------------------------------------------
    // PushReadResult fields
    // -----------------------------------------------------------------------
    int64_t subscriptionId = 0;
    int32_t readTopicId    = 0;   ///< echoed from WorkerOp for gateway routing
    int32_t readPartition  = 0;   ///< echoed from WorkerOp

    // nextOffset to set on the subscription after successful delivery.
    // The gateway must honour back-pressure: if the send fails, it does NOT
    // advance subscription.nextOffset (same invariant as before the SPSC hop).
    int64_t newNextOffset  = 0;

    // HW-bound echoed back so the gateway can apply the isolation bound check
    // during the encode+send phase.
    int64_t hwBound        = 0;

    // Isolation level (0/1) echoed back for the encode+send phase.
    uint8_t isolation      = 0;
    bool    isGroup        = false;
    bool    groupCarrierReceived = false;

    // The data chunk returned by log->read().  Moved out of the worker.
    std::vector<uint8_t> chunk;
};

} // namespace rune::node
