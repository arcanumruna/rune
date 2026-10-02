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
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {

// ---------------------------------------------------------------------------
// WorkerOp — ingress op sent from the control thread to a Worker.
// ---------------------------------------------------------------------------
struct WorkerOp {
    enum class Type : uint8_t {
        Produce,                  ///< gateway produce-append
        PushRead,                 ///< gateway pushSubscriptions log-read
        ApplyReplicated,          ///< transport applyReplicated (follower-side)
        BecomeLeader,             ///< bridge → worker: promote replica to leader
        BecomeFollower,           ///< bridge → worker: demote replica to follower
        SetOwnership,             ///< control thread → worker: replace the worker's owned set
        // E5.1.2c: leader-side replication state handlers — routed to the
        // owning worker so leaderState_/isrManager_ is single-owner (worker thread).
        FetchRequest,             ///< transport → worker: leader handles follower FetchRequest
        OffsetsForLeaderEpochReq, ///< transport → worker: leader handles KIP-101 epoch probe
        ReconcileFetchResp,       ///< transport → worker: deficient leader applies survivor data
        // Task #13 / F15 K3: route txn COMMIT/ABORT marker appends to the owning
        // worker under the produce-routing predicate (every mode with a worker
        // pool).  The worker appends via replica->appendControlBatch (F15 N2: a
        // real control batch, role + min-ISR gated) and answers
        // ControlRecordAppended{correlationId = marker token, baseOffset = where
        // it ACTUALLY landed} (Task 10 round 4: never pre-computed) — or
        // ControlRecordRefused when the ownership guard refuses it.
        // Internal partitions (topicId < 0) never use this path.
        AppendControlRecord,      ///< gateway → worker: append txn control marker to user partition
        // read-rework T3/T4: subscription lifecycle routed to the owning worker so
        // the worker owns the per-subscription push cursor (shared-nothing).  The
        // worker's duty cycle (I3) reads + encodes + pushes for its active subs.
        // Fire-and-forget (no WorkerReturn): registration/removal only.
        Subscribe,                ///< gateway → worker: register/update a subscription cursor
        Unsubscribe,              ///< gateway → worker: remove a subscription
        // Task 7: admin → worker schema validator push.
        // Carries topicId (existing scalar) + descriptor blob (tail, TailKind=DESCRIPTOR_BLOB).
        // Worker builds a local rune::codec::Validator from the blob on its own thread
        // (shared-nothing: validators_ is worker-owned, never touched by any other thread).
        SchemaUpdate,             ///< admin → worker: install/update a per-topic Validator
        // Self-contained worker shards (spec §3.4, Task 13) — values 13/14 = the
        // schema's SESSION_ANNOUNCE / SESSION_CLOSE.  Runtime → EVERY worker,
        // control plane (connect / disconnect rate), never ownership-gated.
        SessionAnnounce,          ///< runtime → worker: session reply destination (tail = egress channel)
        SessionClose,             ///< runtime → worker: forget the session + its queued acks
        // Task 10 round 4: follower/peer-side handlers on a USER replica, routed
        // to the owning worker (they touch the replica's log/state).
        OffsetsForLeaderEpochResp = 15, ///< transport → worker: follower applies a KIP-101 answer
        ReconcileFetchReq         = 16, ///< transport → worker: survivor serves a reconcile pull
        // F1: TEST-ONLY owner-thread hook (BrokerNode::onOwnerThreadForTest).
        // control → owning worker: run a test-owned OwnerThreadTestTask against
        // `replica` on the worker's OWN thread.  The opaque task ref travels in the
        // `egressPub` pointer slot (no new wire field, no hot-path encode branch).
        // Never enqueued by production code; the worker's handler is a cold branch.
        // Value 200 matches the schema's TEST_TASK (outside the dense prod range).
        TestTask = 200,           ///< control → worker: run a test task (test-only)
    };

    Type    type    = Type::Produce;
    int32_t topicId = 0;
    int32_t partition = 0;

    // -----------------------------------------------------------------------
    // Non-owning pointer to the PartitionReplica for this (topicId, partition).
    // Null if no replica wired (passthrough / single-node).
    //
    // Task 10 round 4: the pointer is NEVER dereferenced on the control thread.
    // Production routing (WorkerPool::tryEnqueueOp) resolves (topicId,
    // partition) through the ReplicaRegistry at routing time and stamps the
    // pointer + identity (targetReplicaId, incarnation) from that lookup; the
    // worker executes only if it owns that exact incarnation and then uses ITS
    // OWN pointer for the id.  incarnation 0 = unregistered (not gated: unit
    // tests / passthrough); ReplicaRegistry::kStaleIncarnation = the target is
    // gone (always refused through the op's normal response path).
    // -----------------------------------------------------------------------
    rune::replication::PartitionReplica* replica = nullptr;
    uint32_t targetReplicaId = 0;
    uint32_t incarnation     = 0;
    // Produce (Task 10 round 5): the runtime's current min-ISR admission policy
    // (0 = the replica's configured minIsr).  Applied by the OWNING worker.
    int32_t  minIsr          = 0;

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

    // The RecordBatch bytes for a Produce/AppendControlRecord op.  Two ways to
    // supply the batch to tryEnqueueOp (which copies it ONCE into the ring slot):
    //   - recordBatchView (preferred, zero-copy): a READ-ONLY span aliasing the
    //     source batch (e.g. the Aeron produce fragment).  Valid only for the
    //     duration of the synchronous tryEnqueueOp call.  When non-empty it is
    //     used as the tail source and recordBatchBytes is ignored — NO heap copy.
    //   - recordBatchBytes (legacy / owning): a heap vector.  Used only when
    //     recordBatchView is empty.
    std::span<const uint8_t> recordBatchView;
    std::vector<uint8_t> recordBatchBytes;

    // Hybrid egress (Subscribe op): opaque pointer to the per-consumer UNICAST
    // egress ExclusivePublication created + lifetime-owned by the gateway, written
    // only by the owning worker.  Passed through the ring as a uint64 (like
    // replica/logPtr).  nullptr → worker uses the shared MDC pub (broadcast).
    void* egressPub = nullptr;

    // SessionAnnounce (Task 13): the client session's egress stream + channel.
    // egressChannel is a VIEW valid only for the synchronous tryEnqueueOp call
    // (copied once into the ring slot as the tail, TailKind = EGRESS_CHANNEL).
    int32_t          egressStreamId = 0;
    std::string_view egressChannel;

    // SetOwnership: the ReplicaRegistry generation this owned set reflects
    // (acked back as WorkerReturn::OwnershipAck).  Task 10 fix 3.
    uint64_t ownershipGen = 0;

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

    // durabilityTier: controls when the ProduceResult ack is emitted.
    //   0 = Leader  — ack immediately after appendNoSync (today's default).
    //   1 = Majority — ack after HW advance (gateway-side gate, same as today).
    //   2 = Strict  — ack only after the local fsync completion is reaped.
    // Default 0 preserves existing behaviour for all callers that do not set
    // this field (parity with all pre-E3 tests).
    uint8_t durabilityTier = 0;

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

    // -----------------------------------------------------------------------
    // SetOwnership-specific field ([[rune-shared-nothing-law]]): the control
    // thread hands the worker its new owned set by value via the ring; the
    // worker moves it into its own (non-atomic, worker-thread-only) owned_.
    // -----------------------------------------------------------------------
    std::vector<rune::replication::PartitionReplica*> ownedSet;

    // Task 7: SchemaUpdate payload — descriptor blob for a single topic.
    //
    // Lifetime: the blob is COPIED into the ring slot at enqueue time
    // (tryEnqueueOp, via putTail).  The worker reads it zero-copy from the
    // ring view inside executeSchemaUpdateWire_ on ITS OWN thread.
    //
    // topicId is carried in the existing common scalar field (WorkerOp::topicId).
    // The blob is carried as the variable tail (TailKind = DESCRIPTOR_BLOB).
    std::vector<uint8_t> schemaUpdateBlob;

    // -----------------------------------------------------------------------
    // E5.1.2c: FetchRequest-specific fields (leader handles follower fetch).
    // Copied flat from the FetchRequestMsg + timestamp — no pointer into
    // the transient Aeron fragment buffer escapes the poll callback.
    // -----------------------------------------------------------------------
    struct FetchRequestPayload {
        rune::replication::Epoch       leaderEpoch  = 0;
        rune::replication::PartitionId partition    = 0;
        rune::replication::NodeId      followerId   = -1;
        rune::replication::Offset      fetchOffset  = 0;
        int32_t                        maxBytes     = 0;
        int32_t                        topicId      = 0;
        int64_t                        nowNs        = 0;
        /// F27 R1: the follower's connection generation at the transport (0 =
        /// not tracked).  On the SBE ring it rides in `ownershipGen` (a SetOwnership
        /// -only field; no FetchRequest op uses it) — a dedicated rune-protocol
        /// schema field is a follow-up.
        uint32_t                       connGen      = 0;
    } fetchRequestPayload;

    // -----------------------------------------------------------------------
    // E5.1.2c: OffsetsForLeaderEpochReq-specific fields (leader answers KIP-101).
    // Copied flat from the OffsetsForLeaderEpochReqMsg.
    // -----------------------------------------------------------------------
    struct OffsetsForLeaderEpochReqPayload {
        rune::replication::Epoch       currentEpoch   = 0;
        rune::replication::PartitionId partition      = 0;
        rune::replication::NodeId      followerId     = -1;
        rune::replication::Epoch       requestedEpoch = 0;
        rune::replication::Offset      followerLeo    = 0;
        int32_t                        topicId        = 0;
    } offsEpochReqPayload;

    // ReconcileFetchResp reuses replicatedPayload (same FetchResponseMsg fields)
    // + replicatedPayload.records for the variable records tail.
};

// ---------------------------------------------------------------------------
// WorkerReturn — result sent from Worker back to the control thread.
// ---------------------------------------------------------------------------
struct WorkerReturn {
    enum class Type : uint8_t {
        ProduceResult,   ///< result of a Produce op
        PushReadResult,  ///< result of a PushRead op
        /// Task 13 fix round 1 (C2): sessionId's ack publication on worker
        /// `partition` can never deliver (add / find error, CLOSED,
        /// MAX_POSITION_EXCEEDED).  The runtime closes the session everywhere
        /// (the client reconnects).  Rides sbe ReturnType value 2 — RESERVED_2
        /// (PRODUCE_NOTIFY, retired by F15 §7); in-process ring only.
        SessionFailed = 2,
        /// fsyncgate fail-stop (Task 10 fix 1): a log's fsync failed.  topicId /
        /// partition name it, errorCode = errno, chunk = the log directory.  The
        /// runtime logs critical and halts the broker (kExitStorageFailure).
        StorageFailure = 3,   ///< == sbe ReturnType::STORAGE_FAILURE
        /// SetOwnership applied: partition = workerId, lastOffset = ownershipGen.
        OwnershipAck = 4,
        /// AppendControlRecord refused by the ownership guard: topicId /
        /// partition, the marker in chunk (re-routed or surfaced as NotLeader).
        ControlRecordRefused = 5,
        /// AppendControlRecord executed: correlationId = the runtime's marker
        /// token, baseOffset = where the marker ACTUALLY landed (authoritative),
        /// or errorCode != 0 when the append failed.  Task 10 round 4 (P1).
        ControlRecordAppended = 6,
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

    // F15 S2 (review m6): on a ProduceResult, true means "FACTS ONLY, NO
    // ANSWER" — the owning worker's copy of a SUCCESSFUL append (errorCode 0)
    // that it parks the answer of (acks=all: emitted at append; Strict: at fsync
    // completion), so the gateway's PSM / LsoTracker / dirty-log mirrors learn
    // it.  Every answer (OK or error) has deferAck == false; an error result
    // never carries it.  TEMPORARY: deleted with the mirrors by S4 (LSO) and S5
    // (PSM).  (On the op side, WorkerOp::deferAck still means "acks=all".)
    bool    deferAck      = false;

    // Non-owning pointer to the replica.  Task 10 round 4: the runtime never
    // dereferences it — it refers to the replica by (targetReplicaId,
    // incarnation), echoed from the op, and resolves through the registry.
    rune::replication::PartitionReplica* replica = nullptr;
    uint32_t targetReplicaId = 0;
    uint32_t incarnation     = 0;

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

    // -----------------------------------------------------------------------
    // F15 S4 (addendum R-3): a PushReadResult echoes the bound the OWNING
    // worker applied and whether its read_committed state was ready.  The
    // WorkerReturn layout is pinned (ProtocolRegression.WorkerReturnLayoutUnchanged),
    // so two fields that a PushReadResult never otherwise uses carry them:
    //   lastOffset → the effective bound W read up to (exclusive): min(op hwBound,
    //                live HW) for read_uncommitted, min(op hwBound, LSO(live HW))
    //                for read_committed; -1 when nothing was bounded (not ready).
    //   deferAck   → read_committed state of the partition not yet Ready (§2.3).
    // -----------------------------------------------------------------------
    [[nodiscard]] int64_t pushEffectiveBound() const noexcept { return lastOffset; }
    [[nodiscard]] bool    pushRcNotReady()     const noexcept { return deferAck; }
    void setPushEffectiveBound(int64_t b) noexcept { lastOffset = b; }
    void setPushRcNotReady(bool v) noexcept { deferAck = v; }
};

/// F18 (m-1): outcome of enqueueing a WorkerOp.  Full = back-pressure (retry
/// later); TooLarge = the op's tail can never fit one ring frame — NOT retriable
/// (a caller that retries it would livelock); answer it as an error instead.
enum class EnqueueResult : uint8_t { Enqueued, Full, TooLarge };

// ---------------------------------------------------------------------------
// workerOpFrameBytes — the encoded size of one WorkerOp ring frame carrying a
// `tailLen`-byte variable tail (record batch / replicated records): SBE header +
// block + varData.  The ONE formula Worker::tryEnqueueOp claims with, and the one
// the produce intake limit (NodeConfig::produceIntakeLimit, F18) and the follower
// apply guard check a batch against (ingress ring maxMsgLength = capacity / 8).
// ---------------------------------------------------------------------------
[[nodiscard]] inline std::size_t workerOpFrameBytes(std::size_t tailLen) {
    return ::rune::protocol::internal::sbe::MessageHeader::encodedLength() +
           ::rune::protocol::internal::sbe::WorkerOp::computeLength(tailLen);
}

} // namespace rune::node
