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
#include <rune/node/SessionSignalKind.h>
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
        // Carries topicId (existing scalar) + descriptor blob (tail, TailKind=DESCRIPTOR_BLOB)
        // + version (Task 12: carried in the existing `clientVersion` scalar).
        // Worker builds a local rune::codec::Validator from the blob on its own thread
        // (shared-nothing: validators_ is worker-owned, never touched by any other thread),
        // keyed by (topicId, version) so every writer version has its own validator.
        SchemaUpdate,             ///< admin → worker: install/update a per-topic-version Validator
        // Self-contained worker shards (spec §3.4, Task 13) — values 13/14 = the
        // schema's SESSION_ANNOUNCE / SESSION_CLOSE.  Runtime → EVERY worker,
        // control plane (connect / disconnect rate), never ownership-gated.
        SessionAnnounce,          ///< runtime → worker: session reply destination (tail = egress channel)
        SessionClose,             ///< runtime → worker: forget the session + its queued acks
        // Task 10 round 4: follower/peer-side handlers on a USER replica, routed
        // to the owning worker (they touch the replica's log/state).
        OffsetsForLeaderEpochResp = 15, ///< transport → worker: follower applies a KIP-101 answer
        ReconcileFetchReq         = 16, ///< transport → worker: survivor serves a reconcile pull
        // MEDIUM-2: the controller's AlterIsr answer for a USER partition leader
        // (roleEpoch = leaderEpoch, rolePartitionEpoch, replicatedPayload.errorCode).
        AlterIsrResponse          = 18, ///< transport → worker: PartitionReplica::onAlterIsrResponse
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
    // Egress RELEASE op (retire-on-ack): an Unsubscribe with subscriptionId 0 and
    // egressPub != nullptr — the worker drops every subscription on that pub and
    // returns EgressReleased (token = correlationId); only then does the gateway
    // destroy the pub.
    void* egressPub = nullptr;

    // SessionAnnounce (Task 13): the client session's egress stream + channel.
    // egressChannel is a VIEW valid only for the synchronous tryEnqueueOp call
    // (copied once into the ring slot as the tail, TailKind = EGRESS_CHANNEL).
    int32_t          egressStreamId = 0;
    std::string_view egressChannel;
    // D6: SessionAnnounce — the session's ConnectRequest.clientVersion (a
    // worker acking directly gates UnknownProducerId on it).
    // Task 12: ALSO carries SchemaUpdate's schema version (RegistryEntry::version
    // the installed blob was materialized as of) — set it directly when building
    // a SchemaUpdate op (see schemaUpdateBlob's comment).  SessionAnnounce and
    // SchemaUpdate never share one WorkerOp instance, so the reuse is safe and
    // costs no extra branch on the shared encode path (enqueueOp).
    int32_t          clientVersion = 0;

    // Task 19 (per-worker egress crypto addendum, 2026-10-08): SessionAnnounce
    // key handoff. `sessionEncrypted` is the session's own crypto.encrypted
    // (true even if THIS worker's specific key below failed to derive — see
    // senderKeyValid); the worker must treat an encrypted session it has no
    // valid key for as "park/drop", NEVER plaintext. `senderKeyValid` gates
    // whether senderCipher/senderKeyPrefix/senderKey below are a real,
    // installable key (deriveSenderKey() succeeded on the gateway).
    //
    // senderId is deliberately NOT carried here: a worker derives it locally
    // as workerId_ + 1 (unique per worker; gateway == 0) — see
    // RecordsSealCtx::senderId.
    //
    // WIRE NOTE: carried with ZERO new SBE fields (see
    // SessionAnnounceKeyTail.h's file header for why) — sessionEncrypted
    // rides the wire `isolation` byte (PushRead-only otherwise), senderKeyValid
    // rides the wire `durabilityTier` byte (Produce-only otherwise); the
    // cipher/prefix/key ride as a fixed-size PREFIX of the SessionAnnounce
    // tail (ahead of the egressChannel bytes), encoded/decoded by
    // SessionAnnounceKeyTail.h. A plaintext session's wire bytes (isolation,
    // durabilityTier, tail) are completely untouched — byte-identical to
    // every pre-Task-19 SessionAnnounce.
    bool    sessionEncrypted  = false;
    bool    senderKeyValid    = false;
    uint8_t senderCipher      = 0;      ///< rune::crypto::Cipher value (shared by both directions)
    uint8_t senderKeyPrefix[4] = {};    ///< 4-byte AEAD nonce prefix (the session's b2cPrefix)
    uint8_t senderKey[32]      = {};    ///< deriveSenderKey(b2c, workerId+1) output for THIS worker

    // Task 22 (shard worker decrypts produce / per-worker ingress,
    // 2026-10-08): the session's c2b (client->broker) key + prefix, for the
    // worker's OWN ingress open context (RecordsOpenCtx, RecordsOpen.h).
    // UNLIKE senderKey above, this is NOT derived per worker — ingress has a
    // SINGLE sender (the client), so every worker of the session gets the
    // SAME raw c2b key + prefix the gateway itself opens control frames
    // with (one c2b key, no per-sender split — see the addendum's "Scope").
    // `c2bKeyValid` mirrors `senderKeyValid`'s fail-closed contract: false
    // on the first (parked, no-real-key) announce wave or any upstream
    // derivation failure — the worker must then park/drop that session's
    // produce ingress, never decode it as plaintext. Cipher is shared with
    // the egress key above (senderCipher) — one cipher per session.
    bool    c2bKeyValid       = false;
    uint8_t c2bKeyPrefix[4]   = {};     ///< 4-byte AEAD nonce prefix (the session's c2bPrefix)
    uint8_t c2bKey[32]        = {};     ///< the session's RAW c2b key (same for every worker)

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
    //
    // WIRE NOTE (F15 S5 fix round 2, N-4): the SBE WorkerOp field named
    // `deferAck` (rune-internal-ring.xml) is a FLAG BYTE, not a bool:
    //   bit 0 (kOpFlagDeferAck = 0x01)     — this deferAck (acks=all, steady state)
    //   bit 1 (kOpFlagCrcValidated = 0x02) — crcValidated (below; Produce only)
    // Writer: Worker::enqueueOp only.  Readers MUST mask the bit they need
    // (executeProduceWire_ does); `deferAck() != 0` is wrong.  Rename the
    // schema field to `flags` the next time rune-protocol's schema is touched.
    bool deferAck = false;

    // F15 S5 fix round 1 (review M-3): the producer of this Produce op ran
    // RecordBatchReader::ensureValid() (CRC32C) on EXACTLY these batch bytes
    // before enqueueing them.  Only then may the worker skip its own CRC pass.
    // Set by ClientGateway::handleProduce right after its ensureValid; any
    // other op producer (test rigs, a future forward / retry queue) leaves it
    // false and the worker validates.  Carried over the ring as bit 1 of the
    // wire `deferAck` byte (kOpFlagCrcValidated) — no protocol change.
    bool crcValidated = false;

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

    // BecomeLeader / AlterIsrResponse: the committed assignment's partitionEpoch
    // (AlterIsr compare-and-set, MEDIUM-1); -1 = not controller-managed.
    int32_t rolePartitionEpoch = -1;

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
    // Task 12: the schema version this descriptor blob was materialized as of
    // (RegistryEntry::version — matches the live field set/offsets the blob
    // encodes) rides the existing `clientVersion` scalar field below (set it
    // directly — no dedicated schemaUpdateVersion field, so encoding a
    // SchemaUpdate op costs no extra branch on the shared enqueueOp path).
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
        int32_t                        lastFetchedEpoch = -1;   ///< KIP-320/595 (-1 = none)
        int64_t                        incarnation  = 0;        ///< KIP-903 follower process incarnation
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
// SessionSignalKind (the WorkerReturn::Type::SessionSignal kinds): rune/node/SessionSignalKind.h.

struct WorkerReturn {
    enum class Type : uint8_t {
        ProduceResult,   ///< result of a Produce op
        PushReadResult,  ///< result of a PushRead op
        /// Task 13 (fix rounds 1-2): a per-session fact from worker `partition`
        /// about sessionId; `lastOffset` = SessionSignalKind:
        ///   Failed   — its ack publication can never deliver (add / find error,
        ///              CLOSED, MAX_POSITION_EXCEEDED): the runtime closes the
        ///              session everywhere and tells the client (SessionExpired);
        ///   Held     — the worker holds acks for it (the client reads slower
        ///              than it produces): the runtime pauses that session's
        ///              gateway intake (controlled ABORT) — G-A6 on the
        ///              gateway-routed path;
        ///   Released — no longer held on this worker;
        ///   Lost / Found (D8b) — its ack publication reports NOT_CONNECTED /
        ///              is connected again; acks stay queued meanwhile — the
        ///              runtime reaps a session lost on any worker on the
        ///              normal lease measured from the Lost edge;
        ///   Progress (D8b) — a held session's acks are being claimed (the
        ///              client reads): the runtime refreshes its lease.
        ///              At most once per SessionAckSink::kProgressPassInterval
        ///              passes per session.
        ///   Credit (D8c) — baseOffset gateway-routed acks of the session were
        ///              claimed: the runtime returns that many produce credits
        ///              (<= one per session per worker pass).
        /// Rides sbe ReturnType value 2 — RESERVED_2 (PRODUCE_NOTIFY, retired by
        /// F15 §7; static_assert in WorkerReturnCodec.h); in-process ring only.
        SessionSignal = 2,
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

    // D8c: a ProduceResult ANSWER of a GATEWAY-routed (op-ring) produce — the
    // runtime holds a produce credit for it until the worker's SessionAckSink
    // CLAIMS the ack (SessionSignal Credit).  Worker-local only: NOT encoded
    // (the SBE WorkerReturn layout is pinned); emitProduceAck_ reads it from
    // the struct the answer travels in (parked waiters, the return FIFO).
    bool    gatewayCredit = false;

    // D8d: a ProduceResult ANSWER of a DIRECT (worker-ingress) produce counted
    // against the worker's direct budget (SessionAckSink::admitDirect's result,
    // copied to AckEntry::direct; returned when the ack leaves the sink).
    // Worker-local only: NOT encoded, like gatewayCredit.
    uint8_t directAck     = 0;

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

    // The data chunk (owned).  The worker encodes PushRead chunks straight from
    // the log's read mmap into the return ring (F5) and never fills this for
    // them; it carries the other returns' payloads (StorageFailure dir, refused
    // marker) and the chunk of a copy-mode drain (Worker::drainReturn).
    std::vector<uint8_t> chunk;
    // F5: a ZERO-COPY view of the chunk in the return ring, set by a view-mode
    // drain (Worker::drainReturnView) instead of copying into `chunk`.  Valid
    // ONLY inside that drain's callback — never stored, never encoded.
    std::span<const uint8_t> chunkView;
    /// The chunk bytes, whichever carrier holds them.
    [[nodiscard]] std::span<const uint8_t> chunkBytes() const noexcept {
        return chunkView.data() != nullptr ? chunkView : std::span<const uint8_t>(chunk);
    }

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

/// The wire `deferAck` byte of a WorkerOp is a flag set (F15 S5 fix round 1).
inline constexpr uint8_t kOpFlagDeferAck     = 0x01;   ///< WorkerOp::deferAck (acks=all, steady state)
inline constexpr uint8_t kOpFlagCrcValidated = 0x02;   ///< WorkerOp::crcValidated (Produce only)

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
