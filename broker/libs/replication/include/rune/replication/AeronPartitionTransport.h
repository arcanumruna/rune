#pragma once
// ---------------------------------------------------------------------------
// AeronPartitionTransport.h — PartitionTransport implementation over
// aeron:udp.  GR-1 (replication transport wiring).
//
// Namespace: rune::replication
// C++23.  Single-writer.  NO locks.  NO hot-path alloc.
// NO synchronous blocking on the poll/duty-cycle thread.
//
// ## Design
//
//   Per-peer ExclusivePublications  (one per remote node; async resolve).
//   One ingress Subscription + FragmentAssembler (for all inbound frames).
//
//   Outbound (sendFetchRequest / sendFetchResponse):
//     1. SBE-encode into a pre-allocated scratch buffer (no per-call alloc).
//     2. Look up the destination peer's publication.
//     3. If not yet resolved — register addExclusivePublication on the first
//        call and then poll findExclusivePublication inside poll(); return
//        without blocking (frame is dropped until the pub resolves — the
//        retry is driven by the duty-cycle's periodic doWork/nextFetch loop).
//     4. tryClaim → copy → commit; fall back to offer() on back-pressure /
//        NOT_CONNECTED.  Both paths return a result code (> 0 = sent).
//
//   Inbound (poll):
//     Drain the ingress subscription (bounded fragment limit).
//     Decode each frame header (peekTemplateId) and route:
//       FetchRequest  → look up leader replica via registry → onFetchRequest.
//       FetchResponse → look up follower replica via registry → applyReplicated.
//     Routing registry: the caller supplies an
//       std::function<PartitionReplica*(PartitionId)> at construction.
//     Returns total work count (fragments + dispatches).
//
//   Async pub resolve:
//     Each peer slot carries a PendingPub struct:
//       { int64_t registrationId; std::shared_ptr<ExclusivePublication> pub; }
//     poll() iterates pending registrations and calls findExclusivePublication.
//     No sleep, no spin — just one non-blocking probe per poll() call.
//
// ## Thread model
//   All methods (sendFetchRequest, sendFetchResponse, poll) MUST be called
//   from the single duty-cycle thread.  ExclusivePublication is not
//   thread-safe for offer/tryClaim; the Aeron ClientConductor IS thread-safe
//   for addXxx/findXxx.
//
// ## Zero-alloc invariant
//   encodeScratch_ (4 MiB) is pre-allocated in the constructor.
//   The decoded FetchResponseMsg::records span is a zero-copy view into the
//   fragment buffer (same lifetime as the FragmentAssembler callback scope);
//   applyReplicated() must be called WITHIN that callback — the transport does
//   this synchronously.
// ---------------------------------------------------------------------------

// E2 inc2a: WorkerPool forward-declaration for SPSC ApplyReplicated routing.
// Only the pointer is stored in this header (setWorkerPool takes WorkerPool*).
// The full WorkerPool.h / WorkerOp.h are included in AeronPartitionTransport.cpp
// (compiled under rune_replication_aeron which has RUNE_NODE_INCLUDE).
namespace rune::node { class WorkerPool; class ReplicaRegistry; }

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>

#include <rune/aeronutil/AeronPump.h>
#include <rune/consensus/IngressRing.h>  // agrona ManyToOne ring (outbound marshalling)

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <Subscription.h>
#include <ControlledFragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace rune::replication {

// ---------------------------------------------------------------------------
// ReplicaRegistry — lookup callback type
//
// The transport needs to route inbound frames to the right PartitionReplica.
// Callers register replicas via registerReplica() or supply a lookup function.
// ---------------------------------------------------------------------------
class AeronPartitionTransport final : public PartitionTransport {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param aeron          Shared Aeron client (ClientConductor is thread-safe
    ///                       for addXxx/findXxx; must outlive this object).
    /// \param ingressChannel Aeron channel this node listens on for replication
    ///                       ingress, e.g. "aeron:udp?endpoint=127.0.0.1:40300".
    /// \param ingressStreamId Stream ID for inbound replication frames.
    /// \param selfId          This node's NodeId (stamped on FetchRequests).
    ///
    /// The ingress Subscription is registered synchronously (bounded 5 s wait)
    /// in the constructor — this is the only synchronous resolve; it runs once
    /// at startup, never on the duty-cycle hot path.
    /// \param outboundRingBytes Payload bytes of the outbound ManyToOne ring
    ///        (NodeConfig::replicationOutboundRingBytes). Power of two.
    /// \param pump  Startup resolve pump (spec §3.6): an invoker driver +
    ///        conductor (fused runtime) is pumped by this thread between probes;
    ///        the default pump sleep-polls a threaded driver.
    AeronPartitionTransport(std::shared_ptr<aeron::Aeron> aeron,
                            const std::string&             ingressChannel,
                            int32_t                        ingressStreamId,
                            NodeId                         selfId,
                            int                            outboundRingBytes = 4 * 1024 * 1024,
                            rune::aeronutil::AeronPump     pump = {});

    ~AeronPartitionTransport() override = default;

    // Non-copyable, non-movable (holds shared_ptrs and a FragmentAssembler
    // whose lambda captures `this`).
    AeronPartitionTransport(const AeronPartitionTransport&)            = delete;
    AeronPartitionTransport& operator=(const AeronPartitionTransport&) = delete;

    // -----------------------------------------------------------------------
    // Peer egress registration
    //
    // Call once per remote peer before the first send.  Registers an
    // addExclusivePublication (non-blocking); the pub is resolved lazily
    // inside poll().
    // -----------------------------------------------------------------------

    /// Register a peer's egress channel+streamId.  Does NOT block — just
    /// issues addExclusivePublication and stores the registration id.
    void addPeer(NodeId peerId,
                 const std::string& egressChannel,
                 int32_t            egressStreamId);

    // -----------------------------------------------------------------------
    // Replica registry
    //
    // Map (topicId, partitionId) → PartitionReplica*.  The transport
    // doesn't own replicas; callers keep them alive.
    //
    // DR-BUG-5: keying by partitionId alone caused collisions when multiple
    // topics share the same partition index (e.g. "events"/part0 and
    // "multi"/part0).  The composite (topicId, partitionId) key is unique.
    // -----------------------------------------------------------------------

    /// Register a replica so inbound frames can be routed to it.
    /// Task 10 round 4: ignored once a ReplicaRegistry is wired (the transport
    /// then holds NO replica pointer at all — it resolves at use time).
    void registerReplica(int32_t topicId, PartitionId partitionId, PartitionReplica* replica);
    /// Drop a registration (standalone mode — no registry wired).
    void unregisterReplica(int32_t topicId, PartitionId partitionId);

    /// Task 10 round 4 (Critical 1): resolve every inbound frame's replica
    /// through the ReplicaRegistry at USE time (a removed partition resolves to
    /// nothing: the frame is dropped, nothing freed is ever dereferenced).
    /// Wiring it drops any pointer registered so far.  Control thread only.
    /// \param userPartitions false when USER (topicId >= 0) replicas live on
    ///        per-worker replication planes (E5 Task 5): this legacy transport
    ///        then resolves internal partitions only, exactly as before.
    void setReplicaRegistry(const rune::node::ReplicaRegistry* registry,
                            bool userPartitions = true) noexcept {
        registry_ = registry;
        registryUserPartitions_ = userPartitions;
        replicas_.clear();
    }
    /// Test hook: number of raw replica pointers the transport holds.
    [[nodiscard]] std::size_t heldReplicaPointersForTest() const noexcept { return replicas_.size(); }
    /// Test hook: feed one reassembled replication frame through onFragment.
    void deliverFrameForTest(const uint8_t* frame, std::size_t len, int64_t nowNs);

    // -----------------------------------------------------------------------
    // PartitionTransport interface
    // -----------------------------------------------------------------------

    /// (Follower → Leader) encode and offer a FetchRequest on the leader's pub.
    /// Non-blocking: drops if the pub is not yet resolved or back-pressured.
    void sendFetchRequest(const FetchRequestMsg& req) override;

    /// (Leader → Follower) encode and offer a FetchResponse to a follower.
    /// Non-blocking: drops if the follower's pub is not yet resolved.
    void sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override;

    // DR-BUG-6: OffsetsForLeaderEpoch exchange + deficient-leader reconcile.
    void sendOffsetsForLeaderEpochRequest(
            NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req) override;
    void sendOffsetsForLeaderEpochResponse(
            NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp) override;
    void sendReconcileRequest(
            NodeId toPeer, const FetchRequestMsg& req) override;
    void sendReconcileResponse(
            NodeId toPeer, const FetchResponseMsg& resp) override;

    // -----------------------------------------------------------------------
    // Duty-cycle step
    // -----------------------------------------------------------------------

    /// Drain the ingress subscription (≤ kFragmentLimit fragments), resolve
    /// pending publications, and route decoded frames to registered replicas.
    ///
    /// \param nowNs  Current time (nanoseconds) — passed to onFetchRequest.
    /// \returns Total work count (fragment polls + dispatches).
    int poll(int64_t nowNs);

    // -----------------------------------------------------------------------
    // E2 inc2a: WorkerPool wiring
    // -----------------------------------------------------------------------

    /// Wire in the WorkerPool so inbound FetchResponse frames are routed to the
    /// owning worker's SPSC ingress ring instead of calling applyReplicated()
    /// inline within the Aeron poll callback.
    ///
    /// Must be called BEFORE the first poll() that can receive FetchResponses.
    /// May be null (inline fallback path remains active when unset).
    void setWorkerPool(rune::node::WorkerPool* pool) noexcept {
        workerPool_ = pool;
    }

    // -----------------------------------------------------------------------
    // Accessors (for tests)
    // -----------------------------------------------------------------------

    /// Number of peers whose publication has not yet been resolved.
    [[nodiscard]] int unresolvedPeerCount() const noexcept;

    /// Number of peers registered.
    [[nodiscard]] int peerCount() const noexcept {
        return static_cast<int>(peers_.size());
    }

private:
    // -----------------------------------------------------------------------
    // Internal types
    // -----------------------------------------------------------------------

    struct PeerPub {
        NodeId      peerId         = -1;
        int64_t     registrationId = -1;     ///< from addExclusivePublication
        std::shared_ptr<aeron::ExclusivePublication> pub; ///< null until resolved
        bool        registered     = false;  ///< addExclusivePublication called
    };

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Probe pending publications — called at the start of poll().
    /// One non-blocking findExclusivePublication per unresolved peer.
    void resolvePendingPubs();

    /// Send a raw buffer on a peer's ExclusivePublication.
    /// Returns true if the frame was accepted (> 0 from tryClaim/offer).
    /// Non-blocking; caller bears the drop silently.
    bool sendOnPub(aeron::ExclusivePublication& pub,
                   const uint8_t* buf, std::size_t len);

    /// Look up a peer's PeerPub entry (or nullptr if not found).
    PeerPub* findPeer(NodeId id);

    // -----------------------------------------------------------------------
    // Shared-nothing outbound marshalling ([[rune-shared-nothing-law]]).
    //
    // Sends are invoked from WORKER threads (a replica's doWork emits fetch
    // requests/responses).  Workers must NEVER touch peers_/pubs (control-owned
    // state) — that would be shared mutable state.  Instead each send encodes
    // its wire frame into a per-call STACK buffer, prepends a routing header
    // (targetNodeId; kBroadcast=-1), and writes it to the outbound ring (agrona
    // ManyToOneRingBuffer via IngressRing — MPSC, lock-free).  The CONTROL thread
    // drains the ring in poll() and does the actual peers_/pub offers.
    //
    // Back-pressure: on a full ring, enqueueOutbound() returns false and bumps
    // outboundRingFullCount_ — the send is skipped (NOT silently "succeeded").
    // Replication sends are retriable (follower re-requests / leader re-responds
    // next cycle), so skip-and-retry is the correct policy here.
    // -----------------------------------------------------------------------
    static constexpr int32_t kBroadcast = -1;

    /// Stack fast-path size for a single encoded wire frame.  A frame that fits is
    /// encoded/framed on the stack (no heap); a LARGER frame (e.g. a big replicated
    /// batch) falls back to a heap buffer sized by the generated codec's
    /// encodedSize() — so there is NO fixed cap that can overflow, and the common
    /// 64KB-batch case stays allocation-free.  128 KiB comfortably covers a default
    /// 64KB produce batch + SBE framing on the worker threads' (8 MiB) stacks.
    /// The outbound ring's maxMessageLength (capacity/8) remains the true upper
    /// bound; a frame beyond it fails the ring write as back-pressure (retriable).
    static constexpr std::size_t kMaxWireFrame = 128 * 1024;

    /// Routing-header size prepended to each outbound-ring frame: int32 targetNodeId.
    static constexpr std::size_t kOutboundHeaderSize = sizeof(int32_t);

    /// WORKER-thread side: encode routing header + wire frame → outbound ring.
    /// Returns false on full ring (back-pressure; caller's op is retriable).
    bool enqueueOutbound(int32_t targetNodeId, const uint8_t* wire, std::size_t len);

    /// WORKER-thread side, ZERO-COPY FetchResponse: encode the FetchResponse SBE
    /// message DIRECTLY into the claimed OutboundFrame payload region of the ring
    /// slot — one copy (records: log mmap → ring), skipping the intermediate
    /// `wire` buffer + its memcpy that enqueueOutbound(wire,len) would incur.
    /// \param reconcile  when true, re-stamp the FetchResponse templateId to the
    ///        reconcile id (DR reverse-catch-up) after encoding, matching
    ///        sendReconcileResponse's wire.  Returns false on full ring.
    bool enqueueFetchResponse(int32_t targetNodeId, const FetchResponseMsg& resp,
                              bool reconcile = false);

    /// CONTROL-thread side: drain the outbound ring and do the real peer offers.
    /// Called from poll().  Only this path touches peers_/pubs on send.
    /// Returns the count of ring messages drained (folded into poll()'s work
    /// count so the AgentRunner's IdleStrategy idles only on genuine no-work).
    int drainOutbound();

    // -----------------------------------------------------------------------
    // Fragment callback — called by ControlledFragmentAssembler on each reassembled
    // frame.  Returns a ControlledPollAction so the ingress can apply END-TO-END
    // BACK-PRESSURE: when a target worker's SPSC ingress ring is full we return
    // ABORT, which leaves the fragment UNCONSUMED (position not advanced) so Aeron
    // re-delivers it on the next controlledPoll — the data stays in the publication,
    // flow control backs up to the sender, and nothing is dropped.  Malformed /
    // unroutable frames return CONTINUE (skip + advance).
    // -----------------------------------------------------------------------
    aeron::ControlledPollAction onFragment(aeron::concurrent::AtomicBuffer& buffer,
                                           aeron::util::index_t             offset,
                                           aeron::util::index_t             length,
                                           aeron::Header&                   header);
    /// The frame handler proper (onFragment minus the Aeron plumbing).
    aeron::ControlledPollAction handleFrame_(const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    // Per-tick fragment cap for the control thread's inbound poll (fast-acks from
    // peers) AND outbound drain (pushes/acks to peers).  10 was far too small under
    // the acks=majority fast-ack storm — the HW-advancing fast-ack queued behind the
    // cap at both stages.  256 lets the control thread clear a burst in one tick.
    static constexpr int   kFragmentLimit = 256;
    static constexpr std::size_t kScratchSize = 4 * 1024 * 1024; // 4 MiB

    std::shared_ptr<aeron::Aeron>          aeron_;
    std::shared_ptr<aeron::Subscription>   sub_;
    aeron::ControlledFragmentAssembler     assembler_;

    NodeId                                 selfId_;
    int64_t                                nowNs_ = 0; ///< updated at top of poll()

    // Per-peer egress publications (keyed by NodeId).
    std::unordered_map<NodeId, PeerPub>    peers_;

    // Replica registry: (topicId, partitionId) → PartitionReplica*   [DR-BUG-5]
    struct ReplicaKey {
        int32_t     topicId;
        PartitionId partitionId;
        bool operator==(const ReplicaKey& o) const noexcept {
            return topicId == o.topicId && partitionId == o.partitionId;
        }
    };
    struct ReplicaKeyHash {
        std::size_t operator()(const ReplicaKey& k) const noexcept {
            std::size_t h = static_cast<std::size_t>(k.topicId);
            h ^= (static_cast<std::size_t>(k.partitionId) + 0x9e3779b9u + (h << 6) + (h >> 2));
            return h;
        }
    };
    std::unordered_map<ReplicaKey, PartitionReplica*, ReplicaKeyHash> replicas_;

    // Outbound marshalling ring (agrona ManyToOneRingBuffer via IngressRing).
    // Producers = worker threads (sends); consumer = control thread (drainOutbound).
    // Size is NodeConfig-configurable (replicationOutboundRingBytes) — passed to
    // the ctor and used to construct this ring in the init list.
    rune::consensus::IngressRing outboundRing_;

    // Back-pressure observability: bumped when the outbound ring is full and a
    // (retriable) send had to be skipped. Control-thread reads for logging.
    uint64_t outboundRingFullCount_ = 0;

    // E2 inc2a: optional WorkerPool — when non-null, FetchResponse frames are
    // enqueued to the owning worker instead of calling applyReplicated inline.
    rune::node::WorkerPool* workerPool_ = nullptr;

    // Task 10 round 4: when wired, the ONLY replica source (use-time resolve).
    const rune::node::ReplicaRegistry* registry_ = nullptr;
    bool registryUserPartitions_ = true;
    PartitionReplica* resolveReplica_(int32_t topicId, PartitionId partitionId) const noexcept;
};

} // namespace rune::replication
