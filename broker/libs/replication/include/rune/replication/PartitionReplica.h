#pragma once
// ---------------------------------------------------------------------------
// PartitionReplica.h — Single-writer partition replica: leader and follower
//                      roles for the Rune data-plane replication layer.
//
// Namespace: rune::replication
// C++23. NO locks. NO busy-spin. Single thread owns all state.
//
// ## Overview (spec §4, plan P3-3)
//
// PartitionReplica ties together:
//   - Phase-1 rune::storage::Log  (persistent in-order byte store)
//   - Phase-1 rune::storage::LeaderEpochCache  (epoch→startOffset truncation)
//   - P3-1  ReplicaLeaderState  (HW + per-follower LEO)
//   - P3-2  IsrManager          (ISR shrink/expand, produce gate)
//   - P3-2  EpochFencing        (stale-epoch rejection, truncation offset)
//   - P3-1  ReplicationMessages (FetchRequest/FetchResponse SBE codecs)
//
// ## Role model
//
//   Role::Leader  — appends client batches, serves follower fetches, advances HW.
//   Role::Follower — fetches from leader, truncates on epoch change, applies batches.
//
// Both roles are driven by a single `doWork(int64_t nowNs)` call that MUST be
// called by one owning thread.  doWork() samples nowNs once and runs all
// periodic work (ISR checks, fetch emission) without spinning or sleeping.
//
// ## Transport injection
//
// PartitionTransport is a pure abstract interface so tests can plug in a
// deterministic fake.  Production code will implement it over Aeron; tests use
// InMemoryTransport below.
//
// ## No hot-path allocation
//
// All scratch buffers are pre-allocated in the constructor.  Reusable SBE
// codec instances are fields.  Record views (std::span) point into caller or
// log buffers.
// ---------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <mutex>
#include <span>
#include <unordered_map>
#include <limits>
#include <vector>
#include <string>

#include <rune/replication/Types.h>
#include <rune/replication/FollowerSlots.h>
#include <rune/replication/FollowerSync.h>
#include <rune/replication/HighWatermark.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/EpochFencing.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/LeaderEpochCache.h>
#include <rune/storage/ProducerStateManager.h>

// Forward-declared so this public header does not pull in the rune-protocol
// include path; the builder is held by unique_ptr and only completed in the .cpp.
namespace rune::protocol { class RecordBatchBuilder; }

namespace rune::replication {

class InternalIsrKnowledge;

// ---------------------------------------------------------------------------
// ReplicaPositionReport — soft per-replica position snapshot consumed by the
// controller's failover-election path.  Off-log (rebuilt on controller restart).
// ---------------------------------------------------------------------------
struct ReplicaPositionReport {
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int32_t nodeId         = 0;
    int32_t leaderEpoch    = 0;
    int64_t logEndPosition = 0;
};

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------
enum class Role { Leader, Follower };

// ---------------------------------------------------------------------------
// Error codes (returned in FetchResponseMsg::errorCode)
// ---------------------------------------------------------------------------
inline constexpr int16_t kErrNone         =  0;
inline constexpr int16_t kErrStaleEpoch   = -1;   // request epoch < local epoch
inline constexpr int16_t kErrNotLeader    = -2;   // this node is not the leader
inline constexpr int16_t kErrMinIsr       = -3;   // ISR too small to produce
inline constexpr int16_t kErrOutOfRange   = -4;   // fetchOffset out of range
// RETIRED (DR-BUG-6 reverse reconcile; kept so the code is never reused): a
// leader deferred the epoch answer while it reverse-reconciled a
// genuine committed continuation from this (ahead, on-lineage) follower.  This
// is a KEEP-ALIVE, NOT a truncation directive: the follower KEEPS its tail and
// RESETS its fallback probe counter so a slow reconcile under packet loss never
// trips the destructive fallback-to-committedHw truncation (the DR-8 loss).
inline constexpr int16_t kErrReconcileDeferred = -5;
// F18 ⇄ F3 (one code, both meanings): the first batch at the requested offset
// cannot be delivered AT ALL over this transport — it does not fit ONE
// replication frame (leader: its outbound/publication frame; follower: its own
// worker-ingress ring), independent of the follower's requested maxBytes.
// Distinct from kErrNone-with-empty-records ("caught up"): there is real data
// here that can never reach the follower at this size.  NOT retriable for this
// offset: the follower is forced Behind (F3, FollowerSync::forceBehind) and
// stops fetching it ("wedged", F18), and the leader does not count the request
// as ISR liveness, so the follower leaves the ISR visibly instead of pinning HW
// forever.  Cleared by a leadership/epoch change or a truncation that moves the
// follower's LEO.
inline constexpr int16_t kErrRecordTooLarge = -6;
// KIP-320/595 DivergingEpoch: the follower's (fetchOffset, lastFetchedEpoch) is
// not on the leader's epoch lineage.  The answer carries divergingEpoch /
// divergingEndOffset (= the leader's endOffsetForEpoch(lastFetchedEpoch)); the
// follower truncates to min(divergingEndOffset, its own end offset of that
// epoch) — or, if it never held that epoch, to its own end of the largest
// earlier epoch (another round) — and re-fetches.  Never credited (ISR / HW):
// only a fetch that passed validation counts.
inline constexpr int16_t kErrDivergingEpoch = -7;
// -8 was kErrLeaderNotComplete (the retired leader-completeness HOLD answer).
// Reserved: never reused on the wire.
// KIP-903 (local return code, never on the wire): the fetch came from an OLDER
// process incarnation of the follower than one already seen — dropped (no
// answer, no credit).
inline constexpr int16_t kErrStaleIncarnation = -10;
// The default replication fetch-ceiling (requested chunk size) used for
// every PartitionConfig across the broker (see BrokerNode.cpp).  This is
// deliberately NOT required to cover the gateway's max accepted produce
// batch: F3 round 2's progress rule means a first batch larger than this
// value is still served ALONE whenever it fits the transport's real hard
// per-message maximum (see onFetchRequest/pushToFollowers and
// PartitionTransport::maxFetchRecordsBytes()) — only a batch that exceeds
// even that hard transport maximum is genuinely unfetchable
// (kErrRecordTooLarge).  THE batch limit (NodeConfig::produceIntakeLimit)
// guarantees no such batch is ever ACCEPTED: the gateway and every worker
// reject a produce batch above THE batch limit (Worker::maxAcceptableBatchBytes) with
// MessageTooLarge (F3 round 5).
//
// F3 round 2 briefly raised this to 8 MiB to (wrongly) paper over a gap that
// the progress rule above already closes correctly; round 3 reverted it to
// 1 MiB — raising it changes replication chunking/latency for every topic
// with no benchmark, a real risk against the p99 <=100µs target, and is not
// needed now that the progress rule handles the oversize case correctly.
inline constexpr int32_t kDefaultMaxFetchBytes = 1 << 20;

// ---------------------------------------------------------------------------
// PartitionConfig — constructor argument bundle
// ---------------------------------------------------------------------------
struct PartitionConfig {
    int32_t     topicId           = 0;   ///< DR-BUG-5: topic identifier for multi-topic routing
    PartitionId partitionId       = 0;
    NodeId      selfId            = 0;
    int64_t     replicaLagMaxMs   = 10'000;         ///< ms before follower shrunk from ISR
    Offset      replicaLagMaxOffset = 10'000;       ///< offset lag before ISR shrink
    int         minIsr            = 1;              ///< min ISR for acksAll produce
    int64_t     fetchIntervalNs   = 50'000'000LL;   ///< follower fetch cadence (50 ms backstop; push is steady-state)
    int64_t     isrCheckIntervalNs = 10'000'000LL;  ///< leader ISR check cadence (10 ms)
    int64_t     followerFlushIntervalNs = 20'000'000LL;   ///< follower coalesced fsync interval; 0 = off (durability via replication)
    int32_t     maxFetchBytes     = 1 << 20;        ///< max bytes per fetch (1 MiB)
    bool        pushOnAppend      = true;            ///< GR-10: leader pushes to ISR followers immediately on append
    bool        fastAckOnApply    = true;            ///< GR-10: follower fast-acks (emit FetchRequest immediately after applyReplicated)
    CleanupPolicy cleanupPolicy   = CleanupPolicy::Delete;  ///< CP-1: log cleanup policy for this partition
    // CP-2: compaction tuning fields.
    double      minCleanableDirtyRatio = 0.5;        ///< CP-2: min dirty-offset fraction before compaction fires [0.0, 1.0]
    int64_t     deleteRetentionMs      = 86'400'000LL; ///< CP-2: how long (ms) to retain tombstones before removing them
    // CP-3: size/time retention (used by Delete + CompactDelete policies).
    int64_t     retentionBytes         = -1;          ///< CP-3: keep this many bytes; -1 = unbounded (no size deletion)
    int64_t     retentionMs            = -1;          ///< CP-3: keep segments younger than this ms; -1 = unbounded
    // CP-8: compaction lag gates.
    int64_t     minCompactionLagMs     = 0;           ///< CP-8: dirty segment uncleanable if largestTs within lag. Default 0.
    int64_t     maxCompactionLagMs     = -1;          ///< CP-8: force compaction if oldest dirty seg exceeds this age. Default -1=disabled.
    // CP-8: per-topic segment roll size — used as the coalescing group bound so
    // each topic coalesces to its own roll threshold, not a global constant.
    int64_t     segmentRollBytes       = 64LL * 1024 * 1024; ///< CP-8: max combined bytes per coalesced group. Default 64 MiB.
    // CP-9: retention scheduler cadence + async-delete delay.
    int64_t     retentionCheckMs       = 300'000LL;   ///< CP-9: how often to run retention (ms). Default 5 min (Kafka). 1 for tests.
    int64_t     fileDeleteDelayMs      = 60'000LL;    ///< CP-9: ms before .deleted files are physically removed. Default 1 min.
    // CP-9: log-start-offset retention — delete segments entirely below this offset.
    // 0 = disabled (no log-start-offset deletion).
    int64_t     logStartOffset         = 0;           ///< CP-9: lower bound; segs entirely below here are deleted. 0=disabled.
    DurabilityMode durability = DurabilityMode::Majority; ///< per-topic durability quorum rule.
    // When AllInSync, minIsr is forced to the replica-set size (RF) so acks require every replica.
    // E3 Task 5: async-durability tier (0=Leader, 1=Majority, 2=Strict).
    // Strict (2) = ack only after local fsync completion; others ack on append.
    uint8_t durabilityTier = 1;  // default Majority — parity-safe (existing tests unchanged)
};

// ---------------------------------------------------------------------------
// PartitionTransport — injected interface (no Aeron in this file)
//
// Leader uses sendFetchResponse(); follower uses sendFetchRequest().
//
// F27 — every send REPORTS whether the frame was accepted by the transport
// (true) or refused (false: Aeron BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED,
// no publication, oversized, outbound ring full).  The replica never assumes a
// refused send was delivered: it retries on its next pass (time-independent);
// it never waits for a resend timer to discover its own drop.
// The fake pumps responses back into the leader via its onFetchRequest() and
// pumps requests back into the follower via its applyReplicated().
// ---------------------------------------------------------------------------
enum class TransportKind : uint8_t {
    Generic   = 0,  ///< virtual dispatch — test fakes only (BrokerNode asserts never in production)
    PerWorker = 1,  ///< concrete final WorkerReplicationTransport — direct calls
    Aeron     = 2,  ///< concrete final AeronPartitionTransport (internal partitions) — direct calls
    Null      = 3,  ///< concrete final NullPartitionTransport (no peers) — direct no-op calls
};

class PartitionTransport {
public:
    virtual ~PartitionTransport() = default;

    /// Construction-time classification (spec §3.5a.1): PartitionReplica reads
    /// this ONCE and dispatches the PerWorker arm through a direct call.
    [[nodiscard]] virtual TransportKind kind() const noexcept { return TransportKind::Generic; }

    /// (Follower → Leader) send a FetchRequest.
    virtual bool sendFetchRequest(const FetchRequestMsg& req) = 0;

    /// (Follower → its KNOWN leader) spec §3.5.2: unicast.  Default keeps the old
    /// behaviour for transports that cannot address a peer (tests, legacy ring).
    virtual bool sendFetchRequestTo(NodeId /*leader*/, const FetchRequestMsg& req) {
        return sendFetchRequest(req);
    }

    /// (Leader → Follower) send a FetchResponse.
    virtual bool sendFetchResponse(NodeId toFollower,
                                   const FetchResponseMsg& resp) = 0;

    // -----------------------------------------------------------------------
    // OffsetsForLeaderEpoch exchange + the RETIRED DR-BUG-6 reverse reconcile
    // (PartitionReplica no longer sends either; the plumbing stays for wire
    // compatibility).
    //
    // These are peer-routed (addressed to a specific NodeId).  They have SAFE
    // DEFAULT NO-OP bodies so a transport that has not implemented them yet
    // still compiles AND the replica degrades to the conservative
    // truncate-to-committed-HW fallback (never loses committed data, but may
    // re-fetch a small committed tail).  InMemoryTransport (tests),
    // FilteringTransport (integration harness) and AeronPartitionTransport
    // (production) implement them fully.
    // -----------------------------------------------------------------------

    /// (New follower → New leader) ask for the leader's end offset for the last
    /// epoch they share (KIP-101), BEFORE the follower truncates on epoch change.
    virtual bool sendOffsetsForLeaderEpochRequest(
            NodeId /*toLeader*/, const OffsetsForLeaderEpochReqMsg& /*req*/) { return true; }

    /// (Leader → New follower) the epoch-boundary answer.
    virtual bool sendOffsetsForLeaderEpochResponse(
            NodeId /*toFollower*/, const OffsetsForLeaderEpochRespMsg& /*resp*/) { return true; }

    /// F18: the largest records payload ONE FetchResponse can carry to any peer
    /// on this transport (conservative min over its frame bounds).  Unbounded by
    /// default (test fakes).  Queried only on cold paths (an oversized first
    /// batch) and once per push.
    [[nodiscard]] virtual int64_t maxFetchRecordsBytes() const noexcept {
        return std::numeric_limits<int64_t>::max();
    }

    /// (Deficient leader → ahead survivor) pull missing committed records
    /// [fromOffset, survivorLEO) — a REVERSE fetch (leader fetches from a
    /// follower).  Reuses the FetchRequest shape.
    virtual bool sendReconcileRequest(
            NodeId /*toPeer*/, const FetchRequestMsg& /*req*/) { return true; }

    /// (Ahead survivor → deficient leader) serve the requested records.
    /// Reuses the FetchResponse shape.
    virtual bool sendReconcileResponse(
            NodeId /*toPeer*/, const FetchResponseMsg& /*resp*/) { return true; }

    /// (Partition leader → controller) Kafka AlterPartition: propose an ISR
    /// change.  Cold path (ISR changes only).  Default: no controller channel
    /// (false = not sent; a replica with this transport as its ISR channel
    /// re-sends on its resend cadence).
    virtual bool sendAlterIsr(const AlterIsrMsg& /*msg*/) { return false; }

    /// (Internal-partition leader → replica) the versioned internal ISR state
    /// (MAJOR-2/3).  Cold path (ISR changes only); re-sent until acked.
    virtual bool sendInternalIsr(NodeId /*to*/, const InternalIsrMsg& /*msg*/) { return false; }
    /// (Replica → internal-partition leader) "I durably hold that state".
    virtual bool sendInternalIsrAck(NodeId /*to*/, const InternalIsrAckMsg& /*msg*/) { return false; }

    // (F3 fix-round-2 item A's maxFetchResponseBytes() — the hard per-message
    //  records ceiling a FIRST batch is served alone within — is the SAME
    //  quantity as F18's maxFetchRecordsBytes() above: one virtual.)
};

class AeronPubRegistry;
template <typename PubRegistry> class WorkerReplicationTransportImpl;
class AeronPartitionTransport;

// ---------------------------------------------------------------------------
// NullPartitionTransport — production transport for a replica with no
// replication endpoint (single-node / no repl port): every send is a no-op.
// Final + TransportKind::Null so the replica calls it directly (Task 11 fix 2;
// replaces the test InMemoryTransport — mutex + unbounded queues — in BrokerNode).
// ---------------------------------------------------------------------------
class NullPartitionTransport final : public PartitionTransport {
public:
    [[nodiscard]] TransportKind kind() const noexcept override { return TransportKind::Null; }
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchRequestTo(NodeId, const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
    bool sendOffsetsForLeaderEpochRequest(NodeId, const OffsetsForLeaderEpochReqMsg&) override { return true; }
    bool sendOffsetsForLeaderEpochResponse(NodeId, const OffsetsForLeaderEpochRespMsg&) override { return true; }
    bool sendReconcileRequest(NodeId, const FetchRequestMsg&) override { return true; }
    bool sendReconcileResponse(NodeId, const FetchResponseMsg&) override { return true; }
    bool sendInternalIsr(NodeId, const InternalIsrMsg&) override { return true; }
    bool sendInternalIsrAck(NodeId, const InternalIsrAckMsg&) override { return true; }
};

// ---------------------------------------------------------------------------
// PartitionReplica
// ---------------------------------------------------------------------------
class PartitionReplica {
public:
    /// Transport arm chosen at construction (spec §3.5a.1).
    [[nodiscard]] TransportKind transportKindForTest() const noexcept { return tkind_; }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param cfg        Partition configuration.
    /// \param role       Initial role (Leader or Follower).
    /// \param epoch      Current leader epoch (stamped on every batch).
    /// \param replicas   Full replica set (NodeIds, including self).
    /// \param isr        Initial ISR (subset of replicas; often == replicas at start).
    /// \param log        Non-owning pointer to the partition log (pre-constructed;
    ///                   owner is the test/container).  De-virtualized seam
    ///                   (Milestone D P2): there are TWO concrete overloads — a
    ///                   disk-backed rune::storage::Log* and an in-memory
    ///                   rune::storage::InMemoryLog*.  Exactly one backend is stored
    ///                   and `memoryBacked_` is fixed at construction, so every
    ///                   hot-path log op is a DIRECT concrete call (no vtable).
    /// \param transport  Injected transport (fake for tests, Aeron for prod).
    PartitionReplica(PartitionConfig                       cfg,
                     Role                                   role,
                     Epoch                                  epoch,
                     std::vector<NodeId>                    replicas,
                     std::vector<NodeId>                    isr,
                     rune::storage::Log*                    log,
                     PartitionTransport*                    transport);

    /// In-memory-backed overload (tests / DR harness / memory-persistence topics).
    PartitionReplica(PartitionConfig                       cfg,
                     Role                                   role,
                     Epoch                                  epoch,
                     std::vector<NodeId>                    replicas,
                     std::vector<NodeId>                    isr,
                     rune::storage::InMemoryLog*            log,
                     PartitionTransport*                    transport);

    /// Declared (defaulted in the .cpp) so the unique_ptr<RecordBatchBuilder>
    /// member can hold a forward-declared type — dtor runs where it is complete.
    ~PartitionReplica();

    // -----------------------------------------------------------------------
    // Role transitions (called by control plane on leader election / demotion)
    // -----------------------------------------------------------------------

    /// Transition to Leader with the given epoch.
    /// Resets ISR from replicas, seeds ReplicaLeaderState.
    /// A SAME-EPOCH re-assert while already Leader (a partitionEpoch-only ISR
    /// change: the controller committed a new ISR, e.g. our AlterIsr) only
    /// applies that committed ISR and resets nothing else (follower slots, push
    /// state, HW floors, the epoch cache).
    /// `partitionEpoch` = the committed assignment's partition epoch (the
    /// AlterIsr compare-and-set version, MEDIUM-1); -1 = not controller-managed
    /// (internal partitions, unit rigs).
    void becomeLeader(Epoch newEpoch, std::vector<NodeId> isr, int32_t partitionEpoch = -1);

    /// Transition to Follower with the given leader epoch.  A same-epoch
    /// re-assert of the same leader while already Follower is a no-op.
    void becomeFollower(Epoch leaderEpoch, NodeId leaderId);

    /// Kafka AlterPartition wiring: ISR changes of this (leader) replica are
    /// PROPOSED to the controller through `channel` and applied only once the
    /// controller committed them (they come back as a same-epoch becomeLeader).
    /// nullptr (default) = self-managed ISR (single node / unit rigs).  Set
    /// before the replica is driven.
    void setIsrChannel(PartitionTransport* channel) noexcept { isrChannel_ = channel; }
    /// Kafka broker fencing (leader only): members the cluster considers DEAD
    /// (not in `live`) leave the ISR at once instead of after replicaLagMaxMs —
    /// self-managed ISR: immediately; controller-managed: proposed (AlterIsr).
    /// Cold; called on the control thread for INTERNAL partitions (whose ISR is
    /// leader-local); user partitions are fenced by the controller.
    void shrinkIsrToLive(std::span<const int32_t> live, int64_t nowNs);
    /// Kafka AlterPartitionResponse (MEDIUM-2): the controller's explicit answer
    /// to our AlterIsr.  A reject (INELIGIBLE_REPLICA / FENCED_LEADER_EPOCH /
    /// INVALID_UPDATE_VERSION / invalid) clears the pending proposal (the HW
    /// then counts the committed ISR only); a commit resolves it through the
    /// applied assignment (becomeLeader re-assert with a newer partitionEpoch).
    /// After an INELIGIBLE reject, additions are not re-proposed for
    /// kAlterIsrResendNs (shrinks are never held back).  Owner thread.
    void onAlterIsrResponse(const AlterIsrResponseMsg& r);
    /// KIP-903: this node's PROCESS incarnation (monotonic per node, bumped on
    /// every start; 0 = untracked).  Stamped on every FetchRequest this replica
    /// sends; set once before the replica is driven (ReplicaRegistry::add).
    void setProcessIncarnation(int64_t inc) noexcept { processIncarnation_ = inc; }
    [[nodiscard]] int64_t processIncarnation() const noexcept { return processIncarnation_; }

    // -----------------------------------------------------------------------
    // Leader-epoch restart fence (Kafka parity; 2026-10-05 epoch-restart fix).
    //
    // Every establishment of leadership must use a leader epoch strictly
    // greater than any epoch already in any replica's log: KIP-101 truncation
    // cannot tell two histories written under the SAME epoch apart.  After a
    // restart the committed assignment this node replays can name it leader at
    // the very epoch its previous incarnation led (and wrote) at — leading at
    // it again would write a second history under that epoch.
    //
    // restartFenceEpoch_ = the highest epoch this replica led at in ANY earlier
    // incarnation (durable `leader-epoch-fence` file, written tmp+fsync+rename
    // at every new-epoch becomeLeader, before the first append) or holds in its
    // log lineage (leader-epoch checkpoint).  When a controller manages this
    // partition (setIsrChannel != null), becomeLeader(e <= fence) is REFUSED:
    // the replica stays a follower of no leader (never accepts a produce) and
    // asks the controller for a new epoch (AlterIsr with
    // isrCount == AlterIsrMsg::kLeaderEpochBump), re-sent every kAlterIsrResendNs
    // (loss recovery only) until a newer role arrives.  The controller answers
    // with the same assignment at leaderEpoch + 1 (MetadataService::onAlterIsr).
    // Without a controller (single node: no other replica can diverge) the
    // fence is inert.
    // -----------------------------------------------------------------------
    /// Where the fence persists.  Disk-backed replicas default to
    /// <log dir>/leader-epoch-fence; memory-backed ones are given a path under
    /// the node's data dir by BrokerNode.  Loads the persisted value.  Cold.
    void setLeaderEpochFenceFile(std::string path);
    [[nodiscard]] Epoch restartFenceEpochForTest() const noexcept { return restartFenceEpoch_; }
    [[nodiscard]] bool  staleLeadershipPendingForTest() const noexcept { return staleLead_.pending; }
    [[nodiscard]] uint64_t staleLeadershipRefusalsForTest() const noexcept { return staleLeadRefusals_; }
    /// Test view: the committed partitionEpoch this leader derives proposals from.
    [[nodiscard]] int32_t committedPartitionEpochForTest() const noexcept { return committedPartitionEpoch_; }
    /// INTERNAL partitions (MAJOR-2/3): ISR changes commit on a MAJORITY's
    /// durable knowledge instead of at once — the leader publishes each change
    /// (InternalIsr) to every replica, each persists it in `k` and acks, and it
    /// commits once a majority holds it; the HW counts committed ∪ proposed
    /// until then.  A new leadership starts from every replica (HW-counting
    /// seed) and publishes its first ISR only once every member it keeps
    /// fetched (validated) at >= max(HW, electionLeo_) — so a published ISR
    /// never names a member lacking committed records.  The consensus election
    /// then votes only for members of the newest durable ISRs
    /// (InternalIsrKnowledge::eligible).  nullptr = self-managed.  Set before
    /// the replica is driven (control thread).
    void setInternalIsrKnowledge(InternalIsrKnowledge* k) noexcept { isrKnowledge_ = k; }
    /// (Follower side) persist an internal ISR state, ack it to its leader.
    void onInternalIsr(const InternalIsrMsg& m);
    /// (Leader side) a replica holds our published state (or newer).
    void onInternalIsrAck(const InternalIsrAckMsg& m);
    /// Test view: a new internal leadership has not yet committed its first ISR.
    [[nodiscard]] bool internalSeedPendingForTest() const noexcept { return internalSeedPending_; }
    /// Test view: an AlterIsr proposal is in flight.
    [[nodiscard]] bool isrProposalPendingForTest() const noexcept { return isrProposal_.pending; }
    /// Test view: the maximal ISR the HW is computed over (committed ∪ proposed).
    [[nodiscard]] const std::vector<NodeId>& maximalIsrForTest() const noexcept;

    // -----------------------------------------------------------------------
    // Leader API
    // -----------------------------------------------------------------------

    /// Append a record batch to the leader's log and advance LEO.
    ///
    /// \param records   Raw record bytes (the batch payload — caller-managed).
    /// \param acksAll   true → require |ISR| >= minIsr (majority-durable produce).
    /// \returns base offset of the appended batch, or -1 if gated (minIsr).
    ///
    /// Does NOT block. Does NOT wait for followers. The caller drives doWork()
    /// to pump follower fetches which advance the HW.
    ///
    /// F15 N2: a prebuilt control batch (isControlBatch) is never wrapped: it is
    /// delegated to appendControlBatch (and so may also return kAppendNotLeader).
    Offset append(std::span<const uint8_t> records, bool acksAll = true);

    /// appendControlBatch / append() failure codes (all negative: never an offset).
    static constexpr Offset kAppendMinIsr       = -1;   ///< gated by min-ISR (retriable)
    static constexpr Offset kAppendNotLeader    = -2;   ///< this replica is not the leader (retriable)
    static constexpr Offset kAppendInvalidBatch = -3;   ///< not a whole, CRC-valid control batch
    static constexpr Offset kAppendBackPressure = -4;   ///< in-memory ring full (retriable)

    /// F15 N2: append a PREBUILT control batch (a txn COMMIT/ABORT marker) AS a
    /// control batch — the bytes land verbatim (CONTROL|TRANSACTIONAL, the
    /// marker's producerId), never wrapped as the value of a data batch.
    ///
    ///   - role gate: not Leader → kAppendNotLeader (no assert; retriable);
    ///   - the min-ISR gate of append() → kAppendMinIsr;
    ///   - the base offset is restamped in place (Log::appendNoSyncRestamp;
    ///     CRC-safe: [0..7] precede the CRC region), never via makeBatch_;
    ///   - then append()'s post-append sequence: leader LEO, HW, cleaner
    ///     publish, push-on-append, and the SAME flush request append() issues
    ///     (worker-driven: followerFlushRequested_ for the owning worker;
    ///     control-thread internal partitions: the inline durable sync).
    ///
    /// \returns the marker's base offset, or a negative kAppend* code.
    Offset appendControlBatch(std::span<const uint8_t> controlBatch, bool acksAll = true);

    /// True iff `bytes` is exactly one whole, CRC-valid record batch with the
    /// CONTROL attribute — i.e. a prebuilt txn marker.
    [[nodiscard]] static bool isControlBatch(std::span<const uint8_t> bytes) noexcept;

    /// CP-1: append a single record with an explicit (non-empty) key.
    ///
    /// Builds a one-record RecordBatch with the supplied key and value bytes,
    /// so the on-disk record carries a real compaction key.  All other semantics
    /// (ISR gate, HW advance, push-on-append) are identical to append().
    ///
    /// \param key      Record key bytes (e.g. compaction identity for __consumer_offsets).
    /// \param value    Record value bytes (the payload).
    /// \param acksAll  true → require |ISR| >= minIsr (majority-durable produce).
    /// \returns base offset of the appended batch, or -1 if gated (minIsr).
    Offset appendKeyed(std::span<const uint8_t> key,
                       std::span<const uint8_t> value,
                       bool acksAll = true);

    /// Reconcile the leader's tracked LEO with the underlying Log and recompute HW.
    ///
    /// Called after a batch is appended DIRECTLY to the leader's partition Log by
    /// a co-located writer that bypasses append() — e.g. the transaction layer
    /// stamping already-framed transactional data batches or control (COMMIT/ABORT)
    /// markers into the same partition log the replica owns.  Without this call the
    /// leader's ReplicaLeaderState would still hold the pre-append LEO and HW would
    /// never advance to cover the externally-written bytes.
    ///
    /// GR-10: when pushOnAppend is enabled, also pushes the newly-appended bytes
    /// to each ISR follower via an unsolicited FetchResponse (non-blocking).
    ///
    /// No-op on a follower (follower LEO/HW are driven by applyReplicated()).
    void onLeaderLogAppended() noexcept;

    /// Handle an inbound FetchRequest from a follower.
    ///
    /// Epoch-fences (rejects stale leaderEpoch with kErrStaleEpoch).
    /// Updates follower LEO in ReplicaLeaderState + IsrManager.
    /// Recomputes HW.
    /// Sends a FetchResponse via transport_.
    ///
    /// \param req    The fetch request from the follower.
    /// \param nowNs  Current wall-clock time (nanoseconds) used to record the
    ///               follower's real fetch time in IsrManager.  Callers MUST pass
    ///               the actual current time so time-lag eviction is correct.
    ///
    /// \returns kErrNone on success, error code otherwise.
    /// \param connGen  F27 R1: the follower's connection generation at the
    ///                 transport (0 = not tracked).  A CHANGED generation means
    ///                 the link to that follower went down and came back — frames
    ///                 the transport accepted meanwhile may never have arrived, so
    ///                 its push state is rewound to this fetch (re-sent, not lost).
    int16_t onFetchRequest(const FetchRequestMsg& req, int64_t nowNs, uint32_t connGen = 0);

    /// Read committed records (≤ highWatermark) starting at fetchOffset.
    ///
    /// Returns empty span if fetchOffset >= highWatermark (nothing committed
    /// beyond the caller's position yet).
    ///
    /// The returned vector's lifetime is the caller's responsibility.
    std::vector<uint8_t> read(Offset fetchOffset, int32_t maxBytes) const;

    // -----------------------------------------------------------------------
    // Follower API
    // -----------------------------------------------------------------------

    /// Apply a FetchResponse received from the leader.
    ///
    /// On epoch change (response.leaderEpoch > local):
    /// KIP-320/595: a DivergingEpoch answer runs the KIP-279 truncation and
    /// re-fetches; a frame from an older OR newer epoch than ours is dropped
    /// (never adopted, never truncated on).  Applied batches keep the leader's
    /// stamped partitionLeaderEpoch; the epoch cache is built from those stamps.
    ///
    /// Then append the records to the local Log (if any).
    /// Update local HW = min(local LEO, response.highWatermark).
    void applyReplicated(const FetchResponseMsg& resp);

    /// Build a FetchRequest for the leader: fetchOffset = local LEO,
    /// lastFetchedEpoch = the epoch of the last batch in our log (-1 = empty).
    FetchRequestMsg nextFetch() const noexcept;

    /// KIP-320/595: the epoch of the last record in our log (the epoch-cache
    /// entry owning offset LEO-1), -1 for an empty log.
    [[nodiscard]] int32_t lastFetchedEpoch() const noexcept;
    /// Test view: the epoch of OUR record at offset-1 (what a follower whose log
    /// is a prefix of ours ending at `offset` sends as lastFetchedEpoch).
    [[nodiscard]] int32_t lastFetchedEpochAtForTest(Offset offset) const {
        return offset <= 0 ? -1 : epochCache_.epochForOffset(offset - 1);
    }

    // -----------------------------------------------------------------------
    // OffsetsForLeaderEpoch (KIP-101) + the retired DR-BUG-6 reverse reconcile.
    //
    // Divergence is resolved on the FETCH path (lastFetchedEpoch validation +
    // DivergingEpoch, KIP-320/595); followers never send side probes and a
    // leader never pulls records it did not hold when elected.  The handlers
    // stay so the transports' message plumbing (and a peer on an older build)
    // stays well-defined.
    // -----------------------------------------------------------------------

    /// (Leader) Answer an OffsetsForLeaderEpoch query authoritatively from the
    /// LeaderEpochCache: (responseEpoch, endOffset) = endOffsetForEpoch.  No
    /// state change, no ISR / HW credit.
    void onOffsetsForLeaderEpochRequest(const OffsetsForLeaderEpochReqMsg& req);

    /// (Follower) Retired: this build never probes, so an answer is ignored.
    void applyOffsetsForLeaderEpochResponse(const OffsetsForLeaderEpochRespMsg& resp);

    /// Retired reverse-reconcile pull: ignored (a leader never takes records it
    /// did not hold when elected).
    void onReconcileRequest(const FetchRequestMsg& req);

    /// Retired reverse-reconcile answer: ignored.
    void applyReconcileResponse(const FetchResponseMsg& resp);

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Drive periodic work for this replica.
    ///
    /// Leader:   runs IsrManager shrink/expand on isrCheckIntervalNs cadence;
    ///           propagates updated ISR into ReplicaLeaderState.
    /// Follower: emits a FetchRequest when fetchIntervalNs has elapsed.
    ///
    /// \param nowNs   Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of work items performed (0 when idle — no spin).
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] Role        role()           const noexcept { return role_; }
    [[nodiscard]] Epoch       epoch()          const noexcept { return epoch_; }
    /// THE produce gate — the only predicate every produce / control-batch
    /// append path consults (worker, gateway inline, internal stores via
    /// append*, the DR harness).  Kafka model (HW = min LEO over the ISR, the
    /// leader is always an ISR member): every leader holds every committed
    /// record, so leadership alone is the gate.  Refusal = retriable NotLeader.
    [[nodiscard]] [[gnu::always_inline]] bool acceptsProduce() const noexcept {
        return role_ == Role::Leader;
    }
    /// F15 §2.1 step 3 / §3.2: bumped by every change to the log's existing
    /// content — a tail truncation (KIP-279; formerly also a reconcile append of
    /// foreign old-term batches).  A parked waiter compares it with the value it parked under.
    /// Owner thread only (plain member, no atomic).
    [[nodiscard]] uint64_t    logGeneration()  const noexcept { return logGeneration_; }
    /// The offset the log was cut to by the change that last bumped
    /// logGeneration() (a reconcile append records the LEO before it): every
    /// offset below it is unchanged by that change.  Owner thread only.
    [[nodiscard]] Offset      lastTruncationOffset() const noexcept { return lastTruncationOffset_; }
    /// F15 S4: bumped on every Leader <-> Follower transition (becomeLeader from
    /// a non-leader, becomeFollower, self-demotion in onFetchRequest) — NOT on a
    /// leader-epoch bump while still Leader.  While Follower the log may grow by
    /// replicated appends that a leader-side incremental state never saw, so the
    /// worker's read_committed state is valid only within one leadership.
    /// Owner thread only (plain member, no atomic).
    [[nodiscard]] uint64_t    leadershipGeneration() const noexcept { return leadershipGeneration_; }
    [[nodiscard]] Offset      logEndOffset()   const noexcept { return logEndOffset_(); }
    [[nodiscard]] Offset      highWatermark()  const noexcept;

    /// Soft per-replica position snapshot for the controller's election path.
    /// leaderEpoch = epoch(); logEndPosition = logEndOffset().
    [[nodiscard]] ReplicaPositionReport positionReport() const noexcept {
        return ReplicaPositionReport{ cfg_.topicId, cfg_.partitionId, cfg_.selfId,
                                      static_cast<int32_t>(epoch_), logEndOffset_() };
    }

    // -----------------------------------------------------------------------
    // CLEANER-THREAD-SAFE SNAPSHOTS (TSan race fix — see publishCleanerState()).
    //
    // The LogCleanerAgent runs on its OWN thread (cleanerRunner_) concurrently
    // with the duty/append thread (NodeRuntimeAgent on agentRunner_).  It MUST
    // NOT read the live leader/ISR state (leaderState_, isrManager_->isr(),
    // lastHealthyHw_, role_) — those are mutated by the duty
    // thread without synchronisation (torn std::vector reads, non-atomic scalar
    // writes → UB, TSan-proven).
    //
    // Instead the duty thread PUBLISHES a snapshot of the served HW and the role
    // to these atomics (release-store) whenever they change, and the cleaner
    // READS them (acquire-load).  A slightly-stale HW/role is always SAFE for
    // compaction/retention: both operate strictly BELOW a conservative HW, and
    // a demoted-but-not-yet-observed leader simply does one extra harmless
    // pure-file cycle that is dropped (the swap/delete rings are drained by the
    // append thread which is authoritative).  The duty/append HW semantics are
    // completely unchanged; this only mirrors the value into an atomic.
    //
    // cleanerHighWatermark(): the served committed HW as of the last duty-thread
    //   publication.  Monotone-ish (tracks highWatermark()); the cleaner uses it
    //   as the conservative firstUncleanable / retention upper bound.
    // cleanerRole(): the role as of the last duty-thread publication.  The
    //   cleaner gates all work on cleanerRole() == Leader.
    [[nodiscard]] Offset cleanerHighWatermark() const noexcept {
        return publishedHwForCleaner_.load(std::memory_order_acquire);
    }
    [[nodiscard]] Role cleanerRole() const noexcept {
        return static_cast<Role>(publishedRoleForCleaner_.load(std::memory_order_acquire));
    }
    // Task #13: gatewayHighWatermark() — acquire-load of the same published-HW
    // atomic used by the LogCleanerAgent.  The owning worker publishes with
    // release (in publishCleanerState()) whenever the HW changes; the gateway
    // reads with acquire here.  This is the ONLY correct way for the gateway
    // (control thread) to observe the worker-owned HW without a TSan race
    // (closes R5/R6b).  A slightly-stale snapshot is safe: it is used only as
    // a conservative fetch/ack bound — the acks=All drain will simply wait one
    // more cycle before the worker's next publishCleanerState() stores the
    // fresh value.
    [[nodiscard]] Offset gatewayHighWatermark() const noexcept {
        return publishedHwForCleaner_.load(std::memory_order_acquire);
    }
    // D3b (Kafka LogCleaner parity): the cleaner never cleans at/after the last
    // stable offset — an OPEN transaction's data must stay untouched (it may still
    // abort, and an aborted value must never supersede a committed one).  The
    // OWNING worker publishes the first offset of the earliest open transaction of
    // its read_committed state (RcState LsoTracker) here; the LogCleanerAgent caps
    // firstUncleanable at it, exactly as it does at cleanerHighWatermark().
    // INVARIANT (fix round 1): a partition with read_committed state (topicId >= 0
    // — exactly the partitions a Worker builds an RcState for) STARTS at 0 = clean
    // nothing, and is 0 whenever that state is not Ready (unbuilt / scanning): the
    // worker publishes 0 BEFORE any rebuild step and the first real value only on
    // READY.  So no cleaner pass starts while the log's transaction state is being
    // rebuilt (and the worker defers draining a cleaner swap until the rebuild is
    // done — see Worker::cleanerSwapsDeferred_).  kNoCleanerLso = no open
    // transaction, or no read_committed state at all (internal topics, topicId <
    // 0): no cap beyond HW (the cleaner's own log scan still never cleans an
    // unresolved txn).  Slightly stale is safe like the HW snapshot.
    static constexpr Offset kNoCleanerLso = std::numeric_limits<Offset>::max();
    void publishCleanerLso(Offset firstUnstableOffset) noexcept {
        publishedLsoForCleaner_.store(firstUnstableOffset, std::memory_order_release);
    }
    // D3b fix round 1: the minimum fetch position over EVERY assigned replica (not
    // just the ISR), published by the duty/owning thread in publishCleanerState().
    // A txn marker below it is held by every replica, so the cleaner may remove it;
    // at/above it a lagging replica (possibly outside the ISR) that already holds
    // the txn's data may still need the marker — removing it would make that
    // replica, once leader, read the aborted data as the producer's NEXT txn.  A
    // replica that never fetched counts as 0, so a permanently dead assigned
    // replica keeps every marker (correctness over growth).  Follower role: 0.
    // Only REAL fetch positions count (onFetchRequest), relearned every
    // leadership; an OffsetsForLeaderEpoch probe's LEO never does (it may be a
    // divergent pre-truncation tail).  The bound TRUSTS reported fetch positions:
    // a follower that lost an unflushed tail in a crash keeps its old position
    // here until it fetches again — safe unless a marker's delete horizon
    // (deleteRetentionMs) expires within that follower's restart.
    // kNoCleanerLso when this is the only assigned replica.
    [[nodiscard]] Offset cleanerMarkerRemovalBound() const noexcept {
        return publishedMarkerBoundForCleaner_.load(std::memory_order_acquire);
    }
    [[nodiscard]] Offset cleanerLastStableOffset() const noexcept {
        return publishedLsoForCleaner_.load(std::memory_order_acquire);
    }
    // Task #13: gatewayIsrSize() — acquire-load of the published ISR size snapshot.
    // The owning worker publishes (in publishCleanerState()) the current ISR size
    // whenever the HW/ISR state is updated.  The gateway reads it in
    // drainPendingAcks() instead of directly accessing isrManager_->isr().size()
    // (which races with the worker's ISR vector mutations).  A stale low count
    // delays an acks=All drain by at most one cycle — never delivers a false OK.
    [[nodiscard]] int gatewayIsrSize() const noexcept {
        return publishedIsrSizeForGateway_.load(std::memory_order_acquire);
    }

    // ── CONTROLLER POSITION SNAPSHOT (leadership-convergence path) ─────────────
    // The duty thread publishes (in publishCleanerState()) this replica's view of
    // every replica-node's LEO for THIS partition: a leader publishes its own LEO
    // plus each follower's last-fetched LEO (from leaderState_.followerLeos()); a
    // follower publishes only its own LEO.  The control thread (NodeRuntimeAgent)
    // acquire-loads these and feeds the controller's ReplicaPositionTable so the
    // DynamicBalancer can decide, off POSITIONS (not a heartbeat timeout race),
    // whether a returned replica is caught up enough to reclaim leadership after a
    // transient-liveness ISR collapse.  Same TSan-clean release/acquire idiom as
    // the cleaner/gateway HW snapshots above.  Node ids beyond kMaxControllerNodes
    // are simply not snapshotted (safe: they just won't be restoration candidates).
    static constexpr int kMaxControllerNodes = 32;
    /// leaderEpoch as of the last publication (-1 = never published).
    [[nodiscard]] int32_t controllerLeaderEpoch() const noexcept {
        return publishedControllerEpoch_.load(std::memory_order_acquire);
    }
    /// This replica's published LEO for `nodeId` (self or a follower it leads);
    /// -1 if unknown / out of range.  (Stored as leo+1 so a zero-init slot reads
    /// as "unknown" while a genuine LEO of 0 is representable.)
    [[nodiscard]] Offset controllerReplicaLeo(NodeId nodeId) const noexcept {
        if (nodeId < 0 || nodeId >= kMaxControllerNodes) return -1;
        const Offset raw = publishedReplicaLeo_[static_cast<std::size_t>(nodeId)]
                               .load(std::memory_order_acquire);
        return raw == 0 ? -1 : raw - 1;
    }

    [[nodiscard]] int32_t     topicId()        const noexcept { return cfg_.topicId; }
    [[nodiscard]] PartitionId partitionId()    const noexcept { return cfg_.partitionId; }
    [[nodiscard]] NodeId      selfId()         const noexcept { return cfg_.selfId; }
    /// E5.1.2d: min-ISR required for acks=All produce on this partition.
    [[nodiscard]] int         minIsr()         const noexcept { return cfg_.minIsr; }
    /// TEST SEAM: change min-ISR on a live leader (e.g. an ISR-recovery scenario without a 2nd node).
    void setMinIsrForTest(int v) noexcept { cfg_.minIsr = v; effectiveMinIsr_ = v; }
    /// CP-1: the log cleanup policy for this partition (consulted by the LogCleaner).
    [[nodiscard]] CleanupPolicy cleanupPolicy() const noexcept { return cfg_.cleanupPolicy; }

    // CP-2: compaction config accessors.
    /// Minimum dirty-offset fraction that must be exceeded before compaction fires.
    [[nodiscard]] double      minCleanableDirtyRatio() const noexcept { return cfg_.minCleanableDirtyRatio; }
    /// How long (ms) tombstones are retained before being fully removed.
    [[nodiscard]] int64_t     deleteRetentionMs()      const noexcept { return cfg_.deleteRetentionMs; }
    // CP-3: size/time retention accessors.
    /// Max bytes to retain; -1 = unbounded.
    [[nodiscard]] int64_t     retentionBytes()         const noexcept { return cfg_.retentionBytes; }
    /// Max age (ms) to retain; -1 = unbounded.
    [[nodiscard]] int64_t     retentionMs()            const noexcept { return cfg_.retentionMs; }
    // CP-8: compaction lag accessors.
    /// A dirty segment is uncleanable while largestTs > nowMs - minCompactionLagMs. 0 = none.
    [[nodiscard]] int64_t     minCompactionLagMs()     const noexcept { return cfg_.minCompactionLagMs; }
    /// Force compaction when oldest dirty seg exceeds this age (ms). -1 = disabled.
    [[nodiscard]] int64_t     maxCompactionLagMs()     const noexcept { return cfg_.maxCompactionLagMs; }
    /// CP-8: per-topic segment roll threshold used as coalescing group bound.
    [[nodiscard]] int64_t     segmentRollBytes()       const noexcept { return cfg_.segmentRollBytes; }
    // CP-9: retention scheduler + async-delete config.
    /// How often (ms) the cleaner runs retention. Default 5 min.
    [[nodiscard]] int64_t     retentionCheckMs()       const noexcept { return cfg_.retentionCheckMs; }
    /// Delay (ms) before a .deleted file is physically removed. Default 1 min.
    [[nodiscard]] int64_t     fileDeleteDelayMs()      const noexcept { return cfg_.fileDeleteDelayMs; }
    /// Log-start-offset: segments entirely below this are deleted. 0 = disabled.
    [[nodiscard]] int64_t     logStartOffset()         const noexcept { return cfg_.logStartOffset; }
    /// CP-2: offset up to which the log has already been compacted (cleaned watermark).
    /// Initially 0; advanced by runBoundedCompaction after each segment is cleaned.
    /// Atomic so test threads can poll without a TSan race (cleaner/duty threads write it).
    [[nodiscard]] Offset lastCleanOffset() const noexcept {
        return lastCleanOffset_.load(std::memory_order_relaxed);
    }
    /// CP-2: advance the cleaned watermark (called by NodeRuntimeAgent after each segment).
    void setLastCleanOffset(Offset o) noexcept {
        lastCleanOffset_.store(o, std::memory_order_relaxed);
    }

    /// True if this partition is backed by an in-memory ring (InMemoryLog);
    /// false if backed by an on-disk Log.  Fixed at construction — a partition's
    /// backend never changes.  Disk-only consumers (LogCleanerAgent, retention,
    /// NodeRuntimeAgent drains, PartitionFailoverCoordinator) skip memory-backed
    /// partitions via this predicate.
    [[nodiscard]] bool isMemoryBacked() const noexcept { return memoryBacked_; }
    /// The InMemoryLog of a memory-backed partition (nullptr when disk-backed).
    /// Owner thread only (the producer-state rebuild of the owning worker).
    [[nodiscard]] rune::storage::InMemoryLog* memoryLog() const noexcept {
        return memoryBacked_ ? memLog_ : nullptr;
    }
    [[nodiscard]] bool isDiskBacked()   const noexcept { return !memoryBacked_; }

    /// Shard mode (spec §3.1 never-block): this replica never fsyncs inline.
    /// The time-gated follower flush, the direct append() (txn markers) and the
    /// reconcile append become page-cache writes plus a flush REQUEST that the
    /// owning worker submits to its io backend; the disk log's roll seal is
    /// deferred to the worker too (Log::setDeferSealSync).  Set before the
    /// replica is driven; legacy = false (inline fsyncs, unchanged).
    /// Task 10 fix 3: registry identity (set once by ReplicaRegistry::add on the
    /// control thread, before any worker learns of the replica).  (0, 0) =
    /// unregistered: ops for it are not ownership-gated.
    void setRegistryHandle(uint32_t id, uint32_t incarnation) noexcept {
        registryId_ = id; incarnation_ = incarnation;
    }
    [[nodiscard]] uint32_t registryId()  const noexcept { return registryId_; }
    [[nodiscard]] uint32_t incarnation() const noexcept { return incarnation_; }

    /// Worker-driven user replicas (every placement, F30): the follower flush,
    /// leader append / txn-marker flushes and roll seals become requests the
    /// owning worker submits to its io backend — never an inline fsync.
    void setExternalFollowerFlush(bool on) noexcept {
        externalFollowerFlush_ = on;
        if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->setDeferSealSync(on);
    }
    [[nodiscard]] bool externalFollowerFlush() const noexcept { return externalFollowerFlush_; }
    /// Leader-epoch checkpoint writes (the one accepted synchronous disk write
    /// on a shard loop — see checkpointEpochs_).
    [[nodiscard]] uint64_t epochCheckpointWritesForTest() const noexcept { return epochCheckpointWrites_; }
    /// Worker side: consume a pending flush request (true once per request).
    [[nodiscard]] bool takeFollowerFlushRequest() noexcept {
        const bool r = followerFlushRequested_;
        followerFlushRequested_ = false;
        return r;
    }

    /// Task 9: test accessor — exposes the internal epoch cache for restart-
    /// durability tests.  Disk-backed only (memory-backed partitions have no
    /// checkpoint; their epoch cache is always rebuilt from scratch on restart).
    /// Single-writer; do not call from the cleaner/test threads concurrently.
    [[nodiscard]] const rune::storage::LeaderEpochCache& leaderEpochCacheForTest() const noexcept {
        return epochCache_;
    }

    /// Task 11: test accessor — exposes the internal ProducerStateManager for
    /// PSM re-rebuild-on-truncation tests.  Single-writer; do not call from
    /// the cleaner/test threads concurrently.
    [[nodiscard]] rune::storage::ProducerStateManager& producerStateManagerForTest() noexcept {
        return psm_;
    }
    [[nodiscard]] const rune::storage::ProducerStateManager& producerStateManagerForTest() const noexcept {
        return psm_;
    }

    /// F3 fix-round-2, item A.3 test accessor: drive the private pushToFollowers()
    /// directly, so a test can control fromOffset/pushCeiling precisely without
    /// needing GR-10's real coalesced-push cadence.
    /// F27: `fromOffset` is ignored — every follower is pushed from its own cursor.
    void pushToFollowersForTest(Offset /*fromOffset*/, Offset pushCeiling = kNoPushCeiling) noexcept {
        (void)pushToFollowers(pushCeiling);
    }

    /// F3 round 5, item I2 test accessors: how many times the "batch exceeds the
    /// transport's hard ceiling" condition was EVALUATED (each one is a log read
    /// + parse), and how many critical log lines it actually EMITTED.
    [[nodiscard]] uint64_t unsendableBatchEvaluationsForTest() const noexcept {
        return unsendableBatchLogCount_;
    }
    [[nodiscard]] uint64_t unsendablePushLogEmitsForTest() const noexcept {
        return unsendablePushLogEmits_;
    }
    /// F3 round 6: FETCH responses that carried records (Data serves) — with
    /// push on, a non-zero count means the follower had to be re-served
    /// (lost/dropped push → 20 ms resend window).
    [[nodiscard]] uint64_t fetchDataServesForTest() const noexcept { return fetchDataServes_; }
    /// F27: replication sends this replica had REFUSED by its transport (pushes,
    /// fetch answers, follower fetches/acks) — each one was retried next pass.
    [[nodiscard]] uint64_t sendRefusalsForTest() const noexcept { return sendRefusals_; }
    /// Test view of the push flow window (== kPushFlowWindow, asserted below).
    static constexpr Offset kPushFlowWindowForTest = 16384;
    /// F27 F7: times an owed Data answer's whole-batch walk was (re)built.
    [[nodiscard]] uint64_t owedAnswerRebuildsForTest() const noexcept { return owedRebuilds_; }
    /// The leader's tracked LEO for `follower` (-1 = none) — B1 SET semantics.
    [[nodiscard]] Offset followerLeoForTest(NodeId follower) const noexcept {
        const auto& m = leaderState_.followerLeos();
        const auto it = m.find(follower);
        return it == m.end() ? -1 : it->second;
    }

    /// Non-owning pointer to the underlying disk Log (for cleaner / compaction /
    /// failover code that needs concrete Log methods).  Safe ONLY when this
    /// replica is disk-backed — asserts !memoryBacked_.  Callers MUST gate on
    /// isDiskBacked() (memory-backed partitions have no on-disk Log).
    [[nodiscard]] rune::storage::Log* log() const noexcept {
        assert(!memoryBacked_ && "log(): backend is in-memory — gate on isDiskBacked()");
        return diskLog_;
    }

    /// The durability mode configured for this partition (Task M1).
    [[nodiscard]] DurabilityMode durability() const noexcept { return cfg_.durability; }

    /// E3 Task 5: async-durability tier (0=Leader, 1=Majority, 2=Strict).
    [[nodiscard]] uint8_t durabilityTier() const noexcept { return cfg_.durabilityTier; }

    /// ISR as seen by the leader (empty for follower).
    [[nodiscard]] const std::vector<NodeId>& isr() const noexcept;

    // F18 (cumulative, never reset — exported for ops/tests).
    /// Leader: fetches answered kErrRecordTooLarge (first batch > one frame).
    [[nodiscard]] uint64_t recordTooLargeServed() const noexcept { return recordTooLargeServed_; }
    /// Follower: times this replica wedged on an unreplicable batch.
    [[nodiscard]] uint64_t wedgedRefusals() const noexcept { return wedgedRefusals_; }
    /// Follower: the offset it is wedged at (stops fetching it), or -1.
    [[nodiscard]] Offset wedgedAtOffset() const noexcept { return wedgedAtOffset_; }
    /// F18 (B-1): this node's transport could not hand the follower the batch
    /// whose baseOffset is `batchBase` (it cannot fit one worker-ingress frame on
    /// its own).  Wedges ONLY if that batch is the one at our LEO and the answer
    /// is from the current (or a newer) epoch — a multi-batch prefix, a push from
    /// further ahead or a duplicate never wedges.
    void onUnapplicableBatch(Offset batchBase, Epoch leaderEpoch);

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Shared construction logic for both backend overloads (disk / in-memory).
    /// The pointer fields + memoryBacked_ are set in each constructor's init list;
    /// this seeds the leader/follower state, epoch cache, and cleaner snapshots.
    void initReplica(std::vector<NodeId> replicas, std::vector<NodeId> isr);

    /// (Leader) Recompute HW from ReplicaLeaderState and propagate to leader ISR.
    void recomputeHw();

    /// Compute the served committed high-watermark WITHOUT mutating any state.
    ///
    /// This is the pure, side-effect-free core of highWatermark() (no write to
    /// the mutable lastHealthyHw_ floor).  Both highWatermark() (which then also
    /// advances lastHealthyHw_) and publishCleanerState() call it.  Duty-thread
    /// only (reads leaderState_/isrManager_ non-atomically).
    [[nodiscard]] Offset computeServedHw() const noexcept;

    /// Publish the current served HW + role into the cleaner-visible atomics
    /// (release-store).  MUST be called on the DUTY/append thread only, at every
    /// site that can change the served HW, the ISR, or the
    /// role.  The LogCleanerAgent reads these via cleanerHighWatermark() /
    /// cleanerRole() (acquire-load) so it never touches the live racy state.
    void publishCleanerState() noexcept;
    /// CRITICAL-1: forget every published follower position (new leadership /
    /// demotion) — the controller snapshot then carries self only until a
    /// follower fetches (validated) in this leadership.
    void clearPublishedFollowerPositions_() noexcept;

    /// (Leader) Propagate current ISR into ReplicaLeaderState.
    void syncIsrToLeaderState();

    /// (Follower) Truncate the log to `truncateOffset` (inclusive last valid
    /// offset = truncateOffset - 1).  Updates local LEO.
    void truncateLog(Offset truncateOffset);

    /// (Leader) GR-10: push records to all ISR followers via unsolicited
    /// FetchResponse.  Non-blocking (tryClaim/offer).  F27: each follower is
    /// pushed from ITS OWN cursor (FollowerSlot::pushCursor) and its cursor
    /// advances only when the transport ACCEPTED the push to it — a refused push
    /// is re-sent on the next pass (time-independent), followers that got it are
    /// not re-sent.  Builds the FetchResponse via makeFetchResponse() (zero-copy
    /// records view).  Only called when cfg_.pushOnAppend is true.
    // Flow control: the leader never pushes more than kPushFlowWindow records ahead
    // of the slowest ISR follower's acked LEO.  Outrunning the follower makes Aeron
    // back-pressure and DROP best-effort pushes → the follower stalls behind the gap
    // (the acks=all "push outran follower" collapse).  Bounded in-flight throttles the
    // leader to the follower's apply rate instead → stable.  Well under an Aeron term
    // buffer's record capacity so a full window never overflows the publication.
    static constexpr Offset kPushFlowWindow  = 16384;
    static_assert(kPushFlowWindow == kPushFlowWindowForTest);
    static constexpr Offset kNoPushCeiling   = (1LL << 62);
    /// Push [cursor_f, min(LEO, pushCeiling)) to each ISR follower f (whole-batch
    /// aligned, capped at the frame).  `floor` = pushFloor_() when the caller
    /// already has it (doWork, F9), else kFloorUnknown (computed here).  An ISR
    /// follower without a slot cannot exist (the ISR is within the replica set the
    /// slots are built from) and is skipped.
    /// Returns the number of pushes the transport ACCEPTED (doWork's work count).
    static constexpr Offset kFloorUnknown = -1;
    int pushToFollowers(Offset pushCeiling = kNoPushCeiling, Offset floor = kFloorUnknown) noexcept;
    /// F27: the lowest push cursor over the ISR followers (LEO when there are none)
    /// — the derived replacement of the former shared lastPushedOffset_.
    [[nodiscard]] Offset pushFloor_() const noexcept;
    /// F27: re-send fetch answers the transport refused (FollowerSlot::owed).
    /// Never refreshes ISR liveness (only a real fetch does).  Returns sends made.
    int retryOwedFetchAnswers_() noexcept;
    /// F27: send a fetch answer to `slot`'s follower; on refusal record it as owed
    /// and forget it as served (never conflate against an unsent answer).
    bool sendFetchAnswer_(FollowerSlot* slot, NodeId follower, const FetchResponseMsg& resp,
                          Offset fetchOffset, int32_t maxBytes, bool data, Offset dataEnd);
    /// F27 F3: DR sends that are OWED when refused and re-sent next pass.
    void sendEpochAnswer_(NodeId to, const OffsetsForLeaderEpochRespMsg& m);
    /// F27 F3: re-send owed DR frames (any role).  Returns sends accepted.
    int retryOwedDrSends_() noexcept;

    /// F3 round 5, item I2: true while the push head is a remembered UNSENDABLE
    /// batch and nothing that could change the outcome (LEO, ISR, follower
    /// position via pushCeiling, epoch) has changed — doWork() then skips the
    /// push entirely (no readView/parse, no work count).  One compare when unarmed.
    [[nodiscard]] bool unsendablePushSuppressed_(Offset pushCeiling, Offset floor) const noexcept;
    /// Order-sensitive signature of the current ISR (cold: only computed while
    /// an unsendable-push memo is armed or being armed).
    [[nodiscard]] uint64_t isrSignature_() const noexcept;
    // Spec §3.5.2: follower FetchRequests are unicast to the known leader.
    /// F27: true iff the transport accepted the request.
    bool sendFetchToLeader_(const FetchRequestMsg& req);

    /// (Follower) KIP-279 truncation on a DivergingEpoch answer, then re-fetch.
    void onDivergingEpoch_(int32_t divergingEpoch, Offset divergingEndOffset);
    /// (Follower) Kafka LeaderEpochFileCache.assign on append: record the start
    /// of every new epoch stamped on the batches in [p, p+len) (whole batches).
    /// Returns true when the cache changed.
    bool assignEpochsFromBatches_(const uint8_t* p, std::size_t len);
    /// Cold: rebuild the epoch cache from every stamped batch in the log.
    void rebuildEpochCacheFromLog_();

    /// Build a FetchResponseMsg stamped with this partition's identity
    /// (epoch/partitionId/topicId, logStartOffset=0) plus the given HW,
    /// errorCode and records.  ZERO-COPY: `records` is a view (into the log's
    /// read mmap, or an empty span for an error/HW-only reply) that the caller
    /// MUST keep alive until transport_->sendFetchResponse()/sendReconcileResponse()
    /// returns — both encode synchronously into the outbound ring before
    /// returning.  Replaces the old encode-into-encodeScratch_ → decode →
    /// re-encode round-trip (one full record-bytes copy + a decode parse per
    /// replication frame).
    FetchResponseMsg makeFetchResponse(Offset highWatermark, int16_t errorCode,
                                       std::span<const uint8_t> records) const noexcept {
        FetchResponseMsg r{};
        r.leaderEpoch    = epoch_;
        r.partition      = cfg_.partitionId;
        r.highWatermark  = highWatermark;
        r.logStartOffset = 0;
        r.errorCode      = errorCode;
        r.topicId        = cfg_.topicId;
        r.records        = records;
        return r;
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    PartitionConfig      cfg_;
    Role                 role_;
    Epoch                epoch_;
    NodeId               leaderId_;           ///< (follower) leader's NodeId; -1 if unknown

    // De-virtualized log seam (Milestone D Phase 2).  Exactly ONE of diskLog_ /
    // memLog_ is non-null; memoryBacked_ (fixed at construction) selects which.
    // Every hot-path log op branches on memoryBacked_ to the concrete pointer and
    // calls the concrete (inlinable) method — a perfectly-predicted branch to two
    // direct call targets, NO vtable.  Both pointers are non-owning (the owner is
    // the test/container: BrokerNode::memoryLogs_ or TopicRegistry's Log).
    rune::storage::Log*         diskLog_ = nullptr;  ///< non-null iff !memoryBacked_
    rune::storage::InMemoryLog* memLog_  = nullptr;  ///< non-null iff  memoryBacked_
    bool                        memoryBacked_ = false;
    PartitionTransport*         transport_;   ///< non-owning
    using PerWorkerTransport = WorkerReplicationTransportImpl<AeronPubRegistry>;
    TransportKind       tkind_     = TransportKind::Generic;  ///< fixed at construction
    PerWorkerTransport* perWorker_ = nullptr;                 ///< non-null iff tkind_ == PerWorker
    AeronPartitionTransport* aeronT_ = nullptr;               ///< non-null iff tkind_ == Aeron
    NullPartitionTransport*  nullT_  = nullptr;               ///< non-null iff tkind_ == Null
    /// Apply f to the concrete transport: one predicted branch, direct call on the
    /// PerWorker arm.  Defined in PartitionReplica.cpp (its only user).
    /// F3 fix-round-2: returns auto (was void) so callers can also use this to
    /// QUERY the transport (e.g. maxFetchResponseBytes()), not just send.
    template <class F> auto onTransport(F&& f);

    // -----------------------------------------------------------------------
    // De-virtualized hot-path log dispatch.
    //
    // onLog(F) applies F to the ACTIVE concrete log.  The single branch on
    // memoryBacked_ is emitted once per call site; both arms are direct calls to
    // inlinable concrete methods (Log:: / InMemoryLog::) — no indirect/virtual
    // dispatch.  All PartitionReplica log operations route through these two
    // helpers (onLog for the general case, logEndOffset_ for the hottest read).
    // -----------------------------------------------------------------------
    template<class F>
    [[gnu::always_inline]] inline auto onLog(F&& f) {
        if (memoryBacked_) return f(*memLog_);
        return f(*diskLog_);
    }
    template<class F>
    [[gnu::always_inline]] inline auto onLog(F&& f) const {
        if (memoryBacked_) return f(*memLog_);
        return f(*diskLog_);
    }
    [[gnu::always_inline]] inline Offset logEndOffset_() const noexcept {
        return memoryBacked_ ? memLog_->logEndOffset() : diskLog_->logEndOffset();
    }

    // Leader-side components
    ReplicaLeaderState   leaderState_;        ///< HW + per-follower LEO
    IsrManager*          isrManager_ = nullptr; // heap — allocated on becomeLeader
    std::unique_ptr<IsrManager> isrManagerOwned_; ///< ownership wrapper

    // The full replica set (RF) this partition was assigned at construction.
    // Captured once in initReplica() so a follower-first replica retains its
    // replica-set knowledge across a later becomeLeader().  Without this, a
    // node elected leader with a leader-only ISR would build its IsrManager with
    // replicas == isr (a single member), making maybeExpandIsr() unable to ever
    // re-admit the other replicas → ISR permanently pinned at 1 → MinIsrViolation
    // on acks=all for that partition (observed on multi-partition topics whose
    // election handed a leader-only ISR).
    std::vector<NodeId>  configuredReplicas_;
    // D3b fix round 1: each assigned replica's fetch position in THIS leadership
    // (parallel to configuredReplicas_; reset to 0 on becomeLeader — a position
    // learned in an earlier term may predate a truncation).  Feeds
    // cleanerMarkerRemovalBound().  Duty/owning thread only; no allocation after
    // initReplica (linear search over <= FollowerSlots::kMaxReplicas).
    std::vector<Offset>  replicaFetchPos_;
    void noteReplicaFetchPos_(NodeId id, Offset pos) noexcept {
        for (std::size_t i = 0; i < configuredReplicas_.size(); ++i)
            if (configuredReplicas_[i] == id) { replicaFetchPos_[i] = pos; return; }
    }

    // Epoch caches (both roles need one for truncation)
    rune::storage::LeaderEpochCache epochCache_;

    // Task 11: a replica-held producer dedup state.  NOT the broker's: no
    // produce is validated against it (the owning worker's psm_ and the
    // gateway's inline state are — each proven consistent with this log, see
    // RcState / ClientGateway::proveInlinePsm_).  Only tests rebuild it; then
    // truncateLog re-rebuilds it when a KIP-101 truncation lands below the last
    // rebuild bound.  Single-writer; no locks.
    rune::storage::ProducerStateManager psm_;

    // Timing state
    int64_t lastIsrCheckNs_  = 0;   ///< last ISR shrink/expand timestamp
    int64_t lastFetchEmitNs_ = 0;   ///< last follower fetch emission timestamp


    // Reused batch-framing state (control-plane append(span)/applyReplicated
    // paths).  Held across calls so RecordBatchBuilder::reset()/buildInto()
    // retain capacity → zero steady-state heap allocations per framed batch.
    // Single-writer per replica, so no synchronisation is needed.
    std::unique_ptr<rune::protocol::RecordBatchBuilder> batchBuilder_;
    std::vector<uint8_t>                                batchScratch_;

    /// Frame `payload` as a single-record RecordBatch into batchScratch_ (reusing
    /// batchBuilder_).  Returns a reference valid until the next call; consume it
    /// synchronously.  See the definition in PartitionReplica.cpp.
    const std::vector<uint8_t>& makeBatch_(Offset baseOffset,
                                           std::span<const uint8_t> payload);

    // Follower HW (leader uses leaderState_.highWatermark())
    Offset followerHw_ = 0;

    // ── Follower replication sync — SINGLE SOURCE OF TRUTH ──────────────────────
    // One explicit state machine (see FollowerSync.h) replaces the former scattered
    // flags (heardFromLeader_ / lastResponseEmpty_ / lastKnownLeaderHw_ /
    // leoAtLastPeriodicFetch_ / dataFetchInFlight_ / dataFetchSentNs_).  Governs ONLY
    // the normal follower fetch path (a DivergingEpoch answer resets it)
    // (and keeps its own lastFetchEmitNs_ cadence).  Fast-acks (on apply) are pure
    // (maxBytes=0) and independent of this machine — they advance the leader's
    // follower-offset → HW for acks=all.
    FollowerSync followerSync_;
    // (Caught-up keepalive cadence is computed in doWork from replicaLagMaxMs so it
    //  always keeps the follower in the ISR — see the follower fetch-emit path.)
    int lastLoggedIsrSize_ = -1;   ///< change-gated ISR diagnostic (RUNE_FS_LOG)
    int64_t lastKnownNowNs_ = 0;   ///< most-recent doWork() clock, for message handlers
    bool followerApplyDirty_ = false;  ///< follower applied via appendNoSync since last flush
    int64_t lastFollowerSyncNs_ = 0;   ///< last coalesced follower fsync (time-gated)
    bool externalFollowerFlush_ = false;   ///< worker-driven (every placement): worker submits the flush (async)
    uint32_t registryId_  = 0;             ///< ReplicaRegistry dense id (0 = unregistered)
    uint32_t incarnation_ = 0;             ///< never-reused registration incarnation
    uint64_t logGeneration_ = 0;           ///< F15: bumped on truncation
    Offset   lastTruncationOffset_ = 0;    ///< F15: cut point of the last generation bump
    uint64_t leadershipGeneration_ = 0;    ///< F15 S4: bumped on every Leader <-> Follower transition
    uint64_t epochCheckpointWrites_ = 0;   ///< see checkpointEpochs_
    void checkpointEpochs_();
    bool followerFlushRequested_ = false;  ///< raised by doWork, consumed by the worker
    // Per-second rate counters (RUNE_FS_LOG diagnostic) to localise the acks=all
    // throughput bottleneck: appends (LEO growth), applies (follower push-apply),
    // pushes sent, fast-acks received, HW advance.
    int64_t rtLastNs_ = 0;
    int64_t rtAppends_ = 0, rtApplies_ = 0, rtPushes_ = 0, rtFastAcks_ = 0;
    int64_t rtApplyBytes_ = 0, rtPushBytes_ = 0;
    // F18 unreplicable-batch state (see kErrRecordTooLarge).
    uint64_t recordTooLargeServed_ = 0;
    uint64_t wedgedRefusals_       = 0;
    Offset   wedgedAtOffset_       = -1;
    void wedgeAt_(Offset at, const char* why);   ///< F18: enter the wedged state (log + count)
    Offset  rtLastHw_ = 0, rtLastLeo_ = 0;

    // F3 fix-round-2, item A: count of "batch exceeds even this transport's
    // hard ceiling" events (onFetchRequest or pushToFollowers), for
    // rate-limited critical logging (first occurrence, then every 1024th) —
    // same convention as LogSegment::readMapFailures_.
    uint64_t unsendableBatchLogCount_ = 0;
    // F3 round 5, item I2: critical log lines actually emitted by the push path
    // (rate-limited to once per unsendable-offset transition).
    uint64_t unsendablePushLogEmits_ = 0;
    uint64_t fetchDataServes_ = 0;   // F3 r6: Data fetch responses with records
    // F3 round 5, item I2: memo of the last unsendable push head.  Armed
    // (fromOffset >= 0) by pushToFollowers() when the head batch exceeds the
    // transport's hard ceiling; cleared by any successful push.
    struct UnsendablePushMemo {
        Offset   fromOffset  = -1;   ///< pushFloor_() at the failed push (-1 = unarmed)
        Offset   leo         = -1;
        Offset   pushCeiling = -1;
        uint64_t isrSig      = 0;
        Epoch    epoch       = 0;
    };
    UnsendablePushMemo unsendablePush_{};
    Offset             unsendablePushLoggedOffset_ = -1;   ///< last offset logged critical

    // CP-2: cleaned watermark — the log offset up to which compaction has already
    // been applied.  Starts at 0 (nothing compacted).  NodeRuntimeAgent advances
    // this after each successful segment compaction so already-clean prefixes are
    // never re-scanned or re-rewritten on the next duty-cycle call.
    // Atomic: cleaner/duty threads write; test threads may poll it concurrently.
    std::atomic<Offset> lastCleanOffset_{0};

    // GR-10 push tracking is PER FOLLOWER (F27): FollowerSlot::pushedThrough /
    // pushCursor(); the old shared lastPushedOffset_ advanced even when a push to
    // one follower was refused, so that follower's data waited for a timer.
    uint64_t sendRefusals_ = 0;   // F27: transport-refused replication sends (retried)
    uint64_t owedRebuilds_ = 0;   // F27 F7: owed Data answer rebuilds (log walk)
    // Durability: effective minIsr for the acksAll produce gate.
    // Majority mode: cfg_.minIsr.
    // AllInSync  mode: replicas.size() (== RF), so every replica must be in ISR.
    // Computed once in becomeLeader and used in append/appendKeyed.
    int effectiveMinIsr_ = 1;

    // DR-BUG-6: on becomeLeader the follower-lag TIME clock must be reset to the
    // election time so a newly-elected leader does not evict every follower from
    // the ISR on its first ISR check (their lastFetchTimeNs is 0 → instantly
    // "time-lagged").  We defer the reset to the first leader doWork(nowNs) since
    // becomeLeader has no clock.  See IsrManager::resetLagClock() for the full
    // rationale (this eviction is what disabled the reverse-reconcile in
    // production and caused the DR-8 committed-loss).
    bool leaderLagClockNeedsReset_ = false;

    // Kafka model (user decision 2026-10-03): HW = min LEO over the (maximal)
    // ISR — every ISR member holds every committed record, and a leader is
    // always elected from the ISR (user partitions: the controller elects from
    // its committed ISR; internal partitions: see updateIsr_ — their ISR only
    // drops members the cluster considers dead, so every live node the
    // consensus can elect is in it).  No leader-completeness gate exists.
    Offset electionLeo_       = 0;      ///< our LEO at becomeLeader (healthy-floor recovery bound)
    /// The controller-COMMITTED ISR (elections, min-ISR).  The HW counts the
    /// maximal ISR (isrManager_ = committed ∪ proposed) — see updateIsr_.
    std::vector<NodeId> committedIsr_;
    // AlterIsr proposal state (controller-managed ISR).  ONE proposal in flight
    // (Kafka): re-sent on cadence until the controller commits it or another
    // committed change supersedes it.  Each proposal carries its base
    // (= committedIsr_ when sent); the controller commits it only while that is
    // still the committed ISR (compare-and-set, Kafka's partitionEpoch check), so
    // a stale / re-ordered copy can never commit after a later change.  The
    // controller never refuses a well-formed proposal from the current leader
    // for good (only transiently: another change for the partition in flight),
    // so a pending proposal always resolves.  The HW counts committed ∪ pending
    // (KIP-497/841 maximal ISR).
    struct IsrProposal {
        bool                pending = false;
        std::vector<NodeId> isr;          ///< proposed ISR (AlterIsr)
        int64_t             sentNs  = 0;
        int32_t             basePartitionEpoch = -1;   ///< MEDIUM-1: the CAS version it was derived from
        int64_t             version = 0;               ///< internal quorum mode: the published version
    };
    IsrProposal isrProposal_{};
    /// MEDIUM-1: partitionEpoch of the committed assignment (-1 = unknown /
    /// not controller-managed).  Set by every becomeLeader.
    int32_t committedPartitionEpoch_ = -1;
    int64_t processIncarnation_      = 0;    ///< KIP-903 (see setProcessIncarnation)
    // Internal quorum mode (MAJOR-2/3, see setInternalIsrKnowledge).
    InternalIsrKnowledge* isrKnowledge_     = nullptr;
    bool     internalSeedPending_ = false;   ///< leadership seed (every replica) not yet replaced by a committed ISR
    int64_t  internalIsrVersion_  = 0;       ///< last version published in this leadership
    uint32_t internalIsrAcks_     = 0;       ///< slot bits of replicas holding the latest published version
                                             ///< (pending: commit quorum; committed: anti-entropy re-send until all ack)
    uint32_t internalFencedMask_  = 0;       ///< slot bits the cluster considers dead (shrinkIsrToLive)
    [[nodiscard]] bool internalQuorum_() const noexcept { return isrKnowledge_ != nullptr; }
    void publishInternalIsr_(int64_t nowNs);
    void maybeCommitInternalIsr_();
    [[nodiscard]] int slotIndex_(NodeId id) const noexcept;
    /// MEDIUM-2: after an INELIGIBLE reject, no ISR addition is proposed before
    /// this time (a fenced replica is not re-proposed every ISR check).
    int64_t expandBackoffUntilNs_ = 0;
    PartitionTransport* isrChannel_ = nullptr;   ///< AlterIsr sink (null = self-managed ISR)
    // Leader-epoch restart fence (see setLeaderEpochFenceFile).
    std::string fenceFile_;                  ///< durable highest-led-epoch file ("" = none)
    Epoch       restartFenceEpoch_ = -1;     ///< earlier incarnations' highest epoch (constant once driven)
    Epoch       fencePersisted_    = -1;     ///< highest epoch written to fenceFile_
    struct StaleLeadership {
        bool    pending        = false;      ///< a refused leadership awaits a new epoch
        Epoch   epoch          = -1;         ///< the refused (committed) leader epoch
        int32_t partitionEpoch = -1;         ///< its committed partitionEpoch
        int64_t sentNs         = 0;
        bool    sent           = false;
    };
    StaleLeadership staleLead_;
    uint64_t        staleLeadRefusals_ = 0;
    void refuseStaleLeadership_(Epoch newEpoch, int32_t partitionEpoch);
    int  requestLeaderEpochBump_(int64_t nowNs);
    void persistLedEpoch_(Epoch e);
    static constexpr int64_t kAlterIsrResendNs = 50'000'000LL;   // 50 ms
    std::array<uint8_t, ErrorPayload::SIZE> errPayload_{};      ///< DivergingEpoch / OOR answer payload
    uint64_t v2RefusalsLogged_ = 0;                             ///< mixed v2/v3: CRITICAL once
    [[nodiscard]] bool internalPartition_() const noexcept { return cfg_.topicId < 0; }
    [[nodiscard]] int  computeEffectiveMinIsr_(int rf) const noexcept;
    [[nodiscard]] bool committedIsrMeetsMinIsr_(bool acksAll) const noexcept;
    void applyCommittedIsr_(std::vector<NodeId> isr, int32_t partitionEpoch);
    bool updateIsr_(int64_t nowNs);
    /// Commit (self-managed) or propose (controller-managed) `desired` (an ISR
    /// derived from committedIsr_).  Returns true when something changed / was sent.
    bool changeIsr_(const std::vector<NodeId>& desired, int64_t nowNs, bool force = false);
    /// The maximal ISR = committed ∪ the pending proposal → isrManager_.
    void applyMaximalIsr_();
    void sendIsrProposal_(int64_t nowNs);


    // FETCH CONFLATION: per-follower record of the last (fetchOffset, LEO) served +
    // when.  The leader SKIPS re-serving an IDENTICAL response within kFetchResendNs.
    // A behind/stuck follower re-fetches the same range on every apply + every 5ms
    // periodic tick while its apply is still queued → without this, the leader
    // re-sends the same data N times → a flood of duplicate (already-applied) apply
    // ops that head-of-line-block the useful applies in the follower's ingress ring
    // (measured: 99% useless applies, 47-98ms follower control→worker handoff).
    // Conflating to ONE response per distinct state removes the flood; the timeout
    // re-serve preserves lost-push / lost-response recovery.  Cleared on role change.
    // Per-follower conflation + no-dup-fetch state (spec §3.5) — flat, slot-indexed.
    FollowerSlots followerSlots_;

    // DR-BUG-3: min-ISR-gated committed offset (Kafka min.insync.replicas
    // COMMIT semantics).  ReplicaLeaderState::computeHw() is the min LEO over
    // the ISR: when the ISR shrinks to a single member (the leader stranded in
    // a minority), the min over {leader} == the leader's own LEO, so the raw HW
    // jumps to offsets held by fewer than minIsr replicas.  That HW must NOT be
    // exposed as "committed" — neither to the acks=All drain nor to HW-bounded
    // subscribers (GR-6) — or a minority leader's records would be acked/
    // delivered and then truncated on failover (data-loss / phantom reads).
    //
    // lastHealthyHw_ is the last HW observed while the ISR was healthy
    // (|ISR| >= minIsr).  While |ISR| < minIsr, highWatermark() clamps to this
    // frozen floor so the effective committed offset cannot advance past the
    // last offset committed under a healthy ISR.  It is monotonic (only advances) and is
    // refreshed on every leader HW read/recompute while the ISR is healthy.
    mutable Offset lastHealthyHw_ = 0;

    // TSan race fix — cleaner/gateway-thread-visible snapshots.
    //
    // publishedHwForCleaner_:  the served committed HW, mirrored from
    //   highWatermark()/computeServedHw() by the duty thread via
    //   publishCleanerState() (release-store).  The LogCleanerAgent reads it via
    //   cleanerHighWatermark() (acquire-load) as its conservative compaction /
    //   retention upper bound.  Slightly-stale-but-safe: compaction/retention are
    //   always below a conservative HW.  Also read by the gateway thread via
    //   gatewayHighWatermark() (acquire-load) to avoid the R5/R6b race.
    // publishedRoleForCleaner_:  the role (as int), mirrored the same way.  The
    //   cleaner gates all work on cleanerRole()==Leader so it never reads role_.
    // publishedIsrSizeForGateway_:  the ISR size as last published by the duty
    //   thread.  The gateway reads it via gatewayIsrSize() (acquire-load) in
    //   drainPendingAcks() to avoid the live ISR vector race (Task #13).
    //   Slightly-stale-but-safe: a stale low count delays an acks=All drain by at
    //   most one publishCleanerState() cycle, never delivers a false OK.
    //
    // All three are the ONLY leader/HW/role/ISR state that cleaner/gateway threads
    // observe — no direct reads of role_, leaderState_, or isrManager_.
    std::atomic<Offset> publishedHwForCleaner_{0};
    std::atomic<int>    publishedRoleForCleaner_{static_cast<int>(Role::Follower)};
    std::atomic<int>    publishedIsrSizeForGateway_{0};
    // D3b: first unstable offset (LSO) for the cleaner — see publishCleanerLso().
    std::atomic<Offset> publishedLsoForCleaner_{cfg_.topicId >= 0 ? Offset{0} : kNoCleanerLso};
    // D3b fix round 1: min fetch position over every assigned replica — see
    // cleanerMarkerRemovalBound().  0 until the first leader publication.
    std::atomic<Offset> publishedMarkerBoundForCleaner_{0};

    // Controller position snapshot (see controllerReplicaLeo()/controllerLeaderEpoch()).
    // Per-node published LEO (stored as leo+1; 0 = unknown), indexed by nodeId.
    // Zero-initialised → every slot starts "unknown" with no constructor work.
    std::atomic<int32_t>                              publishedControllerEpoch_{-1};
    std::array<std::atomic<Offset>, kMaxControllerNodes> publishedReplicaLeo_{};
};

// ---------------------------------------------------------------------------
// InMemoryTransport — deterministic fake for tests
//
// Implements a simple in-memory message queue between a leader PartitionReplica
// and one or more follower PartitionReplicas. Call pump() to deliver all
// pending messages (requests to leader, responses to followers).
//
// Usage in a bounded test driver:
//   InMemoryTransport transport(leader, followers);
//   for (int i = 0; i < CAP && !done; ++i) {
//       clock += step;
//       leader.doWork(clock);
//       for (auto& f : followers) f.doWork(clock);
//       transport.pump();
//       done = check();
//   }
//   ASSERT_TRUE(done) << "stalled after " << CAP << " iterations";
// ---------------------------------------------------------------------------
class InMemoryTransport : public PartitionTransport {
public:
    struct PendingRequest {
        FetchRequestMsg req;
    };
    struct PendingResponse {
        NodeId          toFollower;
        FetchResponseMsg resp;
        std::vector<uint8_t> recordsBuf; // owns the bytes resp.records points into
    };

    // Register leader and followers after construction.
    void setLeader(PartitionReplica* leader) {
        leader_ = leader;
        // DR-BUG-6: also index the leader by NodeId so peer-routed messages
        // (OffsetsForLeaderEpoch responses, reconcile requests) can reach it.
        if (leader) byId_[leader->selfId()] = leader;
    }
    void addFollower(NodeId id, PartitionReplica* follower) {
        followers_[id] = follower;
        byId_[id] = follower;   // DR-BUG-6: index for peer-routed messages
    }
    /// Remove a follower from the transport (simulates network partition/crash).
    void removeFollower(NodeId id) {
        followers_.erase(id);
        byId_.erase(id);
    }

    // PartitionTransport implementation
    //
    // GR-10: sendFetchRequest and sendFetchResponse are guarded by a mutex so
    // that the leader push path (called from the produce thread) and the
    // pump() path (called from the replication duty-cycle thread) are safe to
    // call concurrently.  In single-threaded tests the lock is uncontended.
    bool sendFetchRequest(const FetchRequestMsg& req) override {
        enqueue_(pendingRequests_, {req});
        return true;
    }

    bool sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override {
        PendingResponse pr;
        pr.toFollower = toFollower;
        // Deep-copy records bytes so the response outlives the leader's scratch buf.
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        enqueue_(pendingResponses_, std::move(pr));
        return true;
    }

    // DR-BUG-6: OffsetsForLeaderEpoch + reconcile peer-routed messages.
    bool sendOffsetsForLeaderEpochRequest(
            NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req) override {
        enqueue_(pendingEpochReqs_, {toLeader, req});
        return true;
    }
    bool sendOffsetsForLeaderEpochResponse(
            NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp) override {
        enqueue_(pendingEpochResps_, {toFollower, resp});
        return true;
    }
    bool sendReconcileRequest(
            NodeId toPeer, const FetchRequestMsg& req) override {
        enqueue_(pendingReconcileReqs_, {toPeer, req});
        return true;
    }
    // MAJOR-2/3: internal-ISR state / acks, peer-routed (delivered by pump()).
    bool sendInternalIsr(NodeId to, const InternalIsrMsg& m) override {
        enqueue_(pendingInternalIsr_, {to, m});
        return true;
    }
    bool sendInternalIsrAck(NodeId to, const InternalIsrAckMsg& m) override {
        enqueue_(pendingInternalIsrAck_, {to, m});
        return true;
    }
    bool sendReconcileResponse(
            NodeId toPeer, const FetchResponseMsg& resp) override {
        PendingResponse pr;
        pr.toFollower = toPeer;
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        enqueue_(pendingReconcileResps_, std::move(pr));
        return true;
    }

    /// Deliver all pending messages: FetchRequests → leader, FetchResponses → followers.
    ///
    /// \param nowNs  Current wall-clock time passed to onFetchRequest() so the
    ///               leader records the follower's real fetch timestamp.  Defaults
    ///               to 0 for backward compatibility with tests that do not care
    ///               about time-lag eviction; real tests should supply the clock.
    ///
    /// Returns total messages delivered.
    int pump(int64_t nowNs = 0) {
        int count = 0;

        // Deliver fetch requests to the leader.
        std::vector<PendingRequest> reqs;
        take_(pendingRequests_, reqs);
        for (auto& pr : reqs) {
            if (leader_) {
                leader_->onFetchRequest(pr.req, nowNs);
                ++count;
            }
        }

        // Deliver fetch responses to followers.
        std::vector<PendingResponse> resps;
        take_(pendingResponses_, resps);
        for (auto& pr : resps) {
            auto it = followers_.find(pr.toFollower);
            if (it != followers_.end()) {
                it->second->applyReplicated(pr.resp);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch requests → target (the new leader).
        std::vector<PendingEpochReq> ereqs;
        take_(pendingEpochReqs_, ereqs);
        for (auto& e : ereqs) {
            auto it = byId_.find(e.to);
            if (it != byId_.end()) {
                it->second->onOffsetsForLeaderEpochRequest(e.req);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch responses → target (the follower).
        std::vector<PendingEpochResp> eresps;
        take_(pendingEpochResps_, eresps);
        for (auto& e : eresps) {
            auto it = byId_.find(e.to);
            if (it != byId_.end()) {
                it->second->applyOffsetsForLeaderEpochResponse(e.resp);
                ++count;
            }
        }

        // DR-BUG-6: reconcile requests → target (the ahead survivor).
        std::vector<PendingReconcileReq> rreqs;
        take_(pendingReconcileReqs_, rreqs);
        for (auto& r : rreqs) {
            auto it = byId_.find(r.to);
            if (it != byId_.end()) {
                it->second->onReconcileRequest(r.req);
                ++count;
            }
        }

        // DR-BUG-6: reconcile responses → target (the deficient leader).
        std::vector<PendingResponse> rresps;
        take_(pendingReconcileResps_, rresps);
        for (auto& pr : rresps) {
            auto it = byId_.find(pr.toFollower);
            if (it != byId_.end()) {
                it->second->applyReconcileResponse(pr.resp);
                ++count;
            }
        }

        // MAJOR-2/3: internal-ISR states / acks → target.
        std::vector<std::pair<NodeId, InternalIsrMsg>> iisr;
        std::vector<std::pair<NodeId, InternalIsrAckMsg>> iack;
        take_(pendingInternalIsr_, iisr);
        take_(pendingInternalIsrAck_, iack);
        for (auto& [to, m] : iisr) {
            auto it = byId_.find(to);
            if (it != byId_.end()) { it->second->onInternalIsr(m); ++count; }
        }
        for (auto& [to, m] : iack) {
            auto it = byId_.find(to);
            if (it != byId_.end()) { it->second->onInternalIsrAck(m); ++count; }
        }

        return count;
    }

    /// Number of in-flight messages (for test assertions).
    [[nodiscard]] std::size_t pendingCount() const noexcept {
        std::lock_guard lk(mu_);
        return pendingRequests_.size() + pendingResponses_.size()
             + pendingEpochReqs_.size() + pendingEpochResps_.size()
             + pendingReconcileReqs_.size() + pendingReconcileResps_.size();
    }

private:
    // Every pending queue is touched only under mu_ — through these two helpers
    // (test fake: the leader push path and pump() may run on different threads).
    template <class V> void enqueue_(V& v, typename V::value_type x) {
        std::lock_guard lk(mu_);
        v.push_back(std::move(x));
    }
    template <class V> void take_(V& from, V& into) {
        std::lock_guard lk(mu_);
        into.swap(from);
    }
    struct PendingEpochReq  { NodeId to; OffsetsForLeaderEpochReqMsg  req; };
    struct PendingEpochResp { NodeId to; OffsetsForLeaderEpochRespMsg resp; };
    struct PendingReconcileReq { NodeId to; FetchRequestMsg req; };

    PartitionReplica* leader_ = nullptr;
    std::unordered_map<NodeId, PartitionReplica*> followers_;
    std::unordered_map<NodeId, PartitionReplica*> byId_;   ///< DR-BUG-6: all replicas by id

    mutable std::mutex           mu_;              ///< guards all pending queues
    std::vector<PendingRequest>  pendingRequests_;
    std::vector<PendingResponse> pendingResponses_;
    std::vector<PendingEpochReq>  pendingEpochReqs_;   ///< DR-BUG-6
    std::vector<PendingEpochResp> pendingEpochResps_;  ///< DR-BUG-6
    std::vector<PendingReconcileReq> pendingReconcileReqs_;  ///< DR-BUG-6
    std::vector<PendingResponse>  pendingReconcileResps_;    ///< DR-BUG-6
    std::vector<std::pair<NodeId, InternalIsrMsg>>    pendingInternalIsr_;     ///< MAJOR-2/3
    std::vector<std::pair<NodeId, InternalIsrAckMsg>> pendingInternalIsrAck_;  ///< MAJOR-2/3
};

} // namespace rune::replication
