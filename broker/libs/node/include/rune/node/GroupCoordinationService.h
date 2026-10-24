#pragma once
// ---------------------------------------------------------------------------
// GroupCoordinationService.h — CG-3/CG-4: wires GroupCoordinator +
// ReplicatedOffsetStore into the broker duty cycle; translates group
// assignments into HW-bounded push subscriptions (CG-4).
//
// Namespace: rune::node
// C++23. Single-writer (doWork driven). No locks. No hot-path allocation.
//
// ## Responsibilities
//
//   - Per-group GroupCoordinator lifecycle: lazy get-or-create on first Join.
//   - doWork(nowNs): drives every GroupCoordinator::doWork; on
//     CompletingRebalance-transition → autoAssignAndSync; returns work count.
//   - CG-4: on Stable: for each member, seed push subscriptions (one per
//     assigned partition, resumed from committed offset or auto.offset.reset)
//     and send Assign control frames.  On PreparingRebalance: eagerly revoke
//     all group push subscriptions and send empty Assign (revoke) frames.
//   - Six request handlers (JoinGroup / SyncGroup / Heartbeat / LeaveGroup /
//     OffsetCommit / OffsetFetch): decode, act, encode response into a reused
//     caller-supplied buffer; return bytes written.
//   - NotCoordinator fence: when isLocalCoordinator()==false every handler
//     encodes a response with errorCode=NotCoordinator.
//
// ## Constructor deps (chosen for testability without a real bridge pointer)
//
//   - ReplicatedOffsetStore*  offsets  — non-owning; must outlive.
//   - TopicRegistry*          topics   — non-owning; for autoAssignAndSync
//                                        TopicPartitionCounts.  May be null
//                                        (autoAssignAndSync is skipped).
//   - std::function<bool()>   isLeader — leader query callback; called from
//                                        isLocalCoordinator() and every handler.
//                                        Default: always true (for tests).
//   - std::function<NodeId()> leaderId — current leader nodeId callback.
//                                        Default: returns 0.
//
// ## Zero-alloc behaviour
//
//   Handlers allocate nothing on the response-encode path; response buffers
//   are caller-supplied.  Per-handler scratch vectors (assignment results,
//   offset results) are members of GroupCoordinationService and pre-reserved
//   so steady-state is allocation-free.  New-group creation (coordinator map
//   insert + GroupCoordinator ctor) allocates once — cold path only.
//   CG-4: Assign encoding uses assignFrameScratch_ (pre-allocated 68 KiB
//   member buffer); no per-rebalance heap allocation.
// ---------------------------------------------------------------------------

#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/coordination/TxnOffsetApply.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/gateway/IGroupRequestHandler.h>

// Protocol codecs for response encoding scratch types.
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <map>
#include <string>
#include <vector>

// Forward declaration — ClientGateway lives in rune_gateway; node→gateway direction is fine.
namespace rune::gateway { class ClientGateway; }

namespace rune::node {

using NodeId = int32_t;   // mirrors rune::replication::NodeId
using GroupId = rune::coordination::GroupId;

// CG-4 fix: GroupCoordinationService implements IGroupRequestHandler so that
// ClientGateway can dispatch group messages through the interface without
// introducing a rune_gateway→rune_node symbol dependency.
class GroupCoordinationService : public rune::gateway::IGroupRequestHandler {
public:
    // -----------------------------------------------------------------------
    // Construction
    //
    // @param offsets   ReplicatedOffsetStore — non-owning; must outlive.
    // @param topics    TopicRegistry — non-owning, may be null.
    // @param isLeader  Callable → true when this node is the cluster leader.
    // @param leaderId  Callable → current cluster leader NodeId.
    // -----------------------------------------------------------------------
    GroupCoordinationService(
        rune::coordination::ReplicatedOffsetStore* offsets,
        rune::gateway::TopicRegistry*              topics,
        std::function<bool()>                      isLeader = [] { return true; },
        std::function<NodeId()>                    leaderId = [] { return NodeId{0}; });

    // Non-copyable, non-movable (holds per-group map + pre-reserved scratch).
    GroupCoordinationService(const GroupCoordinationService&)            = delete;
    GroupCoordinationService& operator=(const GroupCoordinationService&) = delete;

    // -----------------------------------------------------------------------
    // doWork — drive every GroupCoordinator::doWork; on CompletingRebalance
    // transition → autoAssignAndSync with AssignorType::Range.
    //
    // Returns sum of work counts (0 when idle in steady state).
    // Must be called from ONE thread only (duty-cycle thread).
    // No per-cycle allocation in steady state.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // setGateway — wire in the ClientGateway for CG-4 push-subscription
    // translation.  Must be called before doWork() for group push delivery.
    // May be null (doWork still drives rebalance but sends no Assign frames
    // and creates no push subscriptions — useful for tests that exercise
    // coordinator state without a real gateway).
    // Non-owning; lifetime must exceed the service.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setGateway(rune::gateway::ClientGateway* gateway) noexcept {
        gateway_ = gateway;
    }

    // -----------------------------------------------------------------------
    // E2 final fix wave (C1): committed offsets of TRANSIENT partitions are data
    // positions, and transient data does not survive its ring.  The lineage
    // provider answers the CURRENT data lineage of (topicId, partition) — a
    // non-zero token that changes exactly when the partition's ring data did not
    // survive (a restart) — or 0 when unknown / not transient (no stamp, no
    // check: the historical behaviour).  OffsetCommit stamps each entry with it;
    // OffsetFetch answers "no committed offset" (-1 — the member starts at the
    // partition's log start, never Latest) for an entry whose stamp differs from the current lineage,
    // so a member never resumes a refilled ring at a position of the old data
    // (silent loss of the first records after a restart).  A plain function
    // pointer + context (no allocation, no std::function on the commit path).
    // -----------------------------------------------------------------------
    //
    // C1 multi-node (USER DECISION option b): the lineage is committed metadata
    // confirmed per leadership.  While the partition's current leadership has
    // not confirmed it yet (kLineagePending — e.g. right after a full-cluster
    // restart), OffsetCommit and OffsetFetch answer that entry NotLeader: an
    // EXISTING retriable code the client retries with its bounded budget —
    // never a guess, never silence.
    static constexpr uint64_t kLineagePending = UINT64_MAX;
    using TransientLineageFn = uint64_t (*)(void* ctx, int32_t topicId, int32_t partition) noexcept;
    void setTransientLineage(TransientLineageFn fn, void* ctx) noexcept { lineageFn_ = fn; lineageCtx_ = ctx; }
    /// Phase 2b F Task 11: apply one consumer-group offset of a COMMITTED transaction (TxnOffsetCommit) - the same
    /// overwrite an OffsetCommit performs (replicated, acks-all, stamped with the partition's data lineage), but
    /// without a member generation: the transaction coordinator already fenced the producer.  Refused: this node is
    /// not the group coordinator / its offsets are not loaded yet / the lineage is pending / the append was refused
    /// (the caller counts and logs it - the offset is never dropped silently).
    /// #358: Applied only once the offset record is COMMITTED on the offsets replica (its high watermark passed
    /// it); until then Replicating.  The first call appends; a repeat for the same (group, tp, offset) waits on that
    /// record and appends again only when it may be gone (the offsets replica changed leadership or truncated).
    /// Control thread.
    /// `token` (PR #434 review F2): identifies the committing transaction (the coordinator passes one per
    /// transaction and load; a default token = an anonymous caller, test rigs).
    rune::coordination::TxnOffsetApply applyTransactionalOffset(const std::string& group,
                                                                const rune::coordination::TopicPartition& tp,
                                                                int64_t offset, int64_t nowNs,
                                                                rune::coordination::TxnOffsetToken token = {});
    /// PR #434 review: the transaction coordinator no longer waits for `token`'s offsets (it completed elsewhere,
    /// unloaded, or dropped the transaction): its waiters leave.  Control thread.
    void releaseTransactionalOffsets(const rune::coordination::TxnOffsetToken& token);
    /// #358: transactional offset records appended and not yet known committed (test / observability).
    [[nodiscard]] std::size_t txnOffsetsReplicating() const noexcept { return txnOffsetsWaiting_; }
    /// #359: why the last applyTransactionalOffset was Refused (the transaction coordinator's stuck-commit WARN).
    [[nodiscard]] rune::coordination::TxnOffsetRefusal lastTxnOffsetRefusal() const noexcept { return lastRefusal_; }

    /// #125 (KIP-447): the member fence of a TxnOffsetCommit that carries a member block - exactly the OffsetCommit
    /// rules: NotCoordinator off the coordinator; OK for a current member of `group` bound to `sessionId` at the
    /// group's generation; IllegalGeneration for such a member at another generation; UnknownMember otherwise
    /// (another session's member is answered exactly as a member that does not exist).  Control thread.
    [[nodiscard]] rune::protocol::client::ErrorCode checkTxnOffsetMember(const std::string& group,
                                                                         const std::string& memberId,
                                                                         int32_t generationId, int64_t sessionId);

    /// #438: whether an unfinished transaction holds a (group, tp) (BrokerNode wires the transaction coordinator).
    /// Held / Unknown: OffsetFetch and plain OffsetCommit of that key answer UnstableOffsetCommit (NotCoordinator
    /// to a client below 10) - retriable.  No probe (rigs) = Free.  Plain function pointer + ctx.
    using TxnOffsetHoldProbe = rune::coordination::TxnOffsetHold (*)(void* ctx, const std::string& group,
                                                                     const rune::coordination::TopicPartition& tp);
    void setTxnOffsetHoldProbe(TxnOffsetHoldProbe fn, void* ctx) noexcept { holdProbe_ = fn; holdProbeCtx_ = ctx; }

    /// #433: plain OffsetCommit answers parked until their __consumer_offsets records commit (test / observability).
    [[nodiscard]] std::size_t parkedOffsetCommits() const noexcept { return parkedCount_; }
    /// Test seam: where a parked answer goes when no gateway is wired (production: ClientGateway::sendControlFrame).
    using AnswerSinkFn = void (*)(void* ctx, int64_t sessionId, const uint8_t* frame, std::size_t len);
    void setAnswerSinkForTest(AnswerSinkFn fn, void* ctx) noexcept { answerSink_ = fn; answerSinkCtx_ = ctx; }
    /// E2 residual round 5 (R1): a counter that moves whenever a committed
    /// lineage changes (MetadataStateMachine::lineageVersion).  On a change the
    /// coordinator sweeps its groups: a group holding a committed offset of a
    /// SUPERSEDED lineage (a new one was minted — the data it pointed into did
    /// not survive) is forced to rebalance, so every member re-fetches (→ -1,
    /// the log start) instead of consuming on from a position it fetched before
    /// the mint; a commit at the old generation is fenced (IllegalGeneration).
    using LineageVersionFn = uint64_t (*)(void* ctx) noexcept;
    void setTransientLineageVersion(LineageVersionFn fn, void* ctx) noexcept { lineageVersionFn_ = fn; lineageVersionCtx_ = ctx; }
    [[nodiscard]] uint64_t supersededLineageRebalances() const noexcept { return supersededRebalances_; }
    [[nodiscard]] uint64_t supersededLineageCommitsFenced() const noexcept { return supersededCommitsFenced_; }
    /// Committed offsets answered "none" because their lineage ended (C1).
    [[nodiscard]] uint64_t staleLineageOffsets() const noexcept { return staleLineageOffsets_; }
    /// Entries answered NotLeader because the lineage was still pending (C1).
    [[nodiscard]] uint64_t pendingLineageAnswers() const noexcept { return pendingLineageAnswers_; }

    // -----------------------------------------------------------------------
    // isLocalCoordinator — true when this node is the cluster leader.
    // v1 seam: all groups are coordinated by the cluster leader.
    // Milestone-D will add per-group routing.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLocalCoordinator() const;

    // Task 11 fix 2 (B1): this node's leadership for the current runtime duty
    // cycle, pushed as a VALUE by NodeRuntimeAgent (same thread) right after
    // consensus runs.  Once pushed, isLocalCoordinator() never calls the
    // isLeader std::function (per-cycle, worker 0's pass under fused placement).
    // Unpushed (-1): the callable is used (service-only tests).
    void setLocalLeadership(bool leader) noexcept { localLeader_ = leader ? 1 : 0; }

    // #570: the coordinator term (the cluster leadership term) the loaded groups belong to.  A leadership lost and
    // regained between two duty cycles is invisible to the isLocalCoordinator() sample, but it always changes the
    // term: doWork and every membership handler unload the groups when the term moved since they were loaded
    // (Kafka: a new coordinator epoch rebuilds its groups; members re-join).  No fn: the term never changes (rigs).
    // Cold setter; control thread.
    void setCoordinatorTermFn(std::function<int64_t()> fn) { termFn_ = std::move(fn); }

    // -----------------------------------------------------------------------
    // coordinatorFor — returns the cluster-leader nodeId for any group.
    // v1 seam (Milestone-D: will map groupId → owning node).
    // -----------------------------------------------------------------------
    [[nodiscard]] NodeId coordinatorFor(const GroupId& groupId) const;

    // -----------------------------------------------------------------------
    // coordinator — get-or-create the GroupCoordinator for a group.
    // Cold path: first call per groupId inserts into the map (one allocation).
    // Subsequent calls are O(1) map lookup, no allocation.
    // -----------------------------------------------------------------------
    rune::coordination::GroupCoordinator& coordinator(const GroupId& groupId);

    // -----------------------------------------------------------------------
    // Handlers — decode the request from buf[0..len-1], act, encode the
    // response into out[] (reused by caller), return bytes written.
    //
    // When isLocalCoordinator()==false: encode the protocol response with
    // errorCode = NotCoordinator and return that size (no coordinator action).
    //
    // buf and out MUST not overlap.
    // outCap is the capacity of out[] in bytes; handlers bound-check BEFORE
    // writing each variable-length element so out[] is never overrun.
    // Fixed-size responses (JoinGroup, Heartbeat, LeaveGroup) fit in any
    // reasonable scratch; variable-size ones (OffsetCommit, OffsetFetch)
    // cap the entry count to what fits.
    // Never throws.  Never allocates from heap in steady state.
    // -----------------------------------------------------------------------
    size_t handleJoinGroup   (const uint8_t* buf, size_t len, int32_t clientVersion, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleSyncGroup   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleHeartbeat   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleLeaveGroup  (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleOffsetCommit(const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleOffsetFetch (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    /// No-hang fix round 4 (item 8): every member tracked on `sessionId` leaves
    /// its group (subscriptions revoked, session mapping dropped).  Cold.
    void evictSessionMembers(int64_t sessionId, int64_t nowNs) override;
    /// Authentication Task 14: every member of `sessionId` whose group the check refuses leaves; issue #212: so does
    /// every member of `sessionId` subscribed to a topic `check.mayReadTopic` refuses (cold).
    int removeUnauthorizedMembers(int64_t sessionId, int64_t nowNs, GroupReadCheck check) override;
    /// Issue #212: the session was revoked (terminal) - every member tracked on it leaves its group now (cold).
    void removeSessionMembers(int64_t sessionId, int64_t nowNs) override;
    /// No-hang fix round 5 (N23): only the request's (group, member) on `sessionId`
    /// (memberId empty: this session's members of that group).  Cold.
    void evictMember(std::string_view groupId, std::string_view memberId, int64_t sessionId,
                     int64_t nowNs) override;

    // -----------------------------------------------------------------------
    // Catalog history Task 8 fix round 1 (I3): the catalog DROPPED the topic
    // `topicName` (control thread, the broker's drop effect).  Every group whose
    // current assignment holds a partition of it rebalances NOW (explicit for
    // the group: its members get the revoke + a new Assign without the dropped
    // partitions — buildTopicPartitionCounts no longer lists the topic).
    // Coordinator only; idempotent (a group already rebalancing is left as is).
    // Returns the number of groups rebalanced.  Cold.
    // -----------------------------------------------------------------------
    std::size_t onTopicDropped(std::string_view topicName, int64_t nowNs);

    /// The topic -> partition-count map a rebalance assigns from (test seam, cold).
    [[nodiscard]] rune::coordination::TopicPartitionCounts topicPartitionCountsForTest() const {
        return buildTopicPartitionCounts();
    }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build a TopicPartitionCounts map from the TopicRegistry for autoAssignAndSync.
    /// Cold path — called only on join-timeout transition.
    rune::coordination::TopicPartitionCounts buildTopicPartitionCounts() const;

    // CG-4: called from doWork on the PreparingRebalance entry for a group.
    // Revokes all group push subscriptions and sends empty Assign (revoke) to
    // every member session.
    void onEnterPreparingRebalance(const GroupId& groupId,
                                   rune::coordination::GroupCoordinator& coord);

    // CG-4: called from doWork after autoAssignAndSync succeeds (Stable).
    // Creates push subscriptions (resumed from committed offset) and sends
    // Assign frames with each member's new partition set.
    void onReachedStable(const GroupId& groupId,
                         rune::coordination::GroupCoordinator& coord,
                         int64_t nowNs);

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    rune::coordination::ReplicatedOffsetStore* offsets_;    // non-owning
    /// #358 / PR #434 review: the transactions waiting on their offset records of one (group, topic, partition),
    /// in APPEND order.  Cold path.  The order is the commit order, and it is what keeps the committed offset at the
    /// newest commit:
    ///   - a waiter's offset is safe once its own record, or any NEWER record of the key, is committed;
    ///   - only the newest waiter ever appends again (after the offsets replica's leadership or log changed and its
    ///     record may be gone); an older one waits for the newer record, so an older value never lands over a newer;
    ///   - a waiter leaves only when it is answered Applied or when the transaction coordinator releases it
    ///     (releaseTransactionalOffsets); never evicted - a full key refuses a new transaction instead (it retries).
    /// A plain OffsetCommit counts as the newest record of the key (it is newer than every waiter).
    struct TxnOffsetKey {
        std::string group, topic;
        int32_t     partition = 0;
        auto operator<=>(const TxnOffsetKey&) const = default;
    };
    struct TxnOffsetWaiter {
        rune::coordination::TxnOffsetToken token;
        int64_t  offset       = 0;
        int64_t  recordOffset = -1;   ///< its own record
        uint64_t leadershipGen = 0, logGen = 0;
    };
    struct TxnOffsetKeyState {
        std::vector<TxnOffsetWaiter> waiters;          ///< append order
        int64_t  lastRecord = -1;                      ///< the key's newest record (a waiter's or a plain commit's)
        uint64_t lastLeadershipGen = 0, lastLogGen = 0;
        bool     lastCommitted = false;                ///< observed committed (then it survives any change)
        bool     plainIsLast   = false;                ///< the newest record is a plain OffsetCommit
    };
    static constexpr std::size_t kMaxTxnOffsetWaiters = 64;   ///< per key; beyond: Refused (the coordinator retries)
    std::map<TxnOffsetKey, TxnOffsetKeyState> txnOffsetsInFlight_;
    rune::coordination::TxnOffsetRefusal lastRefusal_ = rune::coordination::TxnOffsetRefusal::Unknown;
    rune::coordination::TxnOffsetApply refuse_(rune::coordination::TxnOffsetRefusal why) noexcept {
        lastRefusal_ = why;
        return rune::coordination::TxnOffsetApply::Refused;
    }
    TxnOffsetHoldProbe holdProbe_    = nullptr;
    void*              holdProbeCtx_ = nullptr;
    AnswerSinkFn       answerSink_    = nullptr;
    void*              answerSinkCtx_ = nullptr;
    /// #438: the key's hold (no probe: Free).  partition < 0 asks for "any key of the group" (fetch-all).
    [[nodiscard]] rune::coordination::TxnOffsetHold holdOf_(const std::string& group,
                                                            const rune::coordination::TopicPartition& tp) const {
        return holdProbe_ != nullptr ? holdProbe_(holdProbeCtx_, group, tp) : rune::coordination::TxnOffsetHold::Free;
    }
    /// #438: the retriable code a held key is answered with on `sessionId`: UnstableOffsetCommit (client >= 10),
    /// NotCoordinator below.
    [[nodiscard]] int16_t unstableFor_(int64_t sessionId);
    uint64_t unstableAnswers_ = 0;

    // ----- #433: a plain OffsetCommit is answered only once its __consumer_offsets records are COMMITTED -----
    // An acks-all append replicates asynchronously: an OK on the local append is lost with a failover of the offsets
    // partition.  So an answer with an OK entry whose record the high watermark has not passed is PARKED - FIFO, in
    // record order - and released by doWork: OK as built once the high watermark passes its newest record under the
    // same leadership and log generation; any change (leadership / log generation, the replica stops leading, this
    // node stops coordinating) answers its OK entries NotCoordinator (retriable, the commit is idempotent) - never
    // an OK for a record that may be gone.  Preallocated: a fixed ring of parked answers and one of their results
    // (no steady-state allocation); when either is full the commit is refused NotCoordinator WITHOUT appending.
    struct ParkedCommit {
        int64_t  sessionId     = 0;
        int64_t  correlationId = 0;
        int64_t  maxRecordAt   = -1;   ///< the newest record of its OK entries
        uint64_t leadershipGen = 0, logGen = 0;
        uint32_t resultsAt     = 0;    ///< ring index of its first result
        uint32_t count         = 0;    ///< its results (the response's entries, in request order)
    };
    static constexpr std::size_t kMaxParkedCommits      = 256;
    static constexpr std::size_t kParkedResultsCapacity = 8192;
    /// The most results one parked answer may hold: the encoded answer fits the release frame (the gateway's
    /// one-frame answer bound).  header 24 + count 2, 10 B per result.
    static constexpr std::size_t kParkedFrameBytes = 68 * 1024;
    static constexpr std::size_t kMaxParkedResultsPerCommit = (kParkedFrameBytes - 26) / 10;
    static_assert(kMaxParkedResultsPerCommit <= kParkedResultsCapacity);
    std::array<ParkedCommit, kMaxParkedCommits> parked_{};
    std::size_t parkedHead_   = 0;
    std::size_t parkedCount_  = 0;
    std::vector<rune::protocol::client::OffsetCommitResult> parkedResults_;   ///< ring, sized once at construction
    std::size_t resultsHead_  = 0;
    std::size_t resultsUsed_  = 0;
    std::array<uint8_t, kParkedFrameBytes> parkedFrame_{};   ///< release encode buffer
    uint64_t parkRefusals_ = 0;
    [[nodiscard]] bool parkHasRoomFor_(std::size_t results) const noexcept {
        return parkedCount_ < kMaxParkedCommits && kParkedResultsCapacity - resultsUsed_ >= results;
    }
    void parkCommit_(int64_t sessionId, int64_t correlationId, int64_t maxRecordAt);
    int  releaseParkedCommits_();
    void sendDeferred_(int64_t sessionId, const uint8_t* frame, std::size_t n);
    using ClientVersionFn = int32_t (*)(void* ctx, int64_t sessionId) noexcept;
    int32_t         maxSessionTimeoutMs_ = kDefaultMaxSessionTimeoutMs;
    ClientVersionFn clientVersionFn_  = nullptr;
    void*           clientVersionCtx_ = nullptr;
public:
    /// #213: the upper bound on a JoinGroup's sessionTimeoutMs (Kafka's group.max.session.timeout.ms default).  A
    /// larger request is CLAMPED to it - the value also sets the group's rebalance timeout, so an unbounded one would
    /// let a single member stall every rebalance (and keep a dead member) for ~24 days.
    static constexpr int32_t kDefaultMaxSessionTimeoutMs = 1'800'000;
    /// A non-positive maximum is no maximum: it keeps the default (never a 0 ms session for every member).
    void setMaxSessionTimeoutMs(int32_t ms) noexcept { maxSessionTimeoutMs_ = ms > 0 ? ms : kDefaultMaxSessionTimeoutMs; }
    [[nodiscard]] int32_t maxSessionTimeoutMs() const noexcept { return maxSessionTimeoutMs_; }
    /// Test seam: resolve a session's client version without a gateway (-1 = unknown: treated as current).
    void setClientVersionResolverForTest(ClientVersionFn fn, void* ctx) noexcept {
        clientVersionFn_ = fn; clientVersionCtx_ = ctx;
    }
    [[nodiscard]] uint64_t parkRefusalsForTest() const noexcept { return parkRefusals_; }
    [[nodiscard]] uint64_t unstableAnswersForTest() const noexcept { return unstableAnswers_; }
private:
    std::size_t txnOffsetsWaiting_ = 0;   ///< total waiters (txnOffsetsReplicating())
    /// A plain OffsetCommit appended record `at` for (group, tp): it is the key's newest record.
    void notePlainOffsetCommit_(const std::string& group, const rune::coordination::TopicPartition& tp, int64_t at);
    rune::gateway::TopicRegistry*              topics_;     // non-owning; may be null
    std::function<bool()>                      isLeaderFn_;
    int8_t                                     localLeader_ = -1;   // see setLocalLeadership()
    std::function<NodeId()>                    leaderIdFn_;

    // CG-6 failover: detect Follower→Leader transition so we can rebuild the
    // in-memory offset index from the replicated Log once we become coordinator.
    // Initialised to false; set to true after the first successful rebuild.
    // Cleared back to false when we detect we are no longer the leader (so a
    // subsequent re-election triggers another rebuild).
    bool wasLeader_ = false;

    // CP-5.2 cold-restart fix: the offset-index rebuild is a NO-OP while the
    // __consumer_offsets partition's high-watermark is still 0 (rebuildFromLog
    // bails on hw<=0).  On a COLD cluster restart a node becomes coordinator with
    // HW temporarily gated at 0 (ISR not yet re-expanded to minIsr / followers not
    // yet caught up), so a one-shot rebuild on the leader transition recovers
    // NOTHING and consumer groups resume from offset 0.  We therefore RETRY the
    // rebuild every coordinator doWork until it actually runs with hw>0
    // (offsetsRebuilt_ latches true only then).  This retry window is BEFORE any
    // group resumes committing, so re-running rebuild (clear+repopulate from the
    // log) cannot clobber a live in-memory commit.
    bool offsetsRebuilt_ = false;
    /// E2 residual round 4: this coordinator has LOADED the offsets for the
    /// current leadership + log content of its __consumer_offsets replica
    /// (offsetsRebuilt_ for exactly the generation the replica has now, and the
    /// replica still leads).  Checked per request — a request can arrive before
    /// the duty cycle saw a leadership change.  Until then OffsetFetch and
    /// OffsetCommit answer the retriable NotLeader: a fetch would answer from
    /// an empty / partial index (-1 or an older offset — a guess), and a commit
    /// would be overwritten by the rebuild (it clears + replays the log).
    [[nodiscard]] bool offsetsLoaded_() const noexcept {
        if (offsets_ == nullptr) return true;
        const rune::replication::PartitionReplica* r = offsets_->replica();
        if (r == nullptr) return true;
        const uint64_t gen = (r->leadershipGeneration() << 32) ^ r->logGeneration();
        return offsetsRebuilt_ && wasLeader_ && gen == offsetsGen_ && r->acceptsProduce();
    }
    uint64_t offsetsNotLoadedAnswers_ = 0;   ///< entries answered NotLeader while loading (round 4)
public:
    [[nodiscard]] uint64_t offsetsNotLoadedAnswersForTest() const noexcept { return offsetsNotLoadedAnswers_; }
private:
    /// (leadershipGeneration << 32) ^ logGeneration of the offsets replica the
    /// index was rebuilt against (see doWork).
    uint64_t offsetsGen_ = 0;

    // CG-4: ClientGateway — non-owning; may be null (tests without push subs).
    rune::gateway::ClientGateway*              gateway_ = nullptr;

    // C1: see setTransientLineage().
    TransientLineageFn lineageFn_  = nullptr;
    void*              lineageCtx_ = nullptr;
    uint64_t           staleLineageOffsets_ = 0;
    uint64_t           pendingLineageAnswers_ = 0;
    [[nodiscard]] uint64_t lineageOf_(int32_t topicId, int32_t partition) const noexcept {
        return lineageFn_ != nullptr ? lineageFn_(lineageCtx_, topicId, partition) : 0;
    }
    /// The committed offset is a position of an ended lineage (C1): answer none.
    bool staleLineage_(const rune::coordination::CommittedOffset& co, int32_t topicId, int32_t partition);

    // R1 (round 5): superseded-lineage rebalances.  Cold (a lineage change is a
    // restart / an unclean election); allocation on this path is acceptable.
    LineageVersionFn lineageVersionFn_  = nullptr;
    void*            lineageVersionCtx_ = nullptr;
    uint64_t         lineageSweptVersion_ = 0;
    int64_t          nextLineageSweepNs_  = 0;
    /// Round 6 (F1): the lineage of each TRANSIENT partition a group's current
    /// generation consumes (the union of its members' assignments — every
    /// partition of its subscribed topics), recorded when it reached Stable;
    /// kLineagePending when unknown then (members could not fetch a position
    /// until it confirmed: the first confirmed lineage is adopted).  A group
    /// whose consumed lineage is superseded is rebalanced even with NO
    /// committed offset (an in-memory position must not cross the change).
    struct GenLineage { int32_t topicId; int32_t partition; uint64_t lineage; };
    std::unordered_map<std::string, std::vector<GenLineage>> genLineages_;
    /// Round 6 (F4): what a sweep could not decide (the lineage was pending):
    /// re-checked alone every 100 ms — no fetchAll, no allocation (capacity
    /// persists; compacted in place).  `ref` = the lineage to compare with.
    struct PendingCheck { std::string groupId; int32_t topicId; int32_t partition; uint64_t ref; bool genEntry; };
    std::vector<PendingCheck> pendingChecks_;
    void recordGenLineages_(const GroupId& groupId, const rune::coordination::GroupCoordinator& coord);
    /// The group's recorded lineage entry for (topicId, partition), or nullptr.
    GenLineage* genLineageOf_(const std::string& groupId, int32_t topicId, int32_t partition);
    /// Decide one (group, partition): `ref` vs the current lineage.  Returns
    /// false when the current lineage is still pending (re-check later).
    bool checkSuperseded_(const std::string& groupId, int32_t topicId, int32_t partition, uint64_t ref,
                          bool genEntry, int64_t nowNs);
    void retryPendingChecks_(int64_t nowNs);
    void pruneGroupLineageState_(const std::string& groupId);
    /// #565: this node is not the group coordinator — drop every in-memory group (Kafka-style unload): end the
    /// members' group push subscriptions, forget the member bindings and per-group lineage state.  No Assign frame
    /// (a member learns NotCoordinator on its next request).  Returns the number of groups unloaded.
    int unloadGroups_(const char* why);
    /// #570: the current coordinator term (0 without a term fn).
    [[nodiscard]] int64_t currentTerm_() const { return termFn_ ? termFn_() : 0; }
    /// #570: unload the groups when they were loaded under another term.  Returns the number unloaded.
    int syncTerm_();
    std::function<int64_t()> termFn_;
    int64_t                  groupsTerm_ = 0;   ///< the term coordinators_ was (re)populated under
public:
    [[nodiscard]] std::size_t groupCountForTest() const noexcept { return coordinators_.size(); }
    [[nodiscard]] std::size_t pendingLineageChecksForTest() const noexcept { return pendingChecks_.size(); }
    [[nodiscard]] std::size_t supersededMarkGroupsForTest() const noexcept { return supersededHandled_.size(); }
private:
    uint64_t         supersededRebalances_    = 0;
    uint64_t         supersededCommitsFenced_ = 0;
    /// group -> (topicId, partition, new lineage) it was already rebalanced for.
    struct SupersededMark { int32_t topicId; int32_t partition; uint64_t lineage; };
    std::unordered_map<std::string, std::vector<SupersededMark>> supersededHandled_;
    // #549: per group, topic ids a JoinGroup named that were not assignable then (unknown here yet, or not placed).
    // On the coordinator only, doWork forces one rebalance once any becomes assignable while the group is Stable and
    // no member holds it, or arrives as a view sink (the re-join then gets #19's refusal).  An id found assignable is
    // dropped; an emptied or Dead group forgets its ids.
    // Bounded: at most kMaxUnassignedJoinTopics ids per group (cold control-plane state).
public:
    static constexpr std::size_t kMaxUnassignedJoinTopics = 64;
    [[nodiscard]] std::size_t unassignedJoinTopicsForTest(const std::string& g) const {
        const auto it = unassignedJoinTopics_.find(g);
        return it == unassignedJoinTopics_.end() ? 0 : it->second.size();
    }
private:
    std::unordered_map<std::string, std::vector<int32_t>> unassignedJoinTopics_;
    /// The one "can this topic id be assigned now" predicate (registry-known, not a view sink, placed).
    [[nodiscard]] bool topicIdAssignable_(int32_t tid) const;
    void sweepSupersededLineages_(int64_t nowNs);
    /// Rebalance `groupId` once for (topicId, partition)'s new lineage `now`;
    /// true when this call forced it (false: already handled / empty group).
    bool rebalanceForSupersededLineage_(const GroupId& groupId, rune::coordination::GroupCoordinator& coord,
                                        int32_t topicId, int32_t partition, uint64_t now, int64_t nowNs);

    /// Per-group coordinators.  Inserted lazily on first Join.
    /// std::map with std::less<> (transparent comparator) so hot-path find()
    /// can be called with std::string_view without constructing a std::string.
    std::map<GroupId, rune::coordination::GroupCoordinator, std::less<>> coordinators_;
    /// LOW-3 (CG round 4): JoinGroup refusals per old-client session (log rate-limit; cold).
    std::map<int64_t, uint64_t> oldClientRefusals_;
    /// Phase 2c: JoinGroup filter-mismatch refusals per "<group>\x1f<session>" (log rate-limit; cold, cleared at 4,096).
    std::unordered_map<std::string, uint64_t> filterMismatchRefusals_;
    std::string filterDetailScratch_;   ///< backs JoinGroupResponse::detail (cold refusal path)
    void noteFilterMismatch_(const std::string& groupId, int64_t sessionId);

    // CG-4: member → sessionId tracking.
    // Keyed by buildMemberKey_(groupId, memberId) ("<len>:<group><member>", unambiguous).
    // Populated on JoinGroup; cleared on Leave and whenever the coordinator
    // removes the member (expiry, join timeout: drainDeparted_).
    // Used by doWork to know which egress session to send Assign frames to and
    // which session to create push subscriptions for.
    // Issues #123 / #124 / #169: it is also THE ownership record — a member id
    // is bound to the session (and principal) that obtained it.  Sync,
    // Heartbeat, Leave and OffsetCommit are honoured only on that session; a
    // JoinGroup naming it is a re-join only for that principal (a reconnect),
    // and a fresh member otherwise.  A refusal is answered exactly as for a
    // member that does not exist.
    // Single-writer (duty-cycle thread); no locks needed.
    // std::map with std::less<> enables transparent find() with string_view.
    struct MemberBinding { int64_t sessionId = 0; uint64_t principal = 0; };
    std::map<std::string, MemberBinding, std::less<>> memberSessionMap_;
    /// The map key of (groupId, memberId) into memberKeyScratch_ (unambiguous: length-prefixed group).
    void buildMemberKey_(std::string_view groupId, std::string_view memberId);
    /// The binding of (groupId, memberId), or nullptr.  Builds memberKeyScratch_.
    const MemberBinding* bindingOf_(std::string_view groupId, std::string_view memberId);
    /// True when (groupId, memberId) is bound to `sessionId`.
    bool ownsMember_(std::string_view groupId, std::string_view memberId, int64_t sessionId) {
        const MemberBinding* b = bindingOf_(groupId, memberId);
        return b != nullptr && b->sessionId == sessionId;
    }
    /// The authenticated principal of `sessionId` (0: none — dev / in-process brokers, unit rigs).
    [[nodiscard]] uint64_t principalOf_(int64_t sessionId) const;
    /// Drop the bindings of the members `coord` removed since the last drain (cold).
    void drainDeparted_(std::string_view groupId, rune::coordination::GroupCoordinator& coord);
    std::vector<std::string> departedScratch_;
public:
    /// Test seam: resolve a session's principal without a gateway.
    using PrincipalFn = uint64_t (*)(void* ctx, int64_t sessionId) noexcept;
    void setPrincipalResolverForTest(PrincipalFn fn, void* ctx) noexcept { principalFn_ = fn; principalCtx_ = ctx; }
private:
    PrincipalFn principalFn_  = nullptr;
    void*       principalCtx_ = nullptr;

    // CG-7 zero-alloc: per-handler scratch strings.
    // Assigned via assign(data, size) each call — no heap alloc when
    // capacity >= size (capacity grows on first long-string call, then stable).
    // Never holds state across call boundaries — purely per-handler scratch.
    std::string groupIdScratch_;    ///< groupId span → reusable std::string
    std::string memberIdScratch_;   ///< memberId span → reusable std::string
    std::string memberKeyScratch_;  ///< "groupId:memberId" composite lookup key
    std::vector<std::string> evictScratch_;   ///< fix round 4: members to evict (cold path)
    /// Every member tracked on `sessionId` leaves its group (subscriptions revoked, session mapping dropped); `why`
    /// names the cause in the warn line.  evictSessionMembers / removeSessionMembers (#212).  Cold.
    void evictSessionMembers_(int64_t sessionId, int64_t nowNs, const char* why);

    // CG-4: scratch buffer for Assign frame encoding.
    // 68 KiB bound: header 24 + generationId 4 + count 2 = 30 B fixed; each
    // TopicPartition32 entry is 8 B (topicId 4 + partition 4).
    // Max entries = (68*1024 - 30) / 8 = 8704 partitions per member in one frame.
    // This matches the encodedSizeFor() guard in onReachedStable; real clusters
    // have far fewer partitions per member.  Frame is capped, not overrun.
    // Pre-allocated; no per-rebalance heap allocation.
    static constexpr std::size_t kAssignFrameScratchSize = 68 * 1024;
    std::array<uint8_t, kAssignFrameScratchSize> assignFrameScratch_{};

    // CG-4: assignment TopicPartition32 encode scratch (for Assign frame encoding).
    // Pre-reserved; stays at steady-state capacity after first rebalance.
    std::vector<rune::protocol::client::TopicPartition32> assignFrameTpScratch_;

    // -----------------------------------------------------------------------
    // Pre-reserved scratch vectors — avoid per-handler allocation on hot path.
    // -----------------------------------------------------------------------

    /// assignment encode scratch: TopicPartition32 for SyncGroupResponse.
    /// Pre-reserved; stays at steady-state capacity.
    std::vector<rune::protocol::client::TopicPartition32> assignScratch_;

    /// OffsetCommit response results scratch.
    std::vector<rune::protocol::client::OffsetCommitResult> commitResultScratch_;

    /// OffsetFetch response results scratch.
    std::vector<rune::protocol::client::OffsetFetchResult> fetchResultScratch_;

    /// topicName ↔ topicId mapping scratch (for OffsetCommit/Fetch topic lookup).
    /// The protocol sends topicId (int32), the coordination layer uses topic name.
    /// We build the reverse map from TopicRegistry on the first lookup per handler.
    /// Re-used scratch; cleared per-call (no allocation if capacity >= nTopics).
    std::vector<std::pair<int32_t, std::string>> topicIdToNameScratch_;

    /// JoinGroup subscribedTopics scratch — reused across calls to avoid per-call
    /// vector allocation on the hot path.  Cleared + refilled on each JoinGroup.
    /// Short topic names benefit from SSO; the vector itself is amortized.
    std::vector<std::string> joinTopicsScratch_;

};

} // namespace rune::node
