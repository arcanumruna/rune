#pragma once
// ---------------------------------------------------------------------------
// TransactionCoordinationService.h — TX-3: wires TransactionCoordinator +
// ReplicatedTxnStateStore into the broker duty cycle; translates client txn
// requests into durable state transitions.
//
// Namespace: rune::node
// C++23. Single-writer (doWork driven). No locks. No hot-path allocation.
//
// ## Responsibilities
//
//   - Five request handlers (InitProducerId / AddPartitionsToTxn /
//     AddOffsetsToTxn / TxnOffsetCommit / EndTxn): decode the client-protocol
//     frame (TxnMessages.h), call TransactionCoordinator, persist the resulting
//     TransactionMetadata via ReplicatedTxnStateStore::put(acksAll=true) BEFORE
//     responding, then encode the response into the caller-supplied buffer.
//   - F15 §5.1 coordinator LOAD state machine (Unloaded | Loaded{epoch}).
//     Loaded only while: this node is the consensus leader (FindCoordinator
//     routes here) AND the __transaction_state replica is Leader AND HW == LEO — then the store
//     is folded to the (committed) HW and the coordinator reloaded under
//     coordinatorEpoch = that replica's leader epoch.  Evaluated at the top of
//     doWork and of every handler (progress-driven, no timer).  Unloaded: every
//     handler answers NotCoordinator, no timeouts run, nothing is written.
//     Unload (consensus or replica leadership lost, or epoch change): deferred
//     EndTxns answered NotCoordinator, marker tasks dropped, the gateway's
//     queued markers of the old epoch purged, the coordinator cleared.
//     Store-less (unit rigs): the predicate is isLeader(), coordinatorEpoch a
//     local counter bumped per load.
//   - F15 §5.6 persisted pid block ceiling (store-backed): pids are handed out
//     only below a COMMITTED ceiling record; the next block is prefetched at
//     half use; InitProducerId answers ConcurrentTransactions (27) meanwhile.
//   - doWork(nowNs): drives TransactionCoordinator::doWork (timeout aborts,
//     only while Loaded); resolves pending markers (Task 8).
//
// ## Constructor deps
//
//   - ReplicatedTxnStateStore*    txnState — non-owning; must outlive.
//   - rune::gateway::TopicRegistry* topics  — non-owning; may be null.
//   - std::function<bool()>  isLeader       — leader query callback.
//   - std::function<NodeId()> leaderId      — current leader nodeId callback.
//
// ## Zero-alloc behaviour (CG-7 rule)
//
//   Handlers allocate nothing on the response-encode path.  txnIdScratch_
//   and groupIdScratch_ are member std::strings that amortise allocation:
//   assign(data, size) reuses capacity.  Partition scratch vectors are
//   pre-reserved members.  No per-call std::string construction for txnId
//   lookup — transparent comparators (std::less<>) on the coordinator map.
// ---------------------------------------------------------------------------

#include <spdlog/spdlog.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/Types.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/coordination/TxnOffsetApply.h>

#include <cstddef>
#include <deque>
#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Forward declaration — ClientGateway lives in rune_gateway; node→gateway direction is fine.
namespace rune::gateway { class ClientGateway; }

namespace rune::node {

using NodeId        = int32_t;
using TransactionalId = rune::transaction::TransactionalId;

// TX-3: TransactionCoordinationService implements ITxnRequestHandler so that
// ClientGateway can dispatch txn messages through the interface without
// introducing a rune_gateway→rune_node symbol dependency (mirrors CG-4 pattern).
class TransactionCoordinationService : public rune::gateway::ITxnRequestHandler {
public:
    // -----------------------------------------------------------------------
    // Construction
    //
    // @param txnState   ReplicatedTxnStateStore — non-owning; must outlive.
    // @param topics     TopicRegistry — non-owning; may be null.
    // @param isLeader   Callable → true when this node is the cluster leader.
    // @param leaderId   Callable → current cluster leader NodeId.
    // -----------------------------------------------------------------------
    TransactionCoordinationService(
        rune::transaction::ReplicatedTxnStateStore* txnState,
        rune::gateway::TopicRegistry*               topics,
        std::function<bool()>                       isLeader = [] { return true; },
        std::function<NodeId()>                     leaderId = [] { return NodeId{0}; });

    // Non-copyable, non-movable (holds coordinator + pre-reserved scratch).
    TransactionCoordinationService(const TransactionCoordinationService&)            = delete;
    TransactionCoordinationService& operator=(const TransactionCoordinationService&) = delete;

    // -----------------------------------------------------------------------
    // doWork — drive TransactionCoordinator::doWork (timeout aborts);
    // resolve pending markers (stub for Task 8).
    //
    // Returns work count (0 when idle in steady state).
    // Must be called from ONE thread only (duty-cycle thread).
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // setGateway — wire in the ClientGateway for marker append (Task 8).
    // Non-owning; lifetime must exceed the service.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setGateway(rune::gateway::ClientGateway* gateway) noexcept {
        gateway_ = gateway;
    }

    /// Issue #122: this cycle's committed metadata feature level, pushed by the runtime (NodeRuntimeAgent).  Bound
    /// producer ids are issued only at MetadataFeature::kBoundProducerIds or above (every member supports them); below
    /// it ids are issued raw and nothing is rebound (an older coordinator may take over).  Unpushed (-1, rigs without
    /// a metadata service): bound.
    void setMetadataFeatureLevel(int16_t level) noexcept { featureLevel_ = level; }
    [[nodiscard]] bool bindsProducerIds() const noexcept;

    /// Test seam (issue #122): resolve a session's principal without a gateway.
    using PrincipalFn = uint64_t (*)(void* ctx, int64_t sessionId) noexcept;
    void setPrincipalResolverForTest(PrincipalFn fn, void* ctx) noexcept { principalFn_ = fn; principalCtx_ = ctx; }

    // -----------------------------------------------------------------------
    // setAppendFnOverride — TEST SEAM ONLY.
    // When gateway_ is null, doWork uses this function to resolve pending
    // markers.  The fn maps (TopicPartition, markerBytes) to a log append.
    // Allows testing the doWork→resolvePendingMarkers path without Aeron.
    // Set to nullptr to clear (production: never call this).
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setAppendFnOverride(
        std::function<int64_t(std::string_view topic, int32_t partition,
                              std::span<const uint8_t> bytes)> fn) noexcept {
        appendFnOverride_ = std::move(fn);
    }

    // -----------------------------------------------------------------------
    // isLocalCoordinator — true when this node is the cluster leader (where
    // FindCoordinator routes).  NOT sufficient to serve: see isLoaded().
    // v1 seam: all transactions are coordinated by the cluster leader.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLocalCoordinator() const;

    // Task 11 fix 2 (B1): this node's leadership for the current runtime duty
    // cycle, pushed as a VALUE by NodeRuntimeAgent (same thread) right after
    // consensus runs.  Once pushed, isLocalCoordinator() never calls the
    // isLeader std::function (per-cycle, worker 0's pass under fused placement).
    // Unpushed (-1): the callable is used (service-only tests).
    void setLocalLeadership(bool leader) noexcept { localLeader_ = leader ? 1 : 0; }

    /// F15 §5.1: true while the coordinator is Loaded (serving).
    [[nodiscard]] bool isLoaded() const noexcept { return loadState_ == LoadState::Loaded; }
    /// F15 §5.1: the loaded coordinatorEpoch (-1 while Unloaded).
    [[nodiscard]] int32_t coordinatorEpoch() const noexcept {
        return loadState_ == LoadState::Loaded ? coordinatorEpoch_ : -1;
    }

    // -----------------------------------------------------------------------
    // coordinatorFor — returns the cluster-leader nodeId for any txnId.
    // v1 seam (future: per-txnId hash routing).
    // -----------------------------------------------------------------------
    [[nodiscard]] NodeId coordinatorFor(const TransactionalId& txnId) const;

    // -----------------------------------------------------------------------
    // Handlers — decode the request from buf[0..len-1], act, encode the
    // response into out[] (reused by caller), return bytes written.
    //
    // While the coordinator is not Loaded (F15 §5.1): encode the protocol
    // response with errorCode = NotCoordinator and return that size.
    //
    // buf and out MUST NOT overlap.
    // Never throws.  Never allocates from heap in steady state.
    // -----------------------------------------------------------------------
    size_t handleInitProducerId (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleAddPartitions  (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleAddOffsets     (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleTxnOffsetCommit(const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleEndTxn         (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;

    /// A control marker this node could not append.  NEVER counts as landed: a
    /// retriable refusal (NotLeader, MinIsrViolation, Internal, ...) is re-dispatched
    /// with a doubling backoff until it lands; a partition that no longer exists
    /// (UnknownTopic / UnknownPartition, or gone from metadata) drops the marker
    /// (Kafka TransactionMarkerChannelManager parity).
    void onControlMarkerRefused(int32_t topicId, int32_t partition,
                                int64_t producerId, int16_t errorCode,
                                int32_t coordinatorEpoch) override;
    /// Task 10 round 5: an asynchronously appended marker landed.
    void onControlMarkerLanded(int32_t topicId, int32_t partition, int64_t producerId,
                               int32_t coordinatorEpoch) override;
    [[nodiscard]] std::size_t awaitingMarkerTxnsForTest() const noexcept { return awaiting_.size(); }
    [[nodiscard]] uint64_t transactionsCompletedForTest() const noexcept { return transactionsCompleted_; }
    [[nodiscard]] uint64_t controlMarkersRefusedForTest() const noexcept { return controlMarkersRefused_; }
    [[nodiscard]] std::size_t queuedMarkerRetriesForTest() const noexcept { return queuedRetries_; }
    /// D9 "stuck markers" gauge (observability only, never a correctness
    /// input): markers of Prepare* transactions unresolved for longer than
    /// kStuckMarkerNs — refused and still retrying (since the first refusal),
    /// or never refused but still in flight / parked on the leader's HW (since
    /// dispatch; re-review MEDIUM-2).  Recomputed every
    /// doWork; a rise from 0 / a return to 0 is logged.  Control thread.
    static constexpr int64_t kStuckMarkerNs = 5'000'000'000;   // 5 × kMarkerRetryMaxNs
    [[nodiscard]] std::size_t stuckMarkers() const noexcept { return stuckMarkers_; }
    /// D9 review MEDIUM-3: where the gauge is published (BrokerNode: an Aeron
    /// counter in the driver's CnC, next to the node-role counters).  Called on
    /// the control thread when the value changes; `false` = not published yet
    /// (counter not resolved) → retried on the next scan.  Function pointer +
    /// ctx (no std::function).
    using StuckMarkersSink = bool (*)(void* ctx, int64_t value);

    /// Phase 2b F Task 11: where a COMMITTED transaction's consumer-group offsets (TxnOffsetCommit) are applied -
    /// the group coordinator's offset store (BrokerNode wires GroupCoordinationService::applyTransactionalOffset).
    /// Refused when it cannot apply NOW (not the group coordinator / offsets not loaded yet / gated): the
    /// transaction then stays PrepareCommit, its EndTxn answer stays deferred, and the completion is retried from
    /// doWork with backoff (duty clock) until the sink accepts - a refused apply never completes the commit.
    /// #358: Replicating while the offset record is appended but not yet committed on the offsets replica: the
    /// transaction likewise stays PrepareCommit (CompleteCommit is not persisted, EndTxn not answered) and the
    /// completion is re-checked on every doWork (no backoff: it waits on replication, not on a refusal) until every
    /// offset is Applied.  No sink wired = no offset store on this node (unit rigs): offsets are accepted and
    /// dropped.  Called on the control thread; the apply is an overwrite, so re-drives are idempotent.
    /// `token` (PR #434 review F2): one value per committing transaction (producer id, epoch, start), so the sink
    /// answers a transaction only from the offset record THAT transaction appended.
    using TxnOffsetSink = rune::coordination::TxnOffsetApply (*)(void* ctx, const std::string& group,
                                                                 const rune::coordination::TopicPartition& tp,
                                                                 int64_t offset, int64_t nowNs, uint64_t token);
    void setTxnOffsetSink(TxnOffsetSink fn, void* ctx) noexcept { offsetSink_ = fn; offsetSinkCtx_ = ctx; }
    /// Completion attempts deferred because the sink refused (a retry counter, not a loss counter).
    [[nodiscard]] uint64_t txnOffsetApplyRetries() const noexcept { return offsetApplyRetries_; }
    /// #358: completion attempts deferred because an offset record was still replicating.
    [[nodiscard]] uint64_t txnOffsetReplicationWaits() const noexcept { return offsetReplicationWaits_; }
    [[nodiscard]] rune::transaction::TxnState txnStateForTest(const rune::transaction::TransactionalId& id) const {
        const auto* m = coordinator_.metadata(id);
        return m != nullptr ? m->state : rune::transaction::TxnState::Empty;
    }
    void setStuckMarkersSink(StuckMarkersSink fn, void* ctx) noexcept {
        stuckSink_ = fn; stuckSinkCtx_ = ctx; stuckPublished_ = -1;
    }

    /// #358 class: answers parked until their __transaction_state record commits, and transactions whose markers
    /// wait for their Prepare* record to commit (test / observability).
    [[nodiscard]] std::size_t parkedAnswers() const noexcept { return parked_.size(); }
    [[nodiscard]] std::size_t prepareGates() const noexcept { return prepareGates_.size(); }
    /// Test seam: where a deferred answer goes when no gateway is wired (production: ClientGateway::sendControlFrame).
    using AnswerSinkFn = void (*)(void* ctx, int64_t sessionId, const uint8_t* frame, std::size_t len);
    void setAnswerSinkForTest(AnswerSinkFn fn, void* ctx) noexcept { answerSink_ = fn; answerSinkCtx_ = ctx; }

private:
    // ----- #358 class: nothing that depends on a __transaction_state record happens before it is COMMITTED -----
    // An acks-all append replicates asynchronously; a coordinator failover before the record reaches the new
    // leader loses it.  So an answer that follows an append (InitProducerId's epoch, AddPartitions' registration,
    // AddOffsets / TxnOffsetCommit's offsets, a synchronous EndTxn) is PARKED until the replica's HW passes the
    // record, and a transaction's markers are written only once its Prepare* record is committed (a commit marker
    // ahead of a lost PrepareCommit lets the next coordinator abort the same transaction elsewhere).
    struct ParkedAnswer {
        int64_t              recordOffset = -1;
        int64_t              sessionId    = 0;
        std::vector<uint8_t> request;   ///< answered NotCoordinator from it if the coordinator unloads first
        std::vector<uint8_t> answer;
    };
    std::deque<ParkedAnswer> parked_;                 ///< in record order (appends are monotone)
    std::vector<std::vector<uint8_t>> spareBufs_;     ///< recycled ParkedAnswer buffers (steady state allocates nothing)
    static constexpr int64_t kNeedsPut = -2;          ///< PrepareGate: the Prepare* record is not appended yet
    struct PrepareGate {
        rune::transaction::TransactionalId          txnId;
        int64_t                                     producerId       = -1;
        int16_t                                     producerEpoch    = 0;
        int32_t                                     coordinatorEpoch = -1;
        bool                                        commit           = false;
        std::vector<rune::transaction::TopicPartition> parts;
        bool                                        respond          = false;   ///< a deferred EndTxn answer
        int64_t                                     sessionId = 0, correlationId = 0;
        bool                                        durable          = false;   ///< loaded from the committed log
        int64_t                                     recordOffset     = kNeedsPut;
    };
    std::vector<PrepareGate> prepareGates_;
    /// The offset of the last record appended per transactional id (cleared on load: the loaded log is committed).
    std::map<rune::transaction::TransactionalId, int64_t, std::less<>> lastPutOffset_;
    int64_t requestPutOffset_  = -1;      ///< the newest record the current request appended
    bool    requestPutRefused_ = false;   ///< the current request's append was refused (min-ISR)
    AnswerSinkFn answerSink_    = nullptr;
    void*        answerSinkCtx_ = nullptr;
    /// Append `meta` (records the offset); false: refused.
    bool persist_(const rune::transaction::TransactionMetadata& meta);
    [[nodiscard]] bool committed_(int64_t recordOffset) const;
    void beginRequest_() noexcept { requestPutOffset_ = -1; requestPutRefused_ = false; }
    /// The handler's answer `n`, sent now when every record it depends on is committed, else parked (returns 0).
    size_t finishRequest_(const uint8_t* buf, size_t len, uint8_t* out, size_t outCap, size_t n);
    void sendDeferred_(int64_t sessionId, const uint8_t* frame, std::size_t n);
    void sendEndTxnAnswer_(int64_t sessionId, int64_t correlationId, int16_t errorCode);
    int  releaseParked_();
    /// The gate for `meta`'s markers (respond: the EndTxn answer waits on them).
    [[nodiscard]] PrepareGate makeGate_(const rune::transaction::TransactionMetadata& meta, bool commit,
                                        std::vector<rune::transaction::TopicPartition> parts, bool respond,
                                        int64_t sessionId, int64_t correlationId);
    /// True once `g`'s Prepare* record is committed (appending it first when needed).
    bool gateOpen_(PrepareGate& g);
    int  processPrepareGates_();
    void dropDeferred_();   ///< unload: every parked answer / gated EndTxn answered NotCoordinator
    size_t handleInitProducerId_ (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleAddPartitions_  (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleAddOffsets_     (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleTxnOffsetCommit_(const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleEndTxn_         (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);

    uint64_t controlMarkersRefused_ = 0;
    uint64_t transactionsCompleted_ = 0;
    TxnOffsetSink offsetSink_    = nullptr;
    void*         offsetSinkCtx_ = nullptr;
    uint64_t      offsetApplyRetries_ = 0;
    uint64_t      offsetReplicationWaits_ = 0;
    bool          offsetRefusedThisPass_  = false;   ///< #358: a completion pass met a Refused (vs only Replicating)
    uint64_t      completionToken_        = 0;       ///< F2: the transaction completeAwaiting_ is completing
    std::size_t   offsetsPendingTxns_ = 0;   ///< awaiting_ entries deferred on a refused offset apply
    static constexpr int64_t kOffsetRetryMinNs = 10'000'000;     // 10 ms
    static constexpr int64_t kOffsetRetryMaxNs = 1'000'000'000;  // 1 s
    bool applyTxnOffset_(const std::string& group, const rune::coordination::TopicPartition& tp, int64_t offset);
    bool completeAwaiting_(int64_t producerId, bool respond);   // true: completed (entry erased)
    int  retryDeferredCompletions_(int64_t nowNs);
    // Task 10 round 5: Prepare* transactions whose markers are still in flight,
    // keyed by the markers' producerId (one transaction per producer at a time:
    // the next one is refused with ConcurrentTransactions until this completes).
    // A marker refused with a RETRIABLE error stays outstanding and is re-dispatched from doWork
    // with a doubling backoff until it lands (Kafka TransactionMarkerChannelManager parity); the
    // transaction never completes over a marker that is not in the log.
    struct MarkerRetry {
        int32_t topicId   = 0;
        int32_t partition = 0;
        int64_t dueNs     = 0;
        int64_t backoffNs = 0;
        bool    inFlight  = false;   ///< re-dispatched, reply pending: keeps its backoff
        int64_t firstRefusedNs = 0;  ///< D9 gauge: when this marker was first refused
    };
    struct AwaitingMarkers {
        rune::transaction::TransactionalId txnId;
        int     outstanding      = 0;
        bool    respPending      = false;   // a deferred EndTxn response
        int64_t sessionId        = 0;
        int64_t correlationId    = 0;
        int16_t producerEpoch    = 0;       // to rebuild a retried marker
        int32_t coordinatorEpoch = -1;
        bool    commit           = false;
        bool    offsetsPending   = false;   // completeTransaction deferred: the offset sink refused
        int64_t offsetDueNs      = 0;
        int64_t offsetBackoffNs  = 0;
        int64_t dispatchedNs     = 0;       // D9 re-review MEDIUM-2: gauge age of never-refused markers
        std::vector<MarkerRetry> retries;   // refused markers awaiting re-dispatch
    };
    static constexpr int64_t kMarkerRetryMinNs = 10'000'000;      // 10 ms
    static constexpr int64_t kMarkerRetryMaxNs = 1'000'000'000;   // 1 s
    int64_t lastNowNs_ = 0;                 ///< doWork clock, used to schedule refusal retries
    std::size_t queuedRetries_ = 0;         ///< retries due for (re-)dispatch (not in flight)
    std::size_t stuckMarkers_  = 0;         ///< D9 gauge (see stuckMarkers())
    static constexpr int64_t kStuckScanIntervalNs = 100'000'000;   ///< gauge scan rate (observability only)
    int64_t     nextStuckScanNs_ = 0;
    StuckMarkersSink stuckSink_    = nullptr;
    void*            stuckSinkCtx_ = nullptr;
    int64_t          stuckPublished_ = -1;  ///< last value the sink took (-1 = none)
    void updateStuckMarkers_(int64_t nowNs);
    void publishStuckMarkers_();
    int retryRefusedMarkers_(int64_t nowNs);
    /// Queue (or re-queue with a doubled backoff) the marker for (topicId, partition).
    void queueRetry_(AwaitingMarkers& a, int32_t topicId, int32_t partition);
    /// Drop the retry entry for (topicId, partition) if any; true when one was dropped.
    bool dropRetry_(AwaitingMarkers& a, int32_t topicId, int32_t partition);
    [[nodiscard]] bool partitionExists_(int32_t topicId, int32_t partition) const;
    std::unordered_map<int64_t, AwaitingMarkers> awaiting_;
    bool dispatchMarkers_(const rune::transaction::TransactionalId& txnId, int64_t producerId,
                          int16_t producerEpoch, int32_t coordinatorEpoch, bool commit,
                          const std::vector<rune::transaction::TopicPartition>& parts,
                          int64_t respSessionId = 0, int64_t respCorrelationId = 0);
    void resolveMarker_(int64_t producerId);
    [[nodiscard]] int concurrentIfPreparing_(int err) const;

    // F15 §5.1 — coordinator load state machine (control thread only).
    enum class LoadState : uint8_t { Unloaded, Loaded };
    LoadState loadState_        = LoadState::Unloaded;
    int32_t   coordinatorEpoch_ = -1;   ///< valid while Loaded
    int32_t   localLoads_       = 0;    ///< store-less coordinatorEpoch counter
    /// Store-less load snapshot (always empty; a member, not a function-local
    /// static, so no static-init guard — shared-nothing law).
    const std::map<rune::transaction::TransactionalId, rune::transaction::TransactionMetadata,
                   std::less<>> noSnapshot_;
    /// Load if loadable_, unload if the Loaded predicate broke; true = serving.
    bool ensureLoaded_(int64_t nowNs);
    [[nodiscard]] bool loadable_() const;
    /// The txn-state replica's logGeneration() the loaded state was folded from.
    uint64_t loadedLogGen_ = 0;
    [[nodiscard]] bool stillLoaded_() const;
    void load_(int64_t nowNs);
    void unload_();
    /// Reply epoch filter: a marker reply counts only for the loaded epoch.
    [[nodiscard]] bool currentEpoch_(int32_t coordinatorEpoch) const noexcept;

    // F15 §5.6 — persisted pid block ceiling (store-backed only).
    int64_t                  pidCeilingPending_       = -1;   ///< ceiling put, not yet committed
    int64_t                  pidCeilingPendingOffset_ = -1;   ///< its record offset (-1: none)
    void advancePidBlock_();
    void requestPidCeiling_(int64_t ceiling);
    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    rune::transaction::ReplicatedTxnStateStore* txnState_;    // non-owning
    rune::gateway::TopicRegistry*               topics_;      // non-owning; may be null
    std::function<bool()>                       isLeaderFn_;
    int8_t                                     localLeader_ = -1;   // see setLocalLeadership()
    std::function<NodeId()>                     leaderIdFn_;

    // ClientGateway — non-owning; may be null (Task 8 marker append).
    rune::gateway::ClientGateway*               gateway_ = nullptr;

    // Issue #122: producer ids are issued to, and honoured only for, the session's principal.
    PrincipalFn principalFn_  = nullptr;
    void*       principalCtx_ = nullptr;
    int16_t     featureLevel_ = -1;   // see setMetadataFeatureLevel()
    /// The authenticated principal of `sessionId` (0: none).
    [[nodiscard]] uint64_t principalOf_(int64_t sessionId) const;
    /// The producer id a request may act as: `pid` itself when the session's principal may name it, else an id that
    /// matches no transaction (the request is then answered exactly like any other wrong producer id).
    [[nodiscard]] int64_t requestPid_(int64_t pid, int64_t sessionId) const;

    // TEST SEAM: override appendFn used by doWork when gateway_ is null.
    // Signature: (topic, partition, markerBytes) → baseOffset or -1.
    std::function<int64_t(std::string_view, int32_t, std::span<const uint8_t>)>
                                                appendFnOverride_;

    // Zero-alloc scratch containers for doWork marker-drive path.
    // Cleared+refilled each invocation — pre-reserved to avoid steady-state alloc.
    // PendingInfo: commit flag keyed by producerId.
    struct PendingInfo { bool commit; };
    std::unordered_map<rune::transaction::ProducerId, PendingInfo> pendingByPidScratch_;
    // MarkerNotify: per-marker data for notifyControlMarker post-processing.
    struct MarkerNotify {
        int32_t topicId;
        int32_t partition;
        int64_t producerId;
        int64_t markerOffset;
        bool    commit;
    };
    std::vector<MarkerNotify>                   toNotifyScratch_;
    std::vector<int64_t>                        dueScratch_;   ///< retryDeferredCompletions_ (reused)

    // Per-transactionalId coordinator (single global coordinator, v1 seam).
    // One coordinator manages all transactions on this node.
    rune::transaction::TransactionCoordinator   coordinator_;

    // CG-7 zero-alloc: per-handler scratch strings.
    // Assigned via assign(data, size) — no heap alloc when capacity >= size.
    // Never holds state across call boundaries.
    std::string txnIdScratch_;    ///< txnId span → reusable std::string
    std::string groupIdScratch_;  ///< groupId span → reusable std::string

    // Pre-reserved scratch for partition vectors (addPartitions, endTxn).
    std::vector<rune::transaction::TopicPartition> partitionScratch_;

    // Pre-reserved scratch for partition results (addPartitions / txnOffsetCommit
    // response path).  Cleared+refilled each call — no per-call vector alloc.
    std::vector<rune::protocol::client::TxnPartitionResult> partResultScratch_;

    // Member scratch for the offsets map passed to addOffsetsToTxn
    // (handleTxnOffsetCommit).  Cleared+refilled each call — no per-call map alloc.
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsetsScratch_;

    // Scratch string for topic-name fallback ("topic_<id>") — avoids the
    // per-partition temporary "topic_" + std::to_string(topicId) alloc.
    std::string topicNameScratch_;
};

} // namespace rune::node
