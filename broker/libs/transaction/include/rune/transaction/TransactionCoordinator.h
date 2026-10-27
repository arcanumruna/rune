#pragma once
// ---------------------------------------------------------------------------
// TransactionCoordinator.h — Kafka-faithful transaction state machine.
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. Single-writer doWork(nowNs) driven.
//
// ## Overview (spec P5-2)
//
// TransactionCoordinator manages the per-transactionalId transaction metadata,
// mirroring Kafka's TransactionCoordinator + TransactionMetadata design (KIP-98).
//
// ## State machine (Kafka-faithful)
//
//   Empty           — no ongoing transaction; initial/reset state.
//   Ongoing         — partitions have been added; transaction is open.
//   PrepareCommit   — endTransaction(commit) driven; awaiting marker writes (P5-3).
//   PrepareAbort    — endTransaction(abort) or timeout-abort driven.
//   CompleteCommit  — all COMMIT markers written (P5-3 calls completeTransaction).
//   CompleteAbort   — all ABORT markers written.
//   Dead            — metadata expired/deleted.
//   PrepareEpochFence — transient; epoch being bumped while a txn was Ongoing.
//
// Transitions:
//   Empty       + addPartitions     → Ongoing (records txnStartNs)
//   Ongoing     + addPartitions     → Ongoing (accumulates partitions)
//   Ongoing     + endTransaction(commit) → PrepareCommit
//   Ongoing     + endTransaction(abort)  → PrepareAbort
//   Ongoing     + timeout (doWork)       → PrepareAbort
//   PrepareCommit + completeTransaction  → CompleteCommit (resting, F15 §5.4)
//   PrepareAbort  + completeTransaction  → CompleteAbort  (resting, F15 §5.4)
//   Complete*     + addPartitions        → Ongoing
//   Complete*     + InitProducerId       → Empty (epoch bump, like Empty)
//
// ## InitProducerId / Zombie Fencing
//
//   No txnId → allocate fresh PID, epoch 0. No state machine entry created.
//   New txnId → allocate PID, epoch 0; create TransactionMetadata (Empty).
//   Known txnId (re-init) → BUMP epoch (epoch++; zombie producer with old epoch fenced);
//                           if Ongoing, first drive to PrepareAbort (mirror Kafka's
//                           "PrepareEpochFence" path), then reset to Empty.
//   Known txnId in PrepareCommit/PrepareAbort →
//                           kConcurrentTransactions (Kafka EOS: reject, let in-flight finish).
//   Known txnId in CompleteCommit/CompleteAbort → treated like Empty (bump).
//
// ## Transaction Timeout
//
//   doWork(nowNs) reaps Ongoing transactions where
//   nowNs - txnStartNs > txnTimeoutMs * 1_000_000 by transitioning to PrepareAbort
//   and persisting the transition to the log.
//   Samples nowNs ONCE per call. Returns work count. 0 when idle. NEVER spins.
//
// ## Internally-initiated aborts (fence + timeout) — B1/B2 resolution
//
//   Both fence-abort (initProducerId on Ongoing) and timeout-abort (doWork)
//   transition to PrepareAbort without a live client request.  The coordinator
//   exposes pendingMarkerTransactions() which returns all PrepareAbort/PrepareCommit
//   entries that were internally initiated (markersPending == true).
//   The caller MUST call ControlMarker::resolvePendingMarkers() and then
//   completeTransaction() for each such entry to write ABORT markers and advance
//   to CompleteAbort → Empty.  A recovered PrepareAbort (from the log) also sets
//   markersPending so the same code path resolves it after failover.
//
// ## Epoch overflow
//
//   When producerEpoch == INT16_MAX, a new producerId is allocated (epoch resets
//   to 0), matching Kafka's behaviour — never wrap to 0 on the same PID.
//
// ## Error Codes (returned in result structs)
//
//   kOk                   = 0
//   kInvalidProducerEpoch = 1   (zombie fenced — stale epoch or PID)
//   kInvalidPid           = 2   (wrong producerId for a txnId)
//   kInvalidTransition    = 3   (state machine rejects the operation)
//   kUnknownTxnId         = 4   (transactionalId not found)
//   kInvalidTxnTimeout    = 5   (txnTimeoutMs out of range)
//   kConcurrentTransactions = 6 (InitProducerId while Prepare*/Complete* in flight)
//
// ## Single-writer, no locks, no hot-path allocation.
//   (In-memory only here; P5-3 adds durable transaction log + replay.)
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>
#include <rune/transaction/ProducerIdAllocator.h>
#include <rune/protocol/ProducerIdBinding.h>
#include <rune/coordination/Types.h>

#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Forward-declare TransactionLog to avoid circular include
// (TransactionLog.h includes TransactionCoordinator.h).
namespace rune::transaction { class TransactionLog; }

namespace rune::transaction {

// ---------------------------------------------------------------------------
// InitProducerIdResult — returned by initProducerId()
// ---------------------------------------------------------------------------
struct InitProducerIdResult {
    ProducerId    producerId{-1};
    ProducerEpoch producerEpoch{-1};
    int           errorCode{0};

    static constexpr int kOk                    = 0;
    static constexpr int kInvalidTxnTimeout     = 5;
    static constexpr int kConcurrentTransactions = 6;
    /// The producerId space is exhausted (INT64_MAX) — NOT retriable.
    static constexpr int kProducerIdsExhausted   = 7;

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// AddPartitionsResult — returned by addPartitions()
// ---------------------------------------------------------------------------
struct AddPartitionsResult {
    int errorCode{0};

    static constexpr int kOk                   = 0;
    static constexpr int kInvalidProducerEpoch = 1;
    static constexpr int kInvalidPid           = 2;
    static constexpr int kInvalidTransition    = 3;
    static constexpr int kUnknownTxnId         = 4;
    /// F15 m2: the caller holds the NEW identity of an overflow fence whose
    /// PrepareAbort is still resolving — retriable (wire 27).
    static constexpr int kConcurrentTransactions = 6;

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// EndTransactionResult — returned by endTransaction()
// ---------------------------------------------------------------------------
struct EndTransactionResult {
    int errorCode{0};

    /// The set of partitions that need COMMIT/ABORT markers written (P5-3).
    std::vector<TopicPartition> partitions;

    /// F15 §5.4: true when the OK is the idempotent answer to a same-direction
    /// retry of an already COMPLETED transaction (no transition, no markers).
    bool alreadyComplete{false};

    static constexpr int kOk                   = 0;
    static constexpr int kInvalidProducerEpoch = 1;
    static constexpr int kInvalidPid           = 2;
    static constexpr int kInvalidTransition    = 3;
    static constexpr int kUnknownTxnId         = 4;
    /// F15 §5.4: same-direction retry while Prepare* (or any EndTxn by the
    /// caller of a fence) — retriable (wire 27).
    static constexpr int kConcurrentTransactions = 6;

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// CompleteTransactionResult — returned by completeTransaction()
// ---------------------------------------------------------------------------
struct CompleteTransactionResult {
    int errorCode{0};

    static constexpr int kOk                = 0;
    static constexpr int kInvalidTransition = 3;
    static constexpr int kUnknownTxnId      = 4;
    /// A checked offset sink refused an offset of the COMMIT being completed: nothing was persisted, the
    /// transaction stays PrepareCommit with its pendingOffsets; the caller retries completeTransaction later.
    static constexpr int kOffsetsPending    = 5;

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// PendingTxnOffset — one consumer-group offset bound to a transaction (F3).
// Part of the transaction's state; applied to OffsetStore on commit,
// discarded on abort.  Mirrors Kafka TxnOffsetCommit / sendOffsetsToTransaction
// (KIP-98).  Defined before TransactionMetadata because TransactionMetadata
// holds a std::vector<PendingTxnOffset>.
// ---------------------------------------------------------------------------
struct PendingTxnOffset {
    std::string                        group;
    rune::coordination::TopicPartition tp;
    rune::coordination::Offset         offset{0};
};

// ---------------------------------------------------------------------------
// TransactionMetadata — per-transactionalId state held by the coordinator.
//
// Structured so P5-3 can persist/replay: all fields are plain types, no
// heap-allocated inner containers except the partition set (which is bounded
// by the number of partitions in the cluster).
// ---------------------------------------------------------------------------
struct TransactionMetadata {
    TransactionalId txnId;
    ProducerId      producerId{-1};
    ProducerEpoch   producerEpoch{-1};
    int32_t         txnTimeoutMs{60'000};   ///< per-transactionalId timeout
    TxnState        state{TxnState::Empty};
    int64_t         txnStartNs{0};          ///< set on Ongoing entry; 0 otherwise

    /// Coordinator epoch — bumped on coordinator failover (P5-3/P5-5).
    /// Persisted to TransactionLog so a recovering coordinator can verify it
    /// owns a given transaction (stale-coordinator fencing).
    int32_t         coordinatorEpoch{0};

    /// Set of partitions currently in the transaction.
    /// std::set gives us sorted order + automatic dedup.
    std::set<TopicPartition> partitions;

    /// True while in PrepareEpochFence (transient bump state).
    bool fencingEpoch{false};

    /// True when this PrepareAbort/PrepareCommit was entered by an internally-
    /// initiated path (timeout-abort or epoch-fence-abort) that did NOT go through
    /// endTransaction().  The caller must call resolvePendingMarkers() to write
    /// ABORT markers and advance to CompleteAbort → Empty.  Also set when a
    /// Prepare* state is recovered from the log (failover resolution path, B2).
    bool markersPending{false};

    /// NEW-1: epoch-overflow-during-Ongoing-fence deferred PID swap.
    ///
    /// When an InitProducerId fences an Ongoing transaction AND the producerEpoch
    /// has overflowed (== INT16_MAX), Kafka aborts the in-flight txn under the
    /// EXISTING producerId/epoch first, and only adopts the freshly-allocated PID
    /// AFTER the abort completes.  We therefore keep producerId/producerEpoch as the
    /// OLD values through the PrepareAbort (so the ABORT marker MATCHES the in-flight
    /// data's PID), and stash the new PID/epoch here.  completeTransaction() adopts
    /// them when the PrepareAbort resolves to Empty.  Sentinel -1 == no pending swap.
    ProducerId    pendingNewProducerId{-1};
    ProducerEpoch pendingNewProducerEpoch{-1};

    /// Pending consumer-group offsets bound to this transaction (F3).
    /// Accumulated by addOffsetsToTxn(); applied to the OffsetStore on COMMIT
    /// (via OffsetApplyCallback), discarded on ABORT.
    /// Persisted to TransactionLog so a recovered PrepareCommit can re-apply them.
    std::vector<PendingTxnOffset> pendingOffsets;
};

// ---------------------------------------------------------------------------
// PendingMarkerTxn — descriptor returned by pendingMarkerTransactions().
// Contains everything needed for ControlMarker::build() without a live client
// request (B1 / B2 resolution).
// ---------------------------------------------------------------------------
struct PendingMarkerTxn {
    TransactionalId          txnId;
    ProducerId               producerId{-1};
    ProducerEpoch            producerEpoch{0};
    int32_t                  coordinatorEpoch{0};
    bool                     commit{false};          ///< false = abort
    std::vector<TopicPartition> partitions;
};

// ---------------------------------------------------------------------------
// OffsetApplyCallback — injected into TransactionCoordinator to write pending
// consumer-group offsets to the OffsetStore atomically on commit.
//
// Signature: (group, tp, offset, timestampNs) → void.
// nullptr = no-op (in-memory / unit-test mode without an OffsetStore).
// ---------------------------------------------------------------------------
using OffsetApplyCallback = std::function<void(const std::string&                        group,
                                                const rune::coordination::TopicPartition& tp,
                                                rune::coordination::Offset                offset,
                                                int64_t                                   timestampNs)>;

// ---------------------------------------------------------------------------
// TransactionCoordinator
// ---------------------------------------------------------------------------
class TransactionCoordinator {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param allocator            ProducerIdAllocator (injected for testability).
    /// \param defaultTxnTimeoutMs  Default timeout when none specified (or ≤0).
    /// \param txnLog               Optional durable TransactionLog (nullptr = in-memory only).
    ///                             When non-null, every state transition is appended+fsynced
    ///                             to the log BEFORE the transition's side effects are done.
    ///                             On construction, the log is replayed to rebuild txnMap_.
    /// \param coordinatorEpoch     This coordinator's epoch (P5-3/P5-5 failover fencing).
    /// \param offsetApplyCb        Optional callback called on COMMIT to apply pending consumer
    ///                             offsets to the OffsetStore (nullptr = no-op).
    explicit TransactionCoordinator(ProducerIdAllocator  allocator = ProducerIdAllocator{},
                                    int32_t              defaultTxnTimeoutMs = 60'000,
                                    TransactionLog*      txnLog = nullptr,
                                    int32_t              coordinatorEpoch = 0,
                                    OffsetApplyCallback  offsetApplyCb = nullptr);

    /// Install (or replace) the callback that applies a COMMITTED transaction's pending consumer-group
    /// offsets (see OffsetApplyCallback).  Single-writer: the coordinator's owner thread, before it serves.
    void setOffsetApplyCallback(OffsetApplyCallback cb) { offsetApplyCb_ = std::move(cb); }

    /// Checked variant (takes precedence): returns false when the offset could not be applied NOW (the offset
    /// store is not loaded / not the coordinator / gated).  completeTransaction(COMMIT) then persists NOTHING and
    /// returns kOffsetsPending, leaving the transaction PrepareCommit so the caller re-drives it - a refused apply
    /// is never a lost committed offset.  The apply is an overwrite, so a re-drive is idempotent.
    using CheckedOffsetApplyCallback = std::function<bool(const std::string&, const rune::coordination::TopicPartition&,
                                                          rune::coordination::Offset, int64_t)>;
    void setCheckedOffsetApplyCallback(CheckedOffsetApplyCallback cb) { checkedOffsetApplyCb_ = std::move(cb); }

    // Non-copyable (holds monotonic state).
    TransactionCoordinator(const TransactionCoordinator&)            = delete;
    TransactionCoordinator& operator=(const TransactionCoordinator&) = delete;

    // -----------------------------------------------------------------------
    // InitProducerId
    // -----------------------------------------------------------------------

    /// Allocate or refresh a producer identity.
    ///
    /// No txnId (nullopt) → allocate a fresh PID, epoch 0. No txn state created.
    /// New txnId (unknown) → allocate PID + epoch 0; create TransactionMetadata (Empty).
    /// Known txnId (re-init) → BUMP epoch to fence the previous producer (zombie
    ///   fencing). If an Ongoing transaction exists for this txnId, drive it to
    ///   PrepareAbort first (mirror Kafka's PrepareEpochFence path), then reset to
    ///   Empty with the new epoch. The caller MUST call completeTransaction() once
    ///   ABORT markers are written (P5-3) to move from PrepareAbort→CompleteAbort→Empty.
    ///
    /// \param txnId          Optional transactional id.
    /// \param txnTimeoutMs   Transaction timeout; ≤0 uses the default.
    /// \param nowNs          Injected clock (nanoseconds).
    ///
    /// \param owner          Issue #122: the principal the id is issued to (0 = none).  Set, every id handed out is
    ///                       in the bound format (rune/protocol/ProducerIdBinding.h) and carries it; a known txnId
    ///                       whose id is not the caller's (another principal's, or a legacy id) gets a FRESH id
    ///                       (epoch 0), exactly like the epoch-overflow path - the previous holder is fenced and its
    ///                       Ongoing transaction aborted under the old id.  Unset (unit rigs): raw counter ids.
    ///
    /// \returns {producerId, producerEpoch, errorCode}
    ///   errorCode = kInvalidTxnTimeout if txnTimeoutMs is out of range [1, max].
    InitProducerIdResult initProducerId(std::optional<TransactionalId> txnId,
                                        int32_t                        txnTimeoutMs,
                                        int64_t                        nowNs,
                                        std::optional<uint64_t>        owner = std::nullopt);

    // -----------------------------------------------------------------------
    // addPartitions
    // -----------------------------------------------------------------------

    /// Record that the producer is writing to the given partitions.
    ///
    ///   Empty   → Ongoing (records txnStartNs = nowNs).
    ///   Ongoing → Ongoing (accumulates; deduplicates partitions).
    ///   Any other state → InvalidTransition.
    ///
    /// Fences:
    ///   producerEpoch < txnMetadata.producerEpoch → InvalidProducerEpoch.
    ///   producerId != txnMetadata.producerId      → InvalidPid.
    ///
    /// \param txnId          Transactional id (must be known).
    /// \param producerId     Must match the registered producerId.
    /// \param producerEpoch  Must match the current epoch (zombie fencing).
    /// \param partitions     Partitions being added (may contain duplicates; deduped).
    /// \param nowNs          Injected clock.
    AddPartitionsResult addPartitions(const TransactionalId&          txnId,
                                      ProducerId                       producerId,
                                      ProducerEpoch                    producerEpoch,
                                      const std::vector<TopicPartition>& partitions,
                                      int64_t                          nowNs);

    // -----------------------------------------------------------------------
    // endTransaction
    // -----------------------------------------------------------------------

    /// Begin the commit or abort of a transaction.
    ///
    ///   commit=true  : Ongoing → PrepareCommit
    ///   commit=false : Ongoing → PrepareAbort
    ///   Any other state → InvalidTransition.
    ///
    /// Returns the set of partitions that need COMMIT/ABORT markers (P5-3).
    /// Fences stale epoch/PID.
    ///
    /// \param txnId          Transactional id.
    /// \param producerId     Must match.
    /// \param producerEpoch  Must match.
    /// \param commit         true = commit, false = abort.
    /// \param nowNs          Injected clock.
    EndTransactionResult endTransaction(const TransactionalId& txnId,
                                        ProducerId              producerId,
                                        ProducerEpoch           producerEpoch,
                                        bool                    commit,
                                        int64_t                 nowNs);

    // -----------------------------------------------------------------------
    // addOffsetsToTxn — transactional consumer-group offset commit (F3)
    // -----------------------------------------------------------------------

    /// Bind consumer-group offsets to the current transaction (Kafka
    /// addOffsetsToTxn / TxnOffsetCommit, KIP-98).
    ///
    /// Records each offset as PENDING within the transaction's state,
    /// persisted to the TransactionLog so they survive a coordinator failover.
    ///
    /// On completeTransaction(COMMIT): pending offsets are applied to the
    ///   OffsetStore via the OffsetApplyCallback (atomically with the commit).
    /// On completeTransaction(ABORT) or any abort path (fence / timeout /
    ///   recovered PrepareAbort): pending offsets are DISCARDED (never written).
    ///
    /// Valid states: Ongoing; with `beginsTxn` (the AddOffsetsToTxn request,
    /// X-1 — Kafka) also the resting Empty / Complete*, which it moves to
    /// Ongoing exactly like addPartitions (the client sends AddOffsets as soon
    /// as sendOffsetsToTransaction is applied — often before any
    /// AddPartitions, and an offsets-only transaction never sends one).
    /// TxnOffsetCommit (beginsTxn false) requires an Ongoing transaction.
    /// Prepare* → kInvalidTransition (the service answers it as retriable 27).
    /// Epoch/PID fenced like addPartitions.
    ///
    /// \param txnId         Transactional id.
    /// \param producerId    Must match.
    /// \param producerEpoch Must match.
    /// \param group         Consumer group id.
    /// \param offsets       Map of TopicPartition → offset to commit.
    /// \param nowNs         Injected clock (the txn start when it begins one).
    /// \param beginsTxn     true for AddOffsetsToTxn (may open the transaction).
    AddPartitionsResult addOffsetsToTxn(
        const TransactionalId&                                              txnId,
        ProducerId                                                           producerId,
        ProducerEpoch                                                        producerEpoch,
        const std::string&                                                   group,
        const std::map<rune::coordination::TopicPartition,
                       rune::coordination::Offset>&                          offsets,
        int64_t                                                              nowNs,
        bool                                                                 beginsTxn = false);

    // -----------------------------------------------------------------------
    // completeTransaction
    // -----------------------------------------------------------------------

    /// Called by P5-3 once COMMIT/ABORT markers have been written to all
    /// involved partitions.
    ///
    ///   PrepareCommit → CompleteCommit → Empty
    ///   PrepareAbort  → CompleteAbort  → Empty
    ///   PrepareAbort (epoch-fence path) → CompleteAbort → Empty (epoch kept)
    ///
    /// After this call the coordinator is ready for the next transaction with
    /// the same producerId/epoch.
    CompleteTransactionResult completeTransaction(const TransactionalId& txnId);

    /// Task 10 round 5: the markers of a Prepare* transaction were handed to
    /// the owning partitions (asynchronously).  Clears markersPending so the
    /// doWork re-drive does not write them twice; the state STAYS Prepare*
    /// until completeTransaction() — called once every marker has landed.
    /// (A failover recovery re-sets markersPending for any Prepare*.)
    void markersDispatched(const TransactionalId& txnId);

    // -----------------------------------------------------------------------
    // F15 §5.1 — coordinator load / unload (TransactionCoordinationService).
    // -----------------------------------------------------------------------

    /// Replace the whole in-memory state with @p snapshot (the txn-state store
    /// folded to its committed HW) under @p coordinatorEpoch:
    ///   - Prepare*  → markersPending (the committed decision is re-driven);
    ///   - Complete* → stays Complete* (resting state; a deferred overflow pid
    ///                 swap is adopted);
    ///   - Ongoing   → its timeout clock restarts at @p nowNs (only extends);
    ///   - every entry adopts @p coordinatorEpoch.
    /// The allocator is reseeded past every producerId in the snapshot.
    /// Cold path.  \returns the highest producerId seen (or -1).
    template<class Snapshot>
    ProducerId reload(const Snapshot& snapshot, int32_t coordinatorEpoch, int64_t nowNs) {
        txnMap_.clear();
        coordinatorEpoch_ = coordinatorEpoch;
        ProducerId maxPid = -1;
        for (const auto& [id, meta] : snapshot) {
            TransactionMetadata m = meta;
            adoptRecovered_(m);
            m.coordinatorEpoch = coordinatorEpoch;
            if (m.state == TxnState::Ongoing) m.txnStartNs = nowNs;
            if (m.producerId > maxPid)           maxPid = m.producerId;
            if (m.pendingNewProducerId > maxPid) maxPid = m.pendingNewProducerId;
            txnMap_.insert_or_assign(TransactionalId(id), std::move(m));
        }
        if (maxPid >= 0) allocator_.reseedAtLeast(reseedCounter_(maxPid));
        return maxPid;
    }

    /// Drop every transaction (unload).  The allocator is kept (monotone).
    void clear() noexcept { txnMap_.clear(); }

    /// This coordinator's epoch (stamped on every transition / marker).
    [[nodiscard]] int32_t coordinatorEpoch() const noexcept { return coordinatorEpoch_; }

    /// F15 I7: the producerId allocator (block-ceiling gating is driven by the
    /// TransactionCoordinationService, which persists the ceiling).
    [[nodiscard]] ProducerIdAllocator&       allocator()       noexcept { return allocator_; }
    [[nodiscard]] const ProducerIdAllocator& allocator() const noexcept { return allocator_; }

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Abort Ongoing transactions whose timeout has elapsed:
    ///   nowNs - txnStartNs > txnTimeoutMs * 1_000_000 → PrepareAbort.
    ///
    /// Each timed-out txn is persisted to the log (PrepareAbort) and added to
    /// the internal pending-markers set.  Call pendingMarkerTransactions() then
    /// ControlMarker::resolvePendingMarkers() to write the ABORT markers.
    ///
    /// Samples nowNs ONCE per call. Returns number of transitions performed.
    /// Returns 0 when there is no work. NEVER spins.
    int doWork(int64_t nowNs);

    /// Namespaces Phase 2 Task 5: every Ongoing transaction whose scoped id belongs to namespace `nsId` takes
    /// exactly the timeout-abort transition (PrepareAbort, markersPending, persisted).  Returns the count.  No
    /// allocation.  (Task 4 calls it on suspend / delete.)
    uint32_t abortAllForNamespace(uint32_t nsId, int64_t nowNs);

    // -----------------------------------------------------------------------
    // pendingMarkerTransactions — B1/B2 resolution API
    // -----------------------------------------------------------------------

    /// Return all PrepareAbort/PrepareCommit transactions that need markers
    /// written via an internally-initiated path (fence-abort, timeout-abort,
    /// or recovered Prepare* after failover).
    ///
    /// The caller MUST iterate the result, call ControlMarker::build() for each
    /// partition in each entry, and then call completeTransaction() for each
    /// txnId.  This is the only correct path for these entries — they do NOT
    /// go through endTransaction().
    ///
    /// After completeTransaction() the entry is removed from the pending set.
    [[nodiscard]] std::vector<PendingMarkerTxn> pendingMarkerTransactions() const;

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Transaction state for a transactionalId. Returns nullopt if unknown.
    [[nodiscard]] std::optional<TxnState> state(const TransactionalId& txnId) const noexcept;

    /// Current producer epoch for a transactionalId. Returns -1 if unknown.
    [[nodiscard]] ProducerEpoch epochOf(const TransactionalId& txnId) const noexcept;

    /// Current producerId for a transactionalId. Returns -1 if unknown.
    [[nodiscard]] ProducerId pidOf(const TransactionalId& txnId) const noexcept;

    /// Current partition set for a transactionalId (snapshot).
    [[nodiscard]] std::vector<TopicPartition> partitionsOf(const TransactionalId& txnId) const;

    /// Expose the full metadata for a txnId (for P5-3 persistence).
    [[nodiscard]] const TransactionMetadata* metadata(const TransactionalId& txnId) const noexcept;

    /// F15-FU1 (#112): the transaction whose CURRENT producer id is `pid`, or nullptr.
    /// Cold (one scan of the transactional ids): the caller caches the answer
    /// (TransactionCoordinationService::verifyTxnPartition) and re-validates it.
    [[nodiscard]] const TransactionMetadata* findByProducerId(ProducerId pid) const noexcept;

    /// Number of known transactionalIds.
    [[nodiscard]] std::size_t size() const noexcept { return txnMap_.size(); }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Look up metadata; returns nullptr if not found.
    [[nodiscard]] TransactionMetadata* findMetadata(const TransactionalId& txnId) noexcept;
    [[nodiscard]] const TransactionMetadata* findMetadata(const TransactionalId& txnId) const noexcept;

    /// Recovery normalisation shared by the constructor replay and reload():
    /// Prepare* → markersPending; a CompleteAbort that still carries a deferred
    /// overflow pid swap adopts it (Complete* is otherwise kept as-is).
    static void adoptRecovered_(TransactionMetadata& m) noexcept;

    /// Map an allocator failure sentinel to the InitProducerId answer.
    static InitProducerIdResult allocationFailure_(ProducerId sentinel) noexcept;
    /// The next id for `owner` (bound format) or the raw counter (no owner); a negative allocator sentinel as is.
    [[nodiscard]] ProducerId allocatePid_(const std::optional<uint64_t>& owner) noexcept;
    /// The allocator counter a recovered id was issued from (issue #122): its counter bits once this coordinator
    /// issues bound ids, the id itself in a raw-id rig.
    [[nodiscard]] ProducerId reseedCounter_(ProducerId pid) const noexcept {
        return boundIds_ ? rune::protocol::producerIdCounter(pid) : pid;
    }
    bool boundIds_ = false;
public:
    /// Issue #122: this coordinator issues bound ids (the broker's: every InitProducerId names an owner); recovered
    /// ids then reseed the allocator by their counter bits.  Call before any reload.
    void setBoundProducerIds(bool on) noexcept { boundIds_ = on; }
private:

    /// Validate epoch and PID match; returns errorCode (0 = ok).
    [[nodiscard]] int validateEpochAndPid(const TransactionMetadata& meta,
                                          ProducerId                  pid,
                                          ProducerEpoch               epoch) const noexcept;

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    ProducerIdAllocator allocator_;
    int32_t             defaultTxnTimeoutMs_;

    /// Optional durable log. Non-null → every transition is appended+fsynced.
    TransactionLog*     txnLog_{nullptr};

    /// This coordinator's epoch (persisted with each log entry for fencing).
    int32_t             coordinatorEpoch_{0};

    /// Optional callback: called on COMMIT to apply pending consumer-group offsets
    /// to the OffsetStore atomically with the transaction completion (F3).
    OffsetApplyCallback offsetApplyCb_;
    CheckedOffsetApplyCallback checkedOffsetApplyCb_;

    void beginInternalAbort_(TransactionMetadata& meta);   ///< the timeout-abort transition (doWork, abortAllForNamespace)

    /// Primary state: transactionalId → TransactionMetadata.
    std::unordered_map<TransactionalId, TransactionMetadata> txnMap_;
};

} // namespace rune::transaction
