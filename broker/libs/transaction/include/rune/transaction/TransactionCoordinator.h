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
//   PrepareCommit + completeTransaction  → CompleteCommit → Empty
//   PrepareAbort  + completeTransaction  → CompleteAbort  → Empty
//
// ## InitProducerId / Zombie Fencing
//
//   No txnId → allocate fresh PID, epoch 0. No state machine entry created.
//   New txnId → allocate PID, epoch 0; create TransactionMetadata (Empty).
//   Known txnId (re-init) → BUMP epoch (epoch++; zombie producer with old epoch fenced);
//                           if Ongoing, first drive to PrepareAbort (mirror Kafka's
//                           "PrepareEpochFence" path), then reset to Empty.
//   Known txnId in PrepareCommit/PrepareAbort/CompleteCommit/CompleteAbort →
//                           kConcurrentTransactions (Kafka EOS: reject, let in-flight finish).
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

#include <cassert>
#include <cstdint>
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

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// EndTransactionResult — returned by endTransaction()
// ---------------------------------------------------------------------------
struct EndTransactionResult {
    int errorCode{0};

    /// The set of partitions that need COMMIT/ABORT markers written (P5-3).
    std::vector<TopicPartition> partitions;

    static constexpr int kOk                   = 0;
    static constexpr int kInvalidProducerEpoch = 1;
    static constexpr int kInvalidPid           = 2;
    static constexpr int kInvalidTransition    = 3;
    static constexpr int kUnknownTxnId         = 4;

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

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
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
    explicit TransactionCoordinator(ProducerIdAllocator  allocator = ProducerIdAllocator{},
                                    int32_t              defaultTxnTimeoutMs = 60'000,
                                    TransactionLog*      txnLog = nullptr,
                                    int32_t              coordinatorEpoch = 0);

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
    /// \returns {producerId, producerEpoch, errorCode}
    ///   errorCode = kInvalidTxnTimeout if txnTimeoutMs is out of range [1, max].
    InitProducerIdResult initProducerId(std::optional<TransactionalId> txnId,
                                        int32_t                        txnTimeoutMs,
                                        int64_t                        nowNs);

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

    /// Number of known transactionalIds.
    [[nodiscard]] std::size_t size() const noexcept { return txnMap_.size(); }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Look up metadata; returns nullptr if not found.
    [[nodiscard]] TransactionMetadata* findMetadata(const TransactionalId& txnId) noexcept;
    [[nodiscard]] const TransactionMetadata* findMetadata(const TransactionalId& txnId) const noexcept;

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

    /// Primary state: transactionalId → TransactionMetadata.
    std::unordered_map<TransactionalId, TransactionMetadata> txnMap_;
};

} // namespace rune::transaction
