// ---------------------------------------------------------------------------
// TransactionCoordinator.cpp — Kafka-faithful transaction state machine.
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>

#include <cassert>
#include <cstdint>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t kMinTxnTimeoutMs = 1;
static constexpr int32_t kMaxTxnTimeoutMs = 900'000;  // 15 minutes (matches Kafka default)

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------
TransactionCoordinator::TransactionCoordinator(ProducerIdAllocator allocator,
                                               int32_t             defaultTxnTimeoutMs,
                                               TransactionLog*     txnLog,
                                               int32_t             coordinatorEpoch)
    : allocator_(std::move(allocator))
    , defaultTxnTimeoutMs_(defaultTxnTimeoutMs)
    , txnLog_(txnLog)
    , coordinatorEpoch_(coordinatorEpoch)
{
    // Replay the durable log to rebuild in-memory txnMap_ (P5-3 failover recovery).
    // Any entry in Prepare* state on recovery is treated as needing marker resolution
    // (B2): the original request no longer exists; set markersPending so the caller
    // can drive resolution via pendingMarkerTransactions().
    if (txnLog_ != nullptr) {
        for (const auto& [id, meta] : txnLog_->all()) {
            txnMap_[id] = meta;
            auto& m = txnMap_[id];
            // B2: recovered Prepare* needs marker resolution after failover.
            if (m.state == TxnState::PrepareAbort || m.state == TxnState::PrepareCommit) {
                m.markersPending = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: persist transition to the durable log (fsync before return).
// ---------------------------------------------------------------------------
namespace {
void persistTransition(TransactionLog* log,
                       const TransactionMetadata& meta,
                       int32_t coordinatorEpoch) noexcept
{
    if (log == nullptr) return;
    log->appendFromMetadata(meta, coordinatorEpoch);
}
} // anonymous namespace

// ---------------------------------------------------------------------------
// initProducerId
// ---------------------------------------------------------------------------
InitProducerIdResult TransactionCoordinator::initProducerId(
    std::optional<TransactionalId> txnId,
    int32_t                        txnTimeoutMs,
    int64_t                        nowNs)
{
    // Resolve effective timeout.
    // txnTimeoutMs < 0 → explicitly invalid (mirror Kafka: INVALID_TXN_TIMEOUT_MS).
    // txnTimeoutMs == 0 → caller omitted; use the coordinator's default.
    if (txnTimeoutMs < 0) {
        return { .producerId = -1, .producerEpoch = -1,
                 .errorCode  = InitProducerIdResult::kInvalidTxnTimeout };
    }
    int32_t effectiveTimeout = (txnTimeoutMs == 0) ? defaultTxnTimeoutMs_ : txnTimeoutMs;
    if (effectiveTimeout < kMinTxnTimeoutMs || effectiveTimeout > kMaxTxnTimeoutMs) {
        return { .producerId = -1, .producerEpoch = -1,
                 .errorCode  = InitProducerIdResult::kInvalidTxnTimeout };
    }

    // No transactional id → fresh producer, no state machine entry.
    if (!txnId.has_value() || txnId->empty()) {
        ProducerId pid   = allocator_.allocate();
        return { .producerId = pid, .producerEpoch = 0, .errorCode = 0 };
    }

    const TransactionalId& id = *txnId;
    TransactionMetadata*   meta = findMetadata(id);

    if (meta == nullptr) {
        // Unknown txnId → create new metadata (Empty, epoch 0).
        ProducerId pid = allocator_.allocate();
        TransactionMetadata newMeta;
        newMeta.txnId             = id;
        newMeta.producerId        = pid;
        newMeta.producerEpoch     = 0;
        newMeta.txnTimeoutMs      = effectiveTimeout;
        newMeta.state             = TxnState::Empty;
        newMeta.txnStartNs        = 0;
        newMeta.coordinatorEpoch  = coordinatorEpoch_;
        txnMap_.emplace(id, std::move(newMeta));
        persistTransition(txnLog_, txnMap_.at(id), coordinatorEpoch_);
        return { .producerId = pid, .producerEpoch = 0, .errorCode = 0 };
    }

    // Known txnId — check if a Prepare* or Complete* is in flight.
    // C3/C4: mirror Kafka — return kConcurrentTransactions instead of silently
    // bumping the epoch mid-Prepare*.  The new producer must retry until the
    // in-flight transaction resolves.
    if (meta->state == TxnState::PrepareCommit  ||
        meta->state == TxnState::PrepareAbort   ||
        meta->state == TxnState::CompleteCommit ||
        meta->state == TxnState::CompleteAbort  ||
        meta->state == TxnState::PrepareEpochFence) {
        return { .producerId    = meta->producerId,
                 .producerEpoch = meta->producerEpoch,
                 .errorCode     = InitProducerIdResult::kConcurrentTransactions };
    }

    // Compute the new epoch.
    // C1: Per Kafka, when epoch == INT16_MAX, allocate a NEW producerId (fresh PID,
    // epoch 0) instead of wrapping.  Wrapping to 0 would un-fence epoch-0 zombies.
    ProducerEpoch newEpoch;
    ProducerId    newPid = meta->producerId;
    if (meta->producerEpoch == INT16_MAX) {
        // Epoch space exhausted — allocate a fresh PID.
        newPid   = allocator_.allocate();
        newEpoch = 0;
    } else {
        newEpoch = static_cast<ProducerEpoch>(meta->producerEpoch + 1);
    }

    if (meta->state == TxnState::Ongoing) {
        // Drive ongoing txn to PrepareAbort (epoch-fence path, B1).
        // The partitions are captured in meta->partitions; markersPending is set so
        // pendingMarkerTransactions() exposes them to the caller for marker emission.
        meta->state              = TxnState::PrepareAbort;
        meta->producerId         = newPid;
        meta->producerEpoch      = newEpoch;
        meta->fencingEpoch       = true;
        meta->txnTimeoutMs       = effectiveTimeout;
        meta->coordinatorEpoch   = coordinatorEpoch_;
        meta->markersPending     = true;   // B1: caller must write ABORT markers
        // Persist PrepareAbort to log (durable-before-markers ordering).
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        return { .producerId = newPid, .producerEpoch = newEpoch, .errorCode = 0 };
    }

    // State is Empty (or Dead — treat Dead as re-create).
    // Simply bump epoch and reset to Empty.
    meta->producerId         = newPid;
    meta->producerEpoch      = newEpoch;
    meta->txnTimeoutMs       = effectiveTimeout;
    meta->state              = TxnState::Empty;
    meta->partitions.clear();
    meta->txnStartNs         = 0;
    meta->fencingEpoch       = false;
    meta->markersPending     = false;
    meta->coordinatorEpoch   = coordinatorEpoch_;
    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    return { .producerId = newPid, .producerEpoch = newEpoch, .errorCode = 0 };
}

// ---------------------------------------------------------------------------
// addPartitions
// ---------------------------------------------------------------------------
AddPartitionsResult TransactionCoordinator::addPartitions(
    const TransactionalId&           txnId,
    ProducerId                        producerId,
    ProducerEpoch                     producerEpoch,
    const std::vector<TopicPartition>& partitions,
    int64_t                           nowNs)
{
    TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) {
        return { .errorCode = AddPartitionsResult::kUnknownTxnId };
    }

    // Fence stale epoch / wrong PID.
    if (int e = validateEpochAndPid(*meta, producerId, producerEpoch); e != 0) {
        return { .errorCode = e };
    }

    // Validate state: only Empty and Ongoing are valid for addPartitions.
    if (meta->state != TxnState::Empty && meta->state != TxnState::Ongoing) {
        return { .errorCode = AddPartitionsResult::kInvalidTransition };
    }

    if (meta->state == TxnState::Empty) {
        // Empty → Ongoing: record the transaction start time.
        meta->state      = TxnState::Ongoing;
        meta->txnStartNs = nowNs;
    }

    // Accumulate partitions (std::set deduplicates automatically).
    for (const auto& tp : partitions) {
        meta->partitions.insert(tp);
    }

    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    return { .errorCode = AddPartitionsResult::kOk };
}

// ---------------------------------------------------------------------------
// endTransaction
// ---------------------------------------------------------------------------
EndTransactionResult TransactionCoordinator::endTransaction(
    const TransactionalId& txnId,
    ProducerId              producerId,
    ProducerEpoch           producerEpoch,
    bool                    commit,
    int64_t                 /*nowNs*/)
{
    TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) {
        return { .errorCode = EndTransactionResult::kUnknownTxnId };
    }

    // Fence stale epoch / wrong PID.
    if (int e = validateEpochAndPid(*meta, producerId, producerEpoch); e != 0) {
        return { .errorCode = e };
    }

    // Only Ongoing transitions are valid here.
    if (meta->state != TxnState::Ongoing) {
        return { .errorCode = EndTransactionResult::kInvalidTransition };
    }

    // Collect the partition set to return (P5-3 needs to write markers there).
    std::vector<TopicPartition> parts(meta->partitions.begin(), meta->partitions.end());

    if (commit) {
        meta->state = TxnState::PrepareCommit;
    } else {
        meta->state = TxnState::PrepareAbort;
    }
    // Client-driven: markers will be written by the caller using the returned
    // partitions, NOT via the pending-markers path.
    meta->markersPending = false;

    // CRITICAL atomicity: PrepareCommit/PrepareAbort MUST be persisted+fsynced
    // to the log HERE, BEFORE any markers are written and BEFORE completeTransaction.
    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    return { .errorCode = EndTransactionResult::kOk, .partitions = std::move(parts) };
}

// ---------------------------------------------------------------------------
// completeTransaction
// ---------------------------------------------------------------------------
CompleteTransactionResult TransactionCoordinator::completeTransaction(
    const TransactionalId& txnId)
{
    TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) {
        return { .errorCode = CompleteTransactionResult::kUnknownTxnId };
    }

    if (meta->state == TxnState::PrepareCommit) {
        // C2: persist CompleteCommit as a distinct record BEFORE clearing to Empty.
        // A crash between marker-write and Empty is recoverable: the recovered
        // coordinator sees CompleteCommit and knows markers were already written.
        meta->state = TxnState::CompleteCommit;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        // Transition to Empty (ready for next txn, same epoch).
        meta->partitions.clear();
        meta->txnStartNs     = 0;
        meta->fencingEpoch   = false;
        meta->markersPending = false;
        meta->state          = TxnState::Empty;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        return { .errorCode = CompleteTransactionResult::kOk };
    }

    if (meta->state == TxnState::PrepareAbort) {
        // C2: persist CompleteAbort before clearing.
        meta->state = TxnState::CompleteAbort;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        meta->partitions.clear();
        meta->txnStartNs     = 0;
        meta->fencingEpoch   = false;
        meta->markersPending = false;
        meta->state          = TxnState::Empty;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        return { .errorCode = CompleteTransactionResult::kOk };
    }

    return { .errorCode = CompleteTransactionResult::kInvalidTransition };
}

// ---------------------------------------------------------------------------
// doWork
// ---------------------------------------------------------------------------
int TransactionCoordinator::doWork(int64_t nowNs)
{
    int work = 0;

    // Single pass over all known transactions. Sample nowNs once.
    for (auto& [id, meta] : txnMap_) {
        if (meta.state != TxnState::Ongoing) continue;

        int64_t timeoutNs = static_cast<int64_t>(meta.txnTimeoutMs) * 1'000'000LL;
        if (nowNs - meta.txnStartNs > timeoutNs) {
            meta.state           = TxnState::PrepareAbort;
            meta.markersPending  = true;   // B1: caller must write ABORT markers
            // Persist PrepareAbort to log (durable-before-markers ordering).
            persistTransition(txnLog_, meta, coordinatorEpoch_);
            ++work;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// pendingMarkerTransactions — B1/B2 resolution API
// ---------------------------------------------------------------------------
std::vector<PendingMarkerTxn> TransactionCoordinator::pendingMarkerTransactions() const
{
    std::vector<PendingMarkerTxn> result;
    for (const auto& [id, meta] : txnMap_) {
        if (!meta.markersPending) continue;
        // Only PrepareAbort and PrepareCommit can be pending.
        if (meta.state != TxnState::PrepareAbort &&
            meta.state != TxnState::PrepareCommit) continue;

        PendingMarkerTxn p;
        p.txnId            = id;
        p.producerId       = meta.producerId;
        p.producerEpoch    = meta.producerEpoch;
        p.coordinatorEpoch = meta.coordinatorEpoch;
        p.commit           = (meta.state == TxnState::PrepareCommit);
        p.partitions.assign(meta.partitions.begin(), meta.partitions.end());
        result.push_back(std::move(p));
    }
    return result;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

std::optional<TxnState> TransactionCoordinator::state(const TransactionalId& txnId) const noexcept
{
    const TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) return std::nullopt;
    return meta->state;
}

ProducerEpoch TransactionCoordinator::epochOf(const TransactionalId& txnId) const noexcept
{
    const TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) return -1;
    return meta->producerEpoch;
}

ProducerId TransactionCoordinator::pidOf(const TransactionalId& txnId) const noexcept
{
    const TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) return -1;
    return meta->producerId;
}

std::vector<TopicPartition> TransactionCoordinator::partitionsOf(const TransactionalId& txnId) const
{
    const TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) return {};
    return std::vector<TopicPartition>(meta->partitions.begin(), meta->partitions.end());
}

const TransactionMetadata* TransactionCoordinator::metadata(const TransactionalId& txnId) const noexcept
{
    return findMetadata(txnId);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

TransactionMetadata* TransactionCoordinator::findMetadata(const TransactionalId& txnId) noexcept
{
    auto it = txnMap_.find(txnId);
    if (it == txnMap_.end()) return nullptr;
    return &it->second;
}

const TransactionMetadata* TransactionCoordinator::findMetadata(const TransactionalId& txnId) const noexcept
{
    auto it = txnMap_.find(txnId);
    if (it == txnMap_.end()) return nullptr;
    return &it->second;
}

int TransactionCoordinator::validateEpochAndPid(const TransactionMetadata& meta,
                                                 ProducerId                  pid,
                                                 ProducerEpoch               epoch) const noexcept
{
    // PID mismatch: wrong producer for this txnId.
    if (pid != meta.producerId) {
        return AddPartitionsResult::kInvalidPid;
    }
    // Epoch mismatch: stale producer (zombie) — fenced.
    if (epoch != meta.producerEpoch) {
        return AddPartitionsResult::kInvalidProducerEpoch;
    }
    return 0;
}

} // namespace rune::transaction
