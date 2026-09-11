// ---------------------------------------------------------------------------
// TransactionCoordinator.cpp — Kafka-faithful transaction state machine.
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionCoordinator.h>

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
                                               int32_t             defaultTxnTimeoutMs)
    : allocator_(std::move(allocator))
    , defaultTxnTimeoutMs_(defaultTxnTimeoutMs)
{}

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
        newMeta.txnId        = id;
        newMeta.producerId   = pid;
        newMeta.producerEpoch = 0;
        newMeta.txnTimeoutMs = effectiveTimeout;
        newMeta.state        = TxnState::Empty;
        newMeta.txnStartNs   = 0;
        txnMap_.emplace(id, std::move(newMeta));
        return { .producerId = pid, .producerEpoch = 0, .errorCode = 0 };
    }

    // Known txnId — bump the epoch to fence the previous producer.
    // Mirror Kafka's handleInitProducerId:
    //   If state is Ongoing → must complete the abort before assigning new epoch.
    //   Kafka uses PrepareEpochFence as an intermediate state; here we transition
    //   directly to PrepareAbort (the "fencing abort") and let the caller drive
    //   completeTransaction() once markers are written (P5-3).
    //   After completeTransaction() the metadata is Empty and the new epoch is set.

    // Compute the new epoch (saturating at INT16_MAX to avoid overflow).
    ProducerEpoch newEpoch = static_cast<ProducerEpoch>(
        (meta->producerEpoch == INT16_MAX) ? 0 : meta->producerEpoch + 1);

    if (meta->state == TxnState::Ongoing) {
        // Drive ongoing txn to PrepareAbort (epoch-fence path).
        // We stash the new epoch in the metadata but keep PrepareAbort as the
        // state — completeTransaction will finalise and reset to Empty.
        meta->state        = TxnState::PrepareAbort;
        meta->producerEpoch = newEpoch;
        meta->fencingEpoch  = true;
        meta->txnTimeoutMs  = effectiveTimeout;
        // Return the new PID + epoch immediately so the new producer can start
        // as soon as the abort is complete (mirrors Kafka behaviour).
        return { .producerId = meta->producerId, .producerEpoch = newEpoch, .errorCode = 0 };
    }

    if (meta->state == TxnState::PrepareAbort ||
        meta->state == TxnState::PrepareCommit ||
        meta->state == TxnState::CompleteAbort ||
        meta->state == TxnState::CompleteCommit ||
        meta->state == TxnState::PrepareEpochFence) {
        // A previous commit/abort is in flight — the epoch bump is safe to
        // apply here (Kafka does the same: bump epoch regardless, assign the
        // new epoch, and once the in-flight transitions complete the next txn
        // will use the new epoch). But we don't re-drive the state machine.
        meta->producerEpoch = newEpoch;
        meta->txnTimeoutMs  = effectiveTimeout;
        return { .producerId = meta->producerId, .producerEpoch = newEpoch, .errorCode = 0 };
    }

    // State is Empty (or Dead — treat Dead as re-create).
    // Simply bump epoch and reset to Empty.
    meta->producerEpoch = newEpoch;
    meta->txnTimeoutMs  = effectiveTimeout;
    meta->state         = TxnState::Empty;
    meta->partitions.clear();
    meta->txnStartNs    = 0;
    meta->fencingEpoch  = false;

    return { .producerId = meta->producerId, .producerEpoch = newEpoch, .errorCode = 0 };
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
        meta->state = TxnState::CompleteCommit;
        // Immediately transition to Empty (ready for next txn, same epoch).
        meta->partitions.clear();
        meta->txnStartNs  = 0;
        meta->fencingEpoch = false;
        meta->state       = TxnState::Empty;
        return { .errorCode = CompleteTransactionResult::kOk };
    }

    if (meta->state == TxnState::PrepareAbort) {
        meta->state = TxnState::CompleteAbort;
        // Transition to Empty (ready for next txn).
        meta->partitions.clear();
        meta->txnStartNs  = 0;
        meta->fencingEpoch = false;
        meta->state       = TxnState::Empty;
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
            meta.state = TxnState::PrepareAbort;
            ++work;
        }
    }

    return work;
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
