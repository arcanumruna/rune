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
                                               int32_t             coordinatorEpoch,
                                               OffsetApplyCallback offsetApplyCb)
    : allocator_(std::move(allocator))
    , defaultTxnTimeoutMs_(defaultTxnTimeoutMs)
    , txnLog_(txnLog)
    , coordinatorEpoch_(coordinatorEpoch)
    , offsetApplyCb_(std::move(offsetApplyCb))
{
    // Replay the durable log to rebuild in-memory txnMap_ (P5-3 failover recovery).
    // Any entry in Prepare* state on recovery is treated as needing marker resolution
    // (B2): the original request no longer exists; set markersPending so the caller
    // can drive resolution via pendingMarkerTransactions().
    if (txnLog_ != nullptr) {
        // A1: track the highest producerId seen across ALL recovered metadata so
        // we can reseed the allocator past it. This includes the deferred
        // pendingNewProducerId (A2), which was already handed to a client and must
        // never be re-allocated to a different session.
        ProducerId maxPid = -1;
        for (const auto& [id, meta] : txnLog_->all()) {
            txnMap_[id] = meta;
            auto& m = txnMap_[id];
            // B2: recovered Prepare* needs marker resolution after failover.
            if (m.state == TxnState::PrepareAbort || m.state == TxnState::PrepareCommit) {
                m.markersPending = true;
            }
            // LIVENESS FIX (P5-5): a recovered Complete* means all side-effects
            // (markers + offsets) were already durably applied before the Complete*
            // record was persisted.  The only remaining step was the Empty reset,
            // which crashed before it could be written.  Collapsing to Empty here
            // makes the transactionalId immediately reusable — consistent with Kafka
            // treating Complete* as transient.  We do NOT re-apply offsets or
            // re-write markers (both already durable).
            //
            // For CompleteAbort we must also adopt any deferred pendingNewProducerId
            // (NEW-1/A2 overflow path): completeTransaction would have swapped the
            // identity before writing Empty, so we replicate that swap here to keep
            // the recovered PID consistent with what completeTransaction would have
            // produced.
            if (m.state == TxnState::CompleteCommit || m.state == TxnState::CompleteAbort) {
                if (m.state == TxnState::CompleteAbort && m.pendingNewProducerId >= 0) {
                    m.producerId            = m.pendingNewProducerId;
                    m.producerEpoch         = m.pendingNewProducerEpoch;
                    m.pendingNewProducerId    = -1;
                    m.pendingNewProducerEpoch = -1;
                }
                m.state          = TxnState::Empty;
                m.partitions.clear();
                m.pendingOffsets.clear();
                m.txnStartNs     = 0;
                m.fencingEpoch   = false;
                m.markersPending = false;
            }
            if (m.producerId > maxPid)            maxPid = m.producerId;
            if (m.pendingNewProducerId > maxPid)  maxPid = m.pendingNewProducerId;
        }
        // A1: reseed so the next allocation is strictly greater than any live pid.
        // No-op if the injected allocator already starts above maxPid.
        if (maxPid >= 0) {
            allocator_.reseedAtLeast(maxPid);
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
        //
        // NEW-1: the in-flight data on these partitions was written under the OLD
        // producerId/producerEpoch.  A read-committed consumer keys aborted-txn
        // tracking by producerId, so the ABORT marker MUST carry the OLD PID (and
        // an old-PID-consistent epoch) to match that data.  Therefore:
        //   - overflow case: keep OLD producerId + OLD producerEpoch on the
        //     PrepareAbort (so the marker matches in-flight data); stash the fresh
        //     PID/epoch to be adopted only AFTER the abort resolves (completeTxn).
        //   - non-overflow case: same PID with a bumped epoch is Kafka-faithful —
        //     consumers key by PID, which is unchanged, so the bumped-epoch abort
        //     still matches the in-flight data.
        const bool overflow = (newPid != meta->producerId);
        if (overflow) {
            // Keep the OLD identity for the abort marker; defer the new PID.
            meta->pendingNewProducerId    = newPid;
            meta->pendingNewProducerEpoch = newEpoch;   // 0
            // meta->producerId / meta->producerEpoch stay OLD (matches in-flight data).
        } else {
            meta->producerId    = newPid;
            meta->producerEpoch = newEpoch;             // bumped, same PID
        }
        meta->state              = TxnState::PrepareAbort;
        meta->fencingEpoch       = true;
        meta->txnTimeoutMs       = effectiveTimeout;
        meta->coordinatorEpoch   = coordinatorEpoch_;
        meta->markersPending     = true;   // B1: caller must write ABORT markers
        // Persist PrepareAbort to log (durable-before-markers ordering).
        // NOTE: for the overflow case this persists the OLD producerId/epoch, which
        // is exactly what resolvePendingMarkers must stamp on the ABORT batch.
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        // Return the NEW identity to the caller for its NEXT transaction; the OLD
        // PID is retired once the deferred swap is adopted in completeTransaction().
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
    // A7 (justified): nowNs is intentionally unused here. endTransaction only
    // advances Ongoing→Prepare*; the deadline is enforced solely by doWork()
    // (single source of truth for timeout aborts), and re-checking the deadline
    // here would race that path and could reject a commit the client already
    // began. The parameter is kept for API symmetry with the other entry points.
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
// addOffsetsToTxn — transactional consumer-group offset commit (F3)
// ---------------------------------------------------------------------------
AddPartitionsResult TransactionCoordinator::addOffsetsToTxn(
    const TransactionalId&                                              txnId,
    ProducerId                                                           producerId,
    ProducerEpoch                                                        producerEpoch,
    const std::string&                                                   group,
    const std::map<rune::coordination::TopicPartition,
                   rune::coordination::Offset>&                          offsets,
    int64_t                                                              /*nowNs*/)
{
    TransactionMetadata* meta = findMetadata(txnId);
    if (meta == nullptr) {
        return { .errorCode = AddPartitionsResult::kUnknownTxnId };
    }

    // Fence stale epoch / wrong PID (same guard as addPartitions).
    if (int e = validateEpochAndPid(*meta, producerId, producerEpoch); e != 0) {
        return { .errorCode = e };
    }

    // Only valid while the transaction is Ongoing.
    if (meta->state != TxnState::Ongoing) {
        return { .errorCode = AddPartitionsResult::kInvalidTransition };
    }

    // Record each offset as PENDING within the transaction.
    // Persisted to the TransactionLog so they survive a coordinator failover
    // while in PrepareCommit: a recovered PrepareCommit re-applies them on resolve.
    for (const auto& [tp, off] : offsets) {
        meta->pendingOffsets.push_back({group, tp, off});
    }

    // Durable-before-visible: persist the updated metadata (including pendingOffsets)
    // BEFORE any OffsetStore write can occur.
    persistTransition(txnLog_, *meta, coordinatorEpoch_);

    return { .errorCode = AddPartitionsResult::kOk };
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
        // F3 / EOS durability: apply the pending consumer-group offsets to the
        // OffsetStore FIRST, while the state is STILL PrepareCommit — i.e. as part
        // of the RE-DRIVABLE resolution, BEFORE the terminal CompleteCommit is made
        // durable.  This ordering is the crash-safety argument:
        //
        //   - Crash BEFORE the CompleteCommit persist below → the durable state
        //     stays PrepareCommit.  Recovery marks a recovered PrepareCommit as
        //     markersPending (see the constructor), so the re-drive re-runs THIS
        //     path: it re-applies the offsets (OffsetStore::commit is an overwrite →
        //     re-applying the same (group,tp,offset) is idempotent) and re-writes
        //     the markers (resolvePendingMarkers is already idempotent — no double
        //     marker).  → NO committed-offset loss.
        //   - Crash AFTER the CompleteCommit persist → the offsets are already
        //     durably in the OffsetStore, and a recovered CompleteCommit is terminal.
        //     → NO loss.
        //
        // The OLD order (persist CompleteCommit, THEN apply offsets) was buggy: a
        // crash in that window left a durable CompleteCommit whose offsets were
        // never applied, and recovery does NOT re-drive CompleteCommit → the
        // committed consumer offsets were lost (a read-process-write pipeline
        // reprocesses inputs after failover — an exactly-once violation).
        if (offsetApplyCb_) {
            for (const auto& po : meta->pendingOffsets) {
                offsetApplyCb_(po.group, po.tp, po.offset, /*timestampNs=*/0);
            }
        }

        // C2: NOW persist CompleteCommit as a distinct terminal record.  The
        // offsets are already durable in the OffsetStore, so this record makes the
        // commit visible only AFTER the offsets are safe (durable-before-visible).
        meta->state = TxnState::CompleteCommit;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);

        // Transition to Empty (ready for next txn, same epoch).
        meta->pendingOffsets.clear();
        meta->partitions.clear();
        meta->txnStartNs     = 0;
        meta->fencingEpoch   = false;
        meta->markersPending = false;
        meta->state          = TxnState::Empty;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);
        return { .errorCode = CompleteTransactionResult::kOk };
    }

    if (meta->state == TxnState::PrepareAbort) {
        // C2: persist CompleteAbort before clearing.  The ABORT markers were
        // stamped with the CURRENT producerId/epoch — for the NEW-1 overflow case
        // that is still the OLD identity, matching the in-flight data.  We adopt the
        // deferred new PID ONLY here, after the abort has been fully resolved.
        meta->state = TxnState::CompleteAbort;
        persistTransition(txnLog_, *meta, coordinatorEpoch_);

        // F3: Discard pending consumer-group offsets — they must NEVER be written
        // to the OffsetStore on an aborted transaction (EOS invariant).
        meta->pendingOffsets.clear();

        meta->partitions.clear();
        meta->txnStartNs     = 0;
        meta->fencingEpoch   = false;
        meta->markersPending = false;
        // NEW-1: adopt the deferred fresh PID (epoch 0) now that the OLD PID's ABORT
        // markers are done.  The OLD PID is thereby retired.
        if (meta->pendingNewProducerId >= 0) {
            meta->producerId            = meta->pendingNewProducerId;
            meta->producerEpoch         = meta->pendingNewProducerEpoch;
            meta->pendingNewProducerId    = -1;
            meta->pendingNewProducerEpoch = -1;
        }
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
