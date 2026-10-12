// ---------------------------------------------------------------------------
// TransactionCoordinationService.cpp — TX-3: ITxnRequestHandler implementation.
//
// Responsibilities (see header for full docs):
//   - Delegate to TransactionCoordinator for state machine transitions.
//   - Persist resulting TransactionMetadata via ReplicatedTxnStateStore (acks=All)
//     BEFORE responding (persist-before-respond contract).
//   - Encode the client-protocol response (TxnMessages.h structs).
//   - NotCoordinator fence on every handler while the coordinator is not
//     Loaded (F15 §5.1 load state machine — see the header).
//   - doWork drives coordinator timeout aborts (marker resolution is Task 8).
//
// Error-code mapping (TransactionCoordinator → Protocol.h):
//   kInvalidProducerEpoch (1) → ErrorCode::InvalidProducerEpoch (17)  [fenced]
//   kInvalidPid          (2) → ErrorCode::ProducerFenced       (18)  [wrong pid]
//   kInvalidTransition   (3) → ErrorCode::InvalidTxnState      (19)
//   kUnknownTxnId        (4) → ErrorCode::InvalidTxnState      (19)  [not found]
//   kConcurrentTransactions (6) → ErrorCode::ConcurrentTransactions (27)  [retriable]
//
// Zero-alloc: txnIdScratch_ / groupIdScratch_ are member std::strings reused
// via assign(). Partition vectors are pre-reserved. TopicPartition topic names
// are resolved from TopicRegistry (may be null → numeric string fallback).
// ---------------------------------------------------------------------------

#include <rune/node/TransactionCoordinationService.h>
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/Types.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <spdlog/spdlog.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <string_view>
#include <unordered_map>

namespace pc = rune::protocol::client;
namespace tx = rune::transaction;

namespace rune::node {

// ---------------------------------------------------------------------------
// Error code translation
// ---------------------------------------------------------------------------
namespace {

// Map TransactionCoordinator error codes to Protocol.h ErrorCode int16 values.
int16_t txnErrorToProtocol(int txnErr) noexcept {
    using pc::ErrorCode;
    switch (txnErr) {
        case 0:  return 0;  // OK
        case 1:  return static_cast<int16_t>(ErrorCode::InvalidProducerEpoch);
        case 2:  return static_cast<int16_t>(ErrorCode::ProducerFenced);
        case 3:  return static_cast<int16_t>(ErrorCode::InvalidTxnState);
        case 4:  return static_cast<int16_t>(ErrorCode::InvalidTxnState);
        case 5:  return static_cast<int16_t>(ErrorCode::InvalidTxnState);
        case 6:  return static_cast<int16_t>(ErrorCode::ConcurrentTransactions);  // retriable (Kafka)
        default: return static_cast<int16_t>(ErrorCode::Internal);
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
TransactionCoordinationService::TransactionCoordinationService(
    tx::ReplicatedTxnStateStore*  txnState,
    rune::gateway::TopicRegistry* topics,
    std::function<bool()>         isLeader,
    std::function<NodeId()>       leaderId)
    : txnState_(txnState)
    , topics_(topics)
    , isLeaderFn_(std::move(isLeader))
    , leaderIdFn_(std::move(leaderId))
    , coordinator_()
{
    // Pre-reserve scratch containers to avoid steady-state allocation.
    partitionScratch_.reserve(32);
    partResultScratch_.reserve(32);
    topicNameScratch_.reserve(32);  // "topic_<N>" stays well within 32 chars
    pendingByPidScratch_.reserve(16);
    toNotifyScratch_.reserve(32);
}

// ---------------------------------------------------------------------------
// isLocalCoordinator
// ---------------------------------------------------------------------------
bool TransactionCoordinationService::isLocalCoordinator() const {
    if (localLeader_ >= 0) return localLeader_ == 1;
    return isLeaderFn_ ? isLeaderFn_() : true;
}

// ---------------------------------------------------------------------------
// coordinatorFor — v1: cluster leader owns all transactions.
// ---------------------------------------------------------------------------
NodeId TransactionCoordinationService::coordinatorFor(const TransactionalId& /*txnId*/) const {
    return leaderIdFn_ ? leaderIdFn_() : NodeId{0};
}

// ---------------------------------------------------------------------------
// F15 §5.1 — coordinator load state machine.
// ---------------------------------------------------------------------------
bool TransactionCoordinationService::loadable_() const
{
    if (!isLocalCoordinator()) return false;            // FindCoordinator routes here
    if (txnState_ == nullptr) return true;              // store-less unit rigs
    const auto* r = txnState_->replica();
    // All plain reads: the txn-state replica is control-owned (BrokerNode
    // drives topicId < 0 replicas on the control thread).
    return r->role() == rune::replication::Role::Leader
        && r->highWatermark() == r->logEndOffset();     // nothing uncommitted above HW (Q2)
}

bool TransactionCoordinationService::stillLoaded_() const
{
    if (!isLocalCoordinator()) return false;
    if (txnState_ == nullptr) return true;
    const auto* r = txnState_->replica();
    // Loaded state is valid only for the leadership AND the log content it was
    // folded from: unload when leadership is lost or the log's existing
    // content changed (a truncation bumps logGeneration) — a reload re-folds.
    return r->role() == rune::replication::Role::Leader &&
           r->logGeneration() == loadedLogGen_ &&
           static_cast<int32_t>(r->epoch()) == coordinatorEpoch_;
}

bool TransactionCoordinationService::ensureLoaded_(int64_t nowNs)
{
    if (loadState_ == LoadState::Loaded) {
        if (stillLoaded_()) {
            advancePidBlock_();
            return true;
        }
        unload_();                                      // an epoch change reloads below
    }
    if (!loadable_()) return false;
    load_(nowNs);
    advancePidBlock_();
    return true;
}

void TransactionCoordinationService::load_(int64_t nowNs)
{
    // While Unloaded this service is the txn-state partition's sole appender
    // and does not append, so HW == LEO means the whole log is committed: fold
    // it (rebuildFromLog reads [0, HW)).  Cold, synchronous, control thread.
    if (txnState_ != nullptr) {
        txnState_->rebuildFromLog();
        coordinatorEpoch_ = static_cast<int32_t>(txnState_->replica()->epoch());
        loadedLogGen_     = txnState_->replica()->logGeneration();
    } else {
        coordinatorEpoch_ = ++localLoads_;
    }
    const tx::ProducerId maxPid =
        coordinator_.reload(txnState_ ? txnState_->all() : noSnapshot_, coordinatorEpoch_, nowNs);
    awaiting_.clear();
    queuedRetries_ = 0;
    stuckMarkers_  = 0;
    nextStuckScanNs_ = 0;
    publishStuckMarkers_();
    if (gateway_) (void)gateway_->purgeQueuedMarkers(coordinatorEpoch_);   // older epochs' leftovers
    if (txnState_ != nullptr) {
        // I7: the next pid lies at or above every ceiling a previous coordinator
        // persisted (idempotent pids are recorded nowhere else) and above every
        // pid in the log.  Nothing is handed out until a fresh ceiling commits.
        tx::ProducerId base = txnState_->persistedPidCeiling();
        if (maxPid + 1 > base) base = maxPid + 1;
        if (base > 0) coordinator_.allocator().reseedAtLeast(base - 1);
        coordinator_.allocator().resetGate();             // gated; no limit from an earlier load
        pidCeilingPending_       = -1;
        pidCeilingPendingOffset_ = -1;
    }
    loadState_ = LoadState::Loaded;
    spdlog::info("TCS: coordinator LOADED (coordinatorEpoch={}, {} transactional ids, next pid {})",
                 coordinatorEpoch_, coordinator_.size(), coordinator_.allocator().nextId());
}

void TransactionCoordinationService::unload_()
{
    // m3: every deferred EndTxn is answered NotCoordinator (the client
    // re-discovers and retries; the new coordinator answers from the durable
    // state), every marker task is dropped.
    for (auto& [pid, a] : awaiting_) {
        if (!a.respPending || gateway_ == nullptr) continue;
        pc::EndTxnResponse resp;
        resp.sessionId     = a.sessionId;
        resp.correlationId = a.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        std::array<uint8_t, pc::EndTxnResponse::ENCODED_SIZE> buf{};
        const std::size_t n = resp.encode(buf.data());
        (void)gateway_->sendControlFrame(a.sessionId, buf.data(), n);
    }
    awaiting_.clear();
    queuedRetries_ = 0;
    stuckMarkers_  = 0;
    nextStuckScanNs_ = 0;
    publishStuckMarkers_();
    if (gateway_) (void)gateway_->purgeQueuedMarkers(coordinatorEpoch_ + 1);   // this (now old) epoch
    coordinator_.clear();
    pidCeilingPending_       = -1;
    pidCeilingPendingOffset_ = -1;
    spdlog::info("TCS: coordinator UNLOADED (coordinatorEpoch={})", coordinatorEpoch_);
    loadState_ = LoadState::Unloaded;
}

bool TransactionCoordinationService::currentEpoch_(int32_t coordinatorEpoch) const noexcept
{
    return loadState_ == LoadState::Loaded &&
           (coordinatorEpoch == rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch ||
            coordinatorEpoch == coordinatorEpoch_);
}

// ---------------------------------------------------------------------------
// F15 §5.6 — persisted pid block ceiling (Kafka ProducerIdManager).
// ---------------------------------------------------------------------------
void TransactionCoordinationService::requestPidCeiling_(int64_t ceiling)
{
    const int64_t off = txnState_->putPidBlockCeiling(ceiling);
    if (off < 0) return;                               // min-ISR: retried on the next pass
    pidCeilingPending_       = ceiling;
    pidCeilingPendingOffset_ = off;
}

void TransactionCoordinationService::advancePidBlock_()
{
    if (txnState_ == nullptr || loadState_ != LoadState::Loaded) return;
    auto& alloc = coordinator_.allocator();
    if (pidCeilingPendingOffset_ < 0) {
        // Prefetch: keep a committed half-block ahead so steady state never waits.
        const int64_t limit = alloc.committedLimit();
        const int64_t next  = alloc.nextId();
        if (limit - next > tx::ProducerIdAllocator::kBlockSize / 2) return;
        requestPidCeiling_((limit > next ? limit : next) + tx::ProducerIdAllocator::kBlockSize);
        if (pidCeilingPendingOffset_ < 0) return;
    }
    // Hand pids out of the new block only once its ceiling record is committed.
    if (txnState_->replica()->highWatermark() > pidCeilingPendingOffset_) {
        alloc.commitLimit(pidCeilingPending_);
        pidCeilingPending_       = -1;
        pidCeilingPendingOffset_ = -1;
    }
}

// ---------------------------------------------------------------------------
// doWork — drive coordinator timeout aborts (only while Loaded).
// ---------------------------------------------------------------------------
int TransactionCoordinationService::doWork(int64_t nowNs) {
    // F15 §5.1: replaces the CG-6 Follower→Leader "rebuild the store" edge — the
    // coordinator is LOADED (store folded to the committed HW + coordinator
    // reloaded) only when the load predicate holds, and serves only then.
    if (!ensureLoaded_(nowNs)) return 0;

    lastNowNs_ = nowNs;
    // Drive coordinator timeout aborts (sets markersPending on timed-out txns).
    int work = coordinator_.doWork(nowNs);
    work += retryRefusedMarkers_(nowNs);
    // Observability only, rate-limited (review LOW-1): never a per-pass walk.
    if ((!awaiting_.empty() && nowNs >= nextStuckScanNs_) || (awaiting_.empty() && stuckMarkers_ != 0)) [[unlikely]] {
        nextStuckScanNs_ = nowNs + kStuckScanIntervalNs;
        updateStuckMarkers_(nowNs);
    }

    // TX-4 Task 8: Resolve pending markers (internally-initiated aborts and the
    // Prepare* re-driven after a load).  Task 10 round 5 (Kafka semantics): the
    // transaction STAYS PrepareAbort / PrepareCommit until every participant
    // partition's marker has LANDED (onControlMarkerLanded); only then
    // completeTransaction.  dispatchMarkers_ hands the markers out and clears
    // markersPending so they are not re-driven.
    const bool haveGateway = (gateway_ != nullptr) && (topics_ != nullptr);
    const bool haveOverride = static_cast<bool>(appendFnOverride_);
    if (haveGateway || haveOverride) {
        const auto pending = coordinator_.pendingMarkerTransactions();
        for (const auto& p : pending) {
            coordinator_.markersDispatched(p.txnId);
            dispatchMarkers_(p.txnId, p.producerId, p.producerEpoch, coordinatorEpoch_,
                             p.commit, p.partitions);
            ++work;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// No-hang fix round 2 (I3): a handler returns 0 ONLY for a deferred answer
// (EndTxn waiting for its markers — ITxnRequestHandler contract).  A response
// that does not fit the caller's buffer is answered MessageTooLarge (its own
// response type, encodeRejectionFor) — never a 0 the gateway would read as
// "deferred" and never answer.
// ---------------------------------------------------------------------------
static size_t answerCannotFit_(const uint8_t* buf, size_t len, uint8_t* out, size_t outCap) noexcept {
    return rune::gateway::encodeRejectionFor(
        buf, len, static_cast<uint16_t>(rune::protocol::client::ErrorCode::MessageTooLarge), out, outCap);
}

// ---------------------------------------------------------------------------
// handleInitProducerId (templateId 45)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleInitProducerId(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::InitProducerId::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered InitProducerIdResponse{InvalidRequest}, never dropped.
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const auto& msg = *msgOpt;

    // Build response shell (sessionId/correlationId always echoed).
    pc::InitProducerIdResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    // NotCoordinator fence (F15 §5.1: served only while Loaded).
    if (!ensureLoaded_(nowNs)) {
        resp.producerId    = -1;
        resp.producerEpoch = -1;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::InitProducerIdResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
        return resp.encode(out);
    }

    // Decode transactionalId as std::optional<std::string_view>.
    std::optional<tx::TransactionalId> txnIdOpt;
    if (!msg.transactionalId.empty()) {
        txnIdScratch_.assign(
            reinterpret_cast<const char*>(msg.transactionalId.data()),
            msg.transactionalId.size());
        txnIdOpt = txnIdScratch_;
    }

    // F15 I1: the pid block ceiling's reserved compaction key is never a
    // transactional id (a txn record under it would be compacted against the
    // ceiling).  Non-retriable, nothing created or persisted.
    if (txnIdOpt.has_value() && tx::ReplicatedTxnStateStore::isReservedKey(*txnIdOpt)) {
        resp.producerId    = -1;
        resp.producerEpoch = -1;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::InvalidTxnState);
        if (outCap < pc::InitProducerIdResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
        return resp.encode(out);
    }

    // Call coordinator.
    const auto result = coordinator_.initProducerId(
        txnIdOpt, msg.transactionTimeoutMs, nowNs);

    // F15 I7: the allocation may have crossed half a block — prefetch the next.
    advancePidBlock_();

    // Persist metadata BEFORE responding (persist-before-respond).
    if (result.ok() && txnIdOpt.has_value()) {
        const auto* meta = coordinator_.metadata(*txnIdOpt);
        if (meta && txnState_) {
            // Ignore persist failure here: the coordinator already updated
            // in-memory state; a log failure is a degraded-durability condition
            // not a protocol error (minIsr-gate on startup would have prevented this).
            txnState_->put(*meta, coordinatorEpoch_);
        }
    }

    resp.producerId    = result.producerId;
    resp.producerEpoch = result.producerEpoch;
    resp.errorCode     = txnErrorToProtocol(result.errorCode);
    if (outCap < pc::InitProducerIdResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleAddPartitions (templateId 47)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleAddPartitions(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::AddPartitionsToTxn::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered AddPartitionsToTxnResponse{InvalidRequest}, never dropped.
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const auto& msg = *msgOpt;

    // NotCoordinator fence — build minimal response (F15 §5.1: only while Loaded).
    if (!ensureLoaded_(nowNs)) {
        // Encode all partitions with NotCoordinator using partResultScratch_ directly
        // (no per-call vector alloc — member scratch cleared+refilled).
        const size_t partCount = msg.partitions.size();
        const size_t needed = pc::AddPartitionsToTxnResponse::encodedSizeFor(partCount);
        if (outCap < needed) return answerCannotFit_(buf, len, out, outCap);

        partResultScratch_.clear();
        for (size_t i = 0; i < partCount; ++i) {
            auto tp = msg.partitions[i];
            partResultScratch_.push_back({
                tp.topicId,
                tp.partition,
                static_cast<int16_t>(pc::ErrorCode::NotCoordinator)
            });
        }

        pc::AddPartitionsToTxnResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::TxnPartitionResult>(partResultScratch_));
    }

    // Decode txnId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());

    // Build partition vector.  Protocol sends topicId (int32) + partition (int32).
    // TransactionCoordinator needs topic name strings. Resolve via TopicRegistry
    // if available; otherwise use numeric placeholder (e.g. "topic_<id>").
    // topicNameScratch_ reused for fallback — avoids per-partition temp string alloc.
    partitionScratch_.clear();
    const size_t partCount = msg.partitions.size();
    for (size_t i = 0; i < partCount; ++i) {
        auto tp = msg.partitions[i];
        tx::TopicPartition txtp;
        txtp.partition = tp.partition;
        if (topics_) {
            const auto* spec = topics_->topic(tp.topicId);
            if (spec) {
                txtp.topic = spec->name;
            } else {
                // Unknown topic: numeric placeholder via scratch (no temp alloc).
                char numBuf[32];
                const int n = std::snprintf(numBuf, sizeof(numBuf), "topic_%d", tp.topicId);
                topicNameScratch_.assign(numBuf, static_cast<size_t>(n > 0 ? n : 0));
                txtp.topic = topicNameScratch_;
            }
        } else {
            char numBuf[32];
            const int n = std::snprintf(numBuf, sizeof(numBuf), "topic_%d", tp.topicId);
            topicNameScratch_.assign(numBuf, static_cast<size_t>(n > 0 ? n : 0));
            txtp.topic = topicNameScratch_;
        }
        partitionScratch_.push_back(std::move(txtp));
    }

    const auto result = coordinator_.addPartitions(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        partitionScratch_, nowNs);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, coordinatorEpoch_);
        }
    }

    // Encode response: one result entry per partition with the same error code.
    // Reuse partResultScratch_ (pre-reserved member) — no per-call alloc.
    const int16_t errCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    const size_t needed = pc::AddPartitionsToTxnResponse::encodedSizeFor(partCount);
    if (outCap < needed) return answerCannotFit_(buf, len, out, outCap);

    partResultScratch_.clear();
    for (size_t i = 0; i < partCount; ++i) {
        auto tp = msg.partitions[i];
        partResultScratch_.push_back({tp.topicId, tp.partition, errCode});
    }

    pc::AddPartitionsToTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, std::span<const pc::TxnPartitionResult>(partResultScratch_));
}

// ---------------------------------------------------------------------------
// handleAddOffsets (templateId 49)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleAddOffsets(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::AddOffsetsToTxn::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered AddOffsetsToTxnResponse{InvalidRequest}, never dropped.
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const auto& msg = *msgOpt;

    pc::AddOffsetsToTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    if (!ensureLoaded_(nowNs)) {
        resp.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::AddOffsetsToTxnResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
        return resp.encode(out);
    }

    // Decode txnId and groupId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()),
        msg.groupId.size());

    // addOffsetsToTxn registers the group so it's included in the commit.
    // The actual offset values are sent via TxnOffsetCommit (templateId 51).
    // We pass an empty offsets map here — the group binding is what matters.
    // AddOffsetsToTxn is a "register intent" call; offsets arrive in TxnOffsetCommit.
    // Reuse offsetsScratch_ cleared as the empty map — no per-call map alloc.
    // X-1: AddOffsetsToTxn opens the transaction from Empty / Complete* (Kafka).
    offsetsScratch_.clear();
    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        groupIdScratch_, offsetsScratch_, nowNs, /*beginsTxn=*/true);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, coordinatorEpoch_);
        }
    }

    resp.errorCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    if (outCap < pc::AddOffsetsToTxnResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleTxnOffsetCommit (templateId 51)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleTxnOffsetCommit(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::TxnOffsetCommit::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered TxnOffsetCommitResponse{InvalidRequest}, never dropped.
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const auto& msg = *msgOpt;

    // NotCoordinator fence — encode all entries with NotCoordinator using
    // partResultScratch_ directly (no per-call vector alloc).
    if (!ensureLoaded_(nowNs)) {
        const size_t entryCount = msg.entries.size();
        const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
        if (outCap < needed) return answerCannotFit_(buf, len, out, outCap);

        partResultScratch_.clear();
        for (const auto& e : msg.entries) {
            partResultScratch_.push_back({e.topicId, e.partition,
                           static_cast<int16_t>(pc::ErrorCode::NotCoordinator)});
        }
        pc::TxnOffsetCommitResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::TxnPartitionResult>(partResultScratch_));
    }

    // Decode txnId and groupId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()),
        msg.groupId.size());

    // Build an offset map from the entries, reusing offsetsScratch_ (member map
    // cleared+refilled each call — no per-call map construction alloc).
    // topicNameScratch_ used for "topic_<id>" fallback (no per-entry temp string).
    offsetsScratch_.clear();
    for (const auto& e : msg.entries) {
        rune::coordination::TopicPartition tp;
        if (topics_) {
            const auto* spec = topics_->topic(e.topicId);
            if (spec) {
                tp.topic = spec->name;
            } else {
                char numBuf[32];
                const int n = std::snprintf(numBuf, sizeof(numBuf), "topic_%d", e.topicId);
                topicNameScratch_.assign(numBuf, static_cast<size_t>(n > 0 ? n : 0));
                tp.topic = topicNameScratch_;
            }
        } else {
            char numBuf[32];
            const int n = std::snprintf(numBuf, sizeof(numBuf), "topic_%d", e.topicId);
            topicNameScratch_.assign(numBuf, static_cast<size_t>(n > 0 ? n : 0));
            tp.topic = topicNameScratch_;
        }
        tp.partition = e.partition;
        offsetsScratch_[tp] = e.offset;
    }

    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        groupIdScratch_, offsetsScratch_, nowNs);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, coordinatorEpoch_);
        }
    }

    // Build result entries — all same error code.
    // Reuse partResultScratch_ (pre-reserved member) — no per-call vector alloc.
    const int16_t errCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    const size_t entryCount = msg.entries.size();
    const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
    if (outCap < needed) return answerCannotFit_(buf, len, out, outCap);

    partResultScratch_.clear();
    for (const auto& e : msg.entries) {
        partResultScratch_.push_back({e.topicId, e.partition, errCode});
    }

    pc::TxnOffsetCommitResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, std::span<const pc::TxnPartitionResult>(partResultScratch_));
}

// ---------------------------------------------------------------------------
// handleEndTxn (templateId 53)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleEndTxn(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::EndTxn::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered EndTxnResponse{InvalidRequest}, never dropped.
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const auto& msg = *msgOpt;

    pc::EndTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    if (!ensureLoaded_(nowNs)) {
        resp.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::EndTxnResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
        return resp.encode(out);
    }

    // Decode txnId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());

    const bool commit = (msg.commit != 0);
    const auto result = coordinator_.endTransaction(
        txnIdScratch_, msg.producerId, msg.producerEpoch, commit, nowNs);

    // F15 §5.4: an OK for a same-direction retry of a COMPLETED transaction is
    // the idempotent answer — no transition, nothing persisted, no markers.
    const bool transitioned = result.ok() && !result.alreadyComplete;

    // Persist before responding (endTxn sets PREPARE state — durable).
    if (transitioned) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, coordinatorEpoch_);
        }
    }

    // TX-4 Task 8 / Task 10 round 5: write the control markers for a client-
    // driven commit/abort.  The txn stays Prepare* and the EndTxn RESPONSE IS
    // DEFERRED until every marker has landed — the producer cannot start its
    // next transaction (whose batches could otherwise land ahead of a marker, or
    // be closed by it in the LSO bookkeeping) before then.
    if (transitioned && gateway_ && topics_) {
        const auto* m = coordinator_.metadata(txnIdScratch_);
        if (m) {
            const std::vector<tx::TopicPartition> parts(m->partitions.begin(), m->partitions.end());
            const bool isCommit = (m->state == tx::TxnState::PrepareCommit);
            const tx::TransactionalId txnId = txnIdScratch_;
            if (dispatchMarkers_(txnId, m->producerId, m->producerEpoch, coordinatorEpoch_,
                                 isCommit, parts, msg.sessionId, msg.correlationId)) {
                return 0;   // response sent by completeAwaiting_ when the last marker lands
            }
        }
    }

    // F15 §5.4: the coordinator implements the whole EndTxn idempotency table
    // (Prepare* same direction 27 / opposite 19, Complete* same direction 0 /
    // opposite 19 or 18) — no InvalidTxnState→27 remapping here.
    resp.errorCode = txnErrorToProtocol(result.errorCode);
    if (outCap < pc::EndTxnResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// Task 10 round 5 — asynchronous marker completion.
// ---------------------------------------------------------------------------
int TransactionCoordinationService::concurrentIfPreparing_(int err) const
{
    // Kafka CONCURRENT_TRANSACTIONS: a request the state machine rejects only
    // because the previous transaction is still writing its markers is
    // RETRIABLE — not an InvalidTxnState.
    if (err != 3) return err;   // kInvalidTransition
    const auto* m = coordinator_.metadata(txnIdScratch_);
    if (m && (m->state == tx::TxnState::PrepareCommit || m->state == tx::TxnState::PrepareAbort ||
              m->state == tx::TxnState::PrepareEpochFence))
        return 6;               // kConcurrentTransactions
    return err;
}

bool TransactionCoordinationService::dispatchMarkers_(
    const tx::TransactionalId& txnId, int64_t producerId, int16_t producerEpoch,
    int32_t coordinatorEpoch, bool commit, const std::vector<tx::TopicPartition>& parts,
    int64_t respSessionId, int64_t respCorrelationId)
{
    AwaitingMarkers& a = awaiting_[producerId];
    a.txnId = txnId;
    a.producerEpoch    = producerEpoch;
    a.coordinatorEpoch = coordinatorEpoch;
    a.commit           = commit;
    a.dispatchedNs     = lastNowNs_;   // D9 re-review MEDIUM-2 (0 = stamped by the first gauge scan)
    const tx::ControlType ct = commit ? tx::ControlType::Commit : tx::ControlType::Abort;
    for (const auto& tp : parts) {
        const auto batch = tx::ControlMarker::build(producerId, producerEpoch, ct, coordinatorEpoch);
        if (gateway_ && topics_) {
            const auto topicIdOpt = topics_->topicIdForName(tp.topic);
            if (!topicIdOpt.has_value()) {
                spdlog::warn("TCS: marker for unknown topic '{}' partition={} — skipped", tp.topic, tp.partition);
                continue;
            }
            const int64_t off = gateway_->appendControlRecord(
                *topicIdOpt, tp.partition, std::span<const uint8_t>(batch.data(), batch.size()),
                producerId, commit, coordinatorEpoch);
            if (off == rune::gateway::ClientGateway::kControlRecordAsync) {
                ++a.outstanding;                       // lands later: onControlMarkerLanded
            } else if (off >= 0) {
                gateway_->notifyControlMarker(*topicIdOpt, tp.partition, producerId, off, commit);
            } else {
                // Refused synchronously: the marker is NOT in the log — it stays outstanding and is
                // retried (dropped only if the partition is gone; see retryRefusedMarkers_).
                spdlog::warn("TCS: appendControlRecord refused topicId={} partition={} — retrying",
                             *topicIdOpt, tp.partition);
                ++a.outstanding;
                queueRetry_(a, *topicIdOpt, tp.partition);
            }
        } else if (appendFnOverride_) {
            // TEST SEAM: the override returns an APPEND offset only — a marker
            // counts toward completion once COMMITTED (§4), which the seam
            // cannot observe.  So the transaction stays Prepare* (awaiting) and
            // is dispatched exactly once; it is never completed by the seam.
            (void)appendFnOverride_(tp.topic, tp.partition,
                                    std::span<const uint8_t>(batch.data(), batch.size()));
            ++a.outstanding;
        }
    }
    if (a.outstanding == 0) {
        completeAwaiting_(producerId, /*respond=*/false);
        return false;                                  // completed synchronously
    }
    if (respSessionId != 0 || respCorrelationId != 0) {
        a.respPending   = true;
        a.sessionId     = respSessionId;
        a.correlationId = respCorrelationId;
    }
    return true;
}

void TransactionCoordinationService::resolveMarker_(int64_t producerId)
{
    auto it = awaiting_.find(producerId);
    if (it == awaiting_.end() || it->second.outstanding <= 0) return;
    if (--it->second.outstanding == 0) completeAwaiting_(producerId, /*respond=*/true);
}

void TransactionCoordinationService::completeAwaiting_(int64_t producerId, bool respond)
{
    auto it = awaiting_.find(producerId);
    if (it == awaiting_.end()) return;
    const AwaitingMarkers a = std::move(it->second);
    awaiting_.erase(it);
    const auto r = coordinator_.completeTransaction(a.txnId);
    ++transactionsCompleted_;
    // F15 m4: persist the resting Complete* (offsets were applied first, inside
    // completeTransaction).  It need not commit before the EndTxn OK: if it is
    // lost, the next coordinator loads Prepare*, re-drives the (idempotent)
    // markers and offsets, and completes again.
    if (r.ok() && txnState_ != nullptr) {
        if (const auto* meta = coordinator_.metadata(a.txnId)) (void)txnState_->put(*meta, coordinatorEpoch_);
    }
    if (respond && a.respPending && gateway_) {
        pc::EndTxnResponse resp;
        resp.sessionId     = a.sessionId;
        resp.correlationId = a.correlationId;
        resp.errorCode     = txnErrorToProtocol(r.errorCode);
        std::array<uint8_t, pc::EndTxnResponse::ENCODED_SIZE> buf{};
        const std::size_t n = resp.encode(buf.data());
        (void)gateway_->sendControlFrame(a.sessionId, buf.data(), n);
    }
}

void TransactionCoordinationService::onControlMarkerLanded(int32_t topicId, int32_t partition, int64_t producerId,
                                                           int32_t coordinatorEpoch)
{
    // m3: a reply for a marker dispatched by an earlier load matches no task.
    if (!currentEpoch_(coordinatorEpoch)) return;
    if (auto it = awaiting_.find(producerId); it != awaiting_.end()) (void)dropRetry_(it->second, topicId, partition);
    resolveMarker_(producerId);
}

void TransactionCoordinationService::onControlMarkerRefused(int32_t topicId, int32_t partition,
                                                            int64_t producerId, int16_t errorCode,
                                                            int32_t coordinatorEpoch)
{
    ++controlMarkersRefused_;
    if (!currentEpoch_(coordinatorEpoch)) return;   // m3: an earlier load's marker
    namespace pcl = rune::protocol::client;
    const auto err = static_cast<pcl::ErrorCode>(errorCode);
    auto it = awaiting_.find(producerId);
    if (err == pcl::ErrorCode::UnknownTopic || err == pcl::ErrorCode::UnknownPartition) {
        // Kafka parity: a partition that no longer exists takes no marker — drop it.
        spdlog::warn("TCS: control marker for a deleted partition topic={} partition={} producerId={} err={} "
                     "— dropped", topicId, partition, producerId, errorCode);
        if (it != awaiting_.end()) (void)dropRetry_(it->second, topicId, partition);
        resolveMarker_(producerId);
        return;
    }
    if (it == awaiting_.end()) return;
    if (errorCode == rune::transaction::kMarkerCoordinatorFencedError) {
        // D9 review MAJOR-2 (Kafka TRANSACTION_COORDINATOR_FENCED): a NEWER
        // coordinator already wrote a marker for this producer there — this load
        // is deposed.  Never retried, never counted as landed: the transaction
        // stays with the newer coordinator (this one unloads on its own edge).
        spdlog::error("TCS: control marker topic={} partition={} producerId={} FENCED by a newer coordinator "
                      "(this epoch {}) — not retried", topicId, partition, producerId, coordinatorEpoch);
        (void)dropRetry_(it->second, topicId, partition);
        return;
    }
    // Every other refusal (NotLeader, MinIsrViolation, Internal, StorageError, ...) is retried: the
    // marker is NOT in the log, so the transaction must stay Prepare* (EndTxn deferred, the next
    // transaction refused ConcurrentTransactions) until a retry lands.
    queueRetry_(it->second, topicId, partition);
}

void TransactionCoordinationService::queueRetry_(AwaitingMarkers& a, int32_t topicId, int32_t partition)
{
    for (auto& r : a.retries) {
        if (r.topicId == topicId && r.partition == partition) {   // refused again: back off further
            if (!r.inFlight) return;                                // already queued
            r.inFlight  = false;
            const bool wasAtMax = r.backoffNs == kMarkerRetryMaxNs;
            r.backoffNs = std::min(r.backoffNs * 2, kMarkerRetryMaxNs);
            r.dueNs     = lastNowNs_ + r.backoffNs;
            ++queuedRetries_;
            if (r.backoffNs == kMarkerRetryMaxNs && !wasAtMax)   // once per marker, at the max backoff
                spdlog::warn("TCS: control marker topic={} partition={} still refused — retrying every {} ms",
                             topicId, partition, kMarkerRetryMaxNs / 1'000'000);
            return;
        }
    }
    // firstRefusedNs: 0 = "not stamped yet" (refused before the first doWork);
    // the gauge scan stamps it with the then-current clock (review LOW-2).
    a.retries.push_back(MarkerRetry{topicId, partition, lastNowNs_ + kMarkerRetryMinNs, kMarkerRetryMinNs, false,
                                    lastNowNs_});
    ++queuedRetries_;
}

void TransactionCoordinationService::updateStuckMarkers_(int64_t nowNs)
{
    std::size_t stuck = 0;
    for (auto& [pid, a] : awaiting_) {
        for (auto& r : a.retries) {
            if (r.firstRefusedNs <= 0) r.firstRefusedNs = nowNs;   // refused before the clock started
            stuck += (nowNs - r.firstRefusedNs >= kStuckMarkerNs) ? 1u : 0u;
        }
        // Re-review NEW MEDIUM-2: markers never refused but still unresolved
        // (in flight / parked on the leader's HW) past the same window.
        if (a.dispatchedNs <= 0) a.dispatchedNs = nowNs;
        const auto firstAttempts = static_cast<std::size_t>(std::max(0, a.outstanding)) > a.retries.size()
                                       ? static_cast<std::size_t>(a.outstanding) - a.retries.size() : 0u;
        if (firstAttempts > 0 && nowNs - a.dispatchedNs >= kStuckMarkerNs) stuck += firstAttempts;
    }
    if (stuck != 0 && stuckMarkers_ == 0)
        spdlog::warn("TCS: {} control marker(s) stuck — refused for more than {} s and still retrying "
                     "(their transactions stay Prepare*)", stuck, kStuckMarkerNs / 1'000'000'000);
    else if (stuck == 0 && stuckMarkers_ != 0)
        spdlog::info("TCS: no stuck control markers any more");
    stuckMarkers_ = stuck;
    publishStuckMarkers_();
}

void TransactionCoordinationService::publishStuckMarkers_()
{
    const auto v = static_cast<int64_t>(stuckMarkers_);
    if (stuckSink_ != nullptr && v != stuckPublished_ && stuckSink_(stuckSinkCtx_, v)) stuckPublished_ = v;
}

bool TransactionCoordinationService::dropRetry_(AwaitingMarkers& a, int32_t topicId, int32_t partition)
{
    for (std::size_t i = 0; i < a.retries.size(); ++i) {
        if (a.retries[i].topicId == topicId && a.retries[i].partition == partition) {
            if (!a.retries[i].inFlight) --queuedRetries_;
            a.retries.erase(a.retries.begin() + static_cast<std::ptrdiff_t>(i));
            return true;
        }
    }
    return false;
}

bool TransactionCoordinationService::partitionExists_(int32_t topicId, int32_t partition) const
{
    if (topics_ == nullptr) return true;   // no metadata to consult: keep retrying
    const auto* spec = topics_->topic(topicId);
    return spec != nullptr && partition >= 0 && partition < spec->partitionCount;
}

int TransactionCoordinationService::retryRefusedMarkers_(int64_t nowNs)
{
    if (gateway_ == nullptr || queuedRetries_ == 0) return 0;
    int work = 0;
    std::vector<int64_t> resolved;   // cold path (refusals only): resolve after the walk
    for (auto& [producerId, a] : awaiting_) {
        for (std::size_t i = 0; i < a.retries.size();) {
            MarkerRetry& r = a.retries[i];
            if (r.inFlight || r.dueNs > nowNs) { ++i; continue; }
            ++work;
            if (!partitionExists_(r.topicId, r.partition)) {   // deleted meanwhile: Kafka drops the marker
                spdlog::warn("TCS: control marker for a deleted partition topic={} partition={} producerId={} "
                             "— dropped", r.topicId, r.partition, producerId);
                --queuedRetries_;
                a.retries.erase(a.retries.begin() + static_cast<std::ptrdiff_t>(i));
                resolved.push_back(producerId);
                continue;
            }
            const tx::ControlType ct = a.commit ? tx::ControlType::Commit : tx::ControlType::Abort;
            const auto batch = tx::ControlMarker::build(producerId, a.producerEpoch, ct, a.coordinatorEpoch);
            const int64_t off = gateway_->appendControlRecord(
                r.topicId, r.partition, std::span<const uint8_t>(batch.data(), batch.size()),
                producerId, a.commit, a.coordinatorEpoch);
            if (off == rune::gateway::ClientGateway::kControlRecordAsync) {
                r.inFlight = true;            // reply pending: the entry keeps its backoff
                --queuedRetries_;
                ++i;
            } else if (off >= 0) {
                gateway_->notifyControlMarker(r.topicId, r.partition, producerId, off, a.commit);
                --queuedRetries_;
                a.retries.erase(a.retries.begin() + static_cast<std::ptrdiff_t>(i));
                resolved.push_back(producerId);
            } else {
                r.backoffNs = std::min(r.backoffNs * 2, kMarkerRetryMaxNs);
                r.dueNs     = nowNs + r.backoffNs;
                ++i;
            }
        }
    }
    for (const int64_t pid : resolved) resolveMarker_(pid);
    return work;
}

} // namespace rune::node
