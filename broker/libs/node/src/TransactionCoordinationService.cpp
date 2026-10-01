// ---------------------------------------------------------------------------
// TransactionCoordinationService.cpp — TX-3: ITxnRequestHandler implementation.
//
// Responsibilities (see header for full docs):
//   - Delegate to TransactionCoordinator for state machine transitions.
//   - Persist resulting TransactionMetadata via ReplicatedTxnStateStore (acks=All)
//     BEFORE responding (persist-before-respond contract).
//   - Encode the client-protocol response (TxnMessages.h structs).
//   - NotCoordinator fence on every handler when isLocalCoordinator()==false.
//   - doWork drives coordinator timeout aborts (marker resolution is Task 8).
//
// Error-code mapping (TransactionCoordinator → Protocol.h):
//   kInvalidProducerEpoch (1) → ErrorCode::InvalidProducerEpoch (17)  [fenced]
//   kInvalidPid          (2) → ErrorCode::ProducerFenced       (18)  [wrong pid]
//   kInvalidTransition   (3) → ErrorCode::InvalidTxnState      (19)
//   kUnknownTxnId        (4) → ErrorCode::InvalidTxnState      (19)  [not found]
//   kConcurrentTransactions (6) → ErrorCode::ConcurrentTransactions (20)
//
// Zero-alloc: txnIdScratch_ / groupIdScratch_ are member std::strings reused
// via assign(). Partition vectors are pre-reserved. TopicPartition topic names
// are resolved from TopicRegistry (may be null → numeric string fallback).
// ---------------------------------------------------------------------------

#include <rune/node/TransactionCoordinationService.h>
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
// doWork — drive coordinator timeout aborts.
// ---------------------------------------------------------------------------
int TransactionCoordinationService::doWork(int64_t nowNs) {
    // CG-6 pattern: detect Follower→Leader TRANSITION (not every doWork while
    // already leader) to rebuild the in-memory txn index from the replicated log.
    // Guard mirrors GroupCoordinationService::doWork exactly:
    //   - rebuild fires once on the first duty cycle after becoming leader.
    //   - losing leadership resets wasLeader_ so the next election triggers again.
    // Without this guard, rebuildFromLog() would clear index_ every cycle,
    // wiping freshly-put in-memory state before TX-4 marker resolution completes.
    const bool leader = isLocalCoordinator();
    if (leader && !wasLeader_) {
        // Follower→Leader: rebuild index from replicated log.
        if (txnState_) txnState_->rebuildFromLog();
        wasLeader_ = true;
    } else if (!leader && wasLeader_) {
        // Leader→Follower: reset so the next election triggers another rebuild.
        wasLeader_ = false;
    }

    if (!leader) return 0;

    // Drive coordinator timeout aborts (sets markersPending on timed-out txns).
    int work = coordinator_.doWork(nowNs);

    // TX-4 Task 8: Resolve pending markers (internally-initiated aborts).
    //
    // pendingMarkerTransactions() returns all PrepareCommit/PrepareAbort entries
    // whose markersPending==true (set by doWork() timeout path or epoch-fence
    // abort in initProducerId).  For each, ControlMarker::resolvePendingMarkers()
    // builds a COMMIT or ABORT control batch for every partition in the txn and
    // calls appendFn(tp, batch).
    //
    // We:
    //   1. Snapshot pendingMarkerTransactions() BEFORE resolving to get the
    //      commit/abort info for each (producerId, partition) — needed to feed
    //      notifyControlMarker() after the append.
    //   2. Build an appendFn that:
    //        a. Looks up topicId from the topic name via topics_->topicIdForName.
    //        b. Calls gateway_->appendControlRecord(topicId, partition, bytes).
    //        c. Records the (topicId, partition, producerId, markerOffset, commit)
    //           in toNotifyScratch_ for notifyControlMarker() calls.
    //   3. Call resolvePendingMarkers() (which also calls completeTransaction()).
    //   4. Feed notifyControlMarker() for all successfully-appended markers.
    //
    // Design note: we do NOT call notifyControlMarker() inside appendFn because
    // completeTransaction() is called AFTER all appendFn calls for a txn — we
    // want both appendFn and completeTransaction to succeed before feeding LSO.
    //
    // Zero-alloc: pendingByPidScratch_ and toNotifyScratch_ are pre-reserved
    // member containers (clear+refill each cycle, no heap alloc in steady state).
    //
    // Single-writer (duty-cycle thread): no locks needed.
    // Task 10 round 5 (Kafka semantics): the transaction STAYS PrepareAbort /
    // PrepareCommit until every participant partition's marker has LANDED
    // (onControlMarkerLanded); only then completeTransaction.  dispatchMarkers_
    // hands the markers out and clears markersPending so they are not re-driven.
    const bool haveGateway = (gateway_ != nullptr) && (topics_ != nullptr);
    const bool haveOverride = static_cast<bool>(appendFnOverride_);
    if (haveGateway || haveOverride) {
        const auto pending = coordinator_.pendingMarkerTransactions();
        for (const auto& p : pending) {
            coordinator_.markersDispatched(p.txnId);
            dispatchMarkers_(p.txnId, p.producerId, p.producerEpoch, p.coordinatorEpoch,
                             p.commit, p.partitions);
            ++work;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// handleInitProducerId (templateId 45)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleInitProducerId(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::InitProducerId::decode(buf, len);
    if (!msgOpt) return 0;
    const auto& msg = *msgOpt;

    // Build response shell (sessionId/correlationId always echoed).
    pc::InitProducerIdResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        resp.producerId    = -1;
        resp.producerEpoch = -1;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::InitProducerIdResponse::ENCODED_SIZE) return 0;
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

    // Call coordinator.
    const auto result = coordinator_.initProducerId(
        txnIdOpt, msg.transactionTimeoutMs, nowNs);

    // Persist metadata BEFORE responding (persist-before-respond).
    if (result.ok() && txnIdOpt.has_value()) {
        const auto* meta = coordinator_.metadata(*txnIdOpt);
        if (meta && txnState_) {
            // Ignore persist failure here: the coordinator already updated
            // in-memory state; a log failure is a degraded-durability condition
            // not a protocol error (minIsr-gate on startup would have prevented this).
            txnState_->put(*meta, meta->coordinatorEpoch);
        }
    }

    resp.producerId    = result.producerId;
    resp.producerEpoch = result.producerEpoch;
    resp.errorCode     = txnErrorToProtocol(result.errorCode);
    if (outCap < pc::InitProducerIdResponse::ENCODED_SIZE) return 0;
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
    if (!msgOpt) return 0;
    const auto& msg = *msgOpt;

    // NotCoordinator fence — build minimal response.
    if (!isLocalCoordinator()) {
        // Encode all partitions with NotCoordinator using partResultScratch_ directly
        // (no per-call vector alloc — member scratch cleared+refilled).
        const size_t partCount = msg.partitions.size();
        const size_t needed = pc::AddPartitionsToTxnResponse::encodedSizeFor(partCount);
        if (outCap < needed) return 0;

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
            txnState_->put(*meta, meta->coordinatorEpoch);
        }
    }

    // Encode response: one result entry per partition with the same error code.
    // Reuse partResultScratch_ (pre-reserved member) — no per-call alloc.
    const int16_t errCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    const size_t needed = pc::AddPartitionsToTxnResponse::encodedSizeFor(partCount);
    if (outCap < needed) return 0;

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
    if (!msgOpt) return 0;
    const auto& msg = *msgOpt;

    pc::AddOffsetsToTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    if (!isLocalCoordinator()) {
        resp.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::AddOffsetsToTxnResponse::ENCODED_SIZE) return 0;
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
    offsetsScratch_.clear();
    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        groupIdScratch_, offsetsScratch_, nowNs);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, meta->coordinatorEpoch);
        }
    }

    resp.errorCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    if (outCap < pc::AddOffsetsToTxnResponse::ENCODED_SIZE) return 0;
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
    if (!msgOpt) return 0;
    const auto& msg = *msgOpt;

    // NotCoordinator fence — encode all entries with NotCoordinator using
    // partResultScratch_ directly (no per-call vector alloc).
    if (!isLocalCoordinator()) {
        const size_t entryCount = msg.entries.size();
        const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
        if (outCap < needed) return 0;

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
            txnState_->put(*meta, meta->coordinatorEpoch);
        }
    }

    // Build result entries — all same error code.
    // Reuse partResultScratch_ (pre-reserved member) — no per-call vector alloc.
    const int16_t errCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    const size_t entryCount = msg.entries.size();
    const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
    if (outCap < needed) return 0;

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
    if (!msgOpt) return 0;
    const auto& msg = *msgOpt;

    pc::EndTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    if (!isLocalCoordinator()) {
        resp.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        if (outCap < pc::EndTxnResponse::ENCODED_SIZE) return 0;
        return resp.encode(out);
    }

    // Decode txnId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());

    const bool commit = (msg.commit != 0);
    const auto result = coordinator_.endTransaction(
        txnIdScratch_, msg.producerId, msg.producerEpoch, commit, nowNs);

    // Persist before responding (endTxn sets PREPARE state — durable).
    // Pass the real coordinatorEpoch from metadata (not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, meta->coordinatorEpoch);
        }
    }

    // TX-4 Task 8 / Task 10 round 5: write the control markers for a client-
    // driven commit/abort.  The txn stays Prepare* and the EndTxn RESPONSE IS
    // DEFERRED until every marker has landed — the producer cannot start its
    // next transaction (whose batches could otherwise land ahead of a marker, or
    // be closed by it in the LSO bookkeeping) before then.
    if (result.ok() && gateway_ && topics_) {
        const auto* m = coordinator_.metadata(txnIdScratch_);
        if (m) {
            const std::vector<tx::TopicPartition> parts(m->partitions.begin(), m->partitions.end());
            const bool isCommit = (m->state == tx::TxnState::PrepareCommit);
            const tx::TransactionalId txnId = txnIdScratch_;
            if (dispatchMarkers_(txnId, m->producerId, m->producerEpoch, m->coordinatorEpoch,
                                 isCommit, parts, msg.sessionId, msg.correlationId)) {
                return 0;   // response sent by completeAwaiting_ when the last marker lands
            }
        }
    }

    resp.errorCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    if (outCap < pc::EndTxnResponse::ENCODED_SIZE) return 0;
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
                producerId, commit);
            if (off == rune::gateway::ClientGateway::kControlRecordAsync) {
                ++a.outstanding;                       // lands later: onControlMarkerLanded
            } else if (off >= 0) {
                gateway_->notifyControlMarker(*topicIdOpt, tp.partition, producerId, off, commit);
            } else {
                spdlog::warn("TCS: appendControlRecord failed topicId={} partition={}", *topicIdOpt, tp.partition);
            }
        } else if (appendFnOverride_) {
            (void)appendFnOverride_(tp.topic, tp.partition,
                                    std::span<const uint8_t>(batch.data(), batch.size()));
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

void TransactionCoordinationService::onControlMarkerLanded(int32_t, int32_t, int64_t producerId)
{
    resolveMarker_(producerId);
}

void TransactionCoordinationService::onControlMarkerRefused(int32_t topicId, int32_t partition,
                                                            int64_t producerId, int16_t errorCode)
{
    ++controlMarkersRefused_;
    spdlog::error("TCS: control marker REFUSED/FAILED topic={} partition={} producerId={} err={} "
                  "(counted as resolved so the transaction can complete)",
                  topicId, partition, producerId, errorCode);
    resolveMarker_(producerId);
}

} // namespace rune::node
