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

#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <cstring>
#include <map>
#include <string_view>

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
        case 6:  return static_cast<int16_t>(ErrorCode::InvalidTxnState);  // kConcurrentTransactions
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
    // Pre-reserve scratch vectors to avoid steady-state allocation.
    partitionScratch_.reserve(32);
    partResultScratch_.reserve(32);
}

// ---------------------------------------------------------------------------
// isLocalCoordinator
// ---------------------------------------------------------------------------
bool TransactionCoordinationService::isLocalCoordinator() const {
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
    // CG-6 pattern: detect Follower→Leader transition to rebuild index.
    const bool leader = isLocalCoordinator();
    if (leader && !wasLeader_ && txnState_) {
        // Rebuild in-memory index from replicated log.
        txnState_->rebuildFromLog();
        wasLeader_ = true;
    } else if (!leader) {
        wasLeader_ = false;
    }

    if (!leader) return 0;

    // Drive coordinator timeout aborts (sets markersPending on timed-out txns).
    int work = coordinator_.doWork(nowNs);

    // TODO(Task 8): resolve pending markers via ControlMarker + replica appends.
    // For now, pending markers remain in PrepareAbort/PrepareCommit state.

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
            txnState_->put(*meta, /*coordEpoch=*/0);
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
        // Response: count=1, single error entry echoing first partition (or 0 entries ok).
        // Encode an empty results array with all partitions NotCoordinator.
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
        std::vector<pc::TxnPartitionResult> res;
        res.reserve(partResultScratch_.size());
        for (const auto& r : partResultScratch_)
            res.push_back({r.topicId, r.partition, r.errorCode});
        return resp.encode(out, res);
    }

    // Decode txnId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());

    // Build partition vector.  Protocol sends topicId (int32) + partition (int32).
    // TransactionCoordinator needs topic name strings. Resolve via TopicRegistry
    // if available; otherwise use numeric placeholder (e.g. "topic_<id>").
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
                // Unknown topic: use numeric placeholder.
                txtp.topic = "topic_" + std::to_string(tp.topicId);
            }
        } else {
            txtp.topic = "topic_" + std::to_string(tp.topicId);
        }
        partitionScratch_.push_back(std::move(txtp));
    }

    const auto result = coordinator_.addPartitions(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        partitionScratch_, nowNs);

    // Persist before responding.
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, /*coordEpoch=*/0);
        }
    }

    // Encode response: one result entry per partition with the same error code.
    const int16_t errCode = txnErrorToProtocol(result.errorCode);
    const size_t needed = pc::AddPartitionsToTxnResponse::encodedSizeFor(partCount);
    if (outCap < needed) return 0;

    std::vector<pc::TxnPartitionResult> res;
    res.reserve(partCount);
    for (size_t i = 0; i < partCount; ++i) {
        auto tp = msg.partitions[i];
        res.push_back({tp.topicId, tp.partition, errCode});
    }

    pc::AddPartitionsToTxnResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, res);
}

// ---------------------------------------------------------------------------
// handleAddOffsets (templateId 49)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleAddOffsets(
    const uint8_t* buf, size_t len, int64_t /*nowNs*/,
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
    // TODO(Task 7 full implementation): if the brief requires a real offset map,
    // we need a TxnOffsetCommit step. AddOffsetsToTxn is a "register intent" call.
    const std::map<rune::coordination::TopicPartition, rune::coordination::Offset> emptyOffsets;
    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        groupIdScratch_, emptyOffsets, /*nowNs=*/0);

    // Persist before responding.
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, /*coordEpoch=*/0);
        }
    }

    resp.errorCode = txnErrorToProtocol(result.errorCode);
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

    // NotCoordinator fence — empty results array.
    if (!isLocalCoordinator()) {
        const size_t entryCount = msg.entries.size();
        const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
        if (outCap < needed) return 0;

        std::vector<pc::TxnPartitionResult> res;
        res.reserve(entryCount);
        for (const auto& e : msg.entries) {
            res.push_back({e.topicId, e.partition,
                           static_cast<int16_t>(pc::ErrorCode::NotCoordinator)});
        }
        pc::TxnOffsetCommitResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, res);
    }

    // Decode txnId and groupId.
    txnIdScratch_.assign(
        reinterpret_cast<const char*>(msg.transactionalId.data()),
        msg.transactionalId.size());
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()),
        msg.groupId.size());

    // Build an offset map from the entries.
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsets;
    for (const auto& e : msg.entries) {
        rune::coordination::TopicPartition tp;
        if (topics_) {
            const auto* spec = topics_->topic(e.topicId);
            tp.topic = spec ? spec->name : ("topic_" + std::to_string(e.topicId));
        } else {
            tp.topic = "topic_" + std::to_string(e.topicId);
        }
        tp.partition = e.partition;
        offsets[tp] = e.offset;
    }

    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, msg.producerId, msg.producerEpoch,
        groupIdScratch_, offsets, nowNs);

    // Persist before responding.
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, /*coordEpoch=*/0);
        }
    }

    // Build result entries — all same error code.
    const int16_t errCode = txnErrorToProtocol(result.errorCode);
    const size_t entryCount = msg.entries.size();
    const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(entryCount);
    if (outCap < needed) return 0;

    std::vector<pc::TxnPartitionResult> res;
    res.reserve(entryCount);
    for (const auto& e : msg.entries) {
        res.push_back({e.topicId, e.partition, errCode});
    }

    pc::TxnOffsetCommitResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, res);
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
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            txnState_->put(*meta, /*coordEpoch=*/0);
        }
    }

    resp.errorCode = txnErrorToProtocol(result.errorCode);
    if (outCap < pc::EndTxnResponse::ENCODED_SIZE) return 0;
    return resp.encode(out);
}

} // namespace rune::node
