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
#include <rune/protocol/ProducerIdBinding.h>
#include <rune/replication/ReplicationMessages.h>   // MetadataFeature (#122)
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <cstdlib>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/Types.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <spdlog/spdlog.h>

#include <algorithm>
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
    // A committed transaction's consumer-group offsets go to the group coordinator's offset store (setTxnOffsetSink).
    coordinator_.setCheckedOffsetApplyCallback(
        [this](const std::string& group, const rune::coordination::TopicPartition& tp, rune::coordination::Offset offset,
               int64_t /*timestampNs*/) { return applyTxnOffset_(group, tp, offset); });
    coordinator_.setBoundProducerIds(true);   // issue #122: every id this service issues carries its owner
    // Pre-reserve scratch containers to avoid steady-state allocation.
    partitionScratch_.reserve(32);
    partResultScratch_.reserve(32);
    topicNameScratch_.reserve(32);  // "topic_<N>" stays well within 32 chars
    pendingByPidScratch_.reserve(16);
    toNotifyScratch_.reserve(32);
    dueScratch_.reserve(16);
}

// ---------------------------------------------------------------------------
// applyTxnOffset_ - one pending (group, tp, offset) of a transaction that is being COMMITTED.
// ---------------------------------------------------------------------------
bool TransactionCoordinationService::applyTxnOffset_(const std::string& group,
                                                     const rune::coordination::TopicPartition& tp, int64_t offset)
{
    if (offsetSink_ == nullptr) return true;   // no offset store on this node: nothing to apply to
    // Duty clock, as handleOffsetCommit's nowNs.  #358: only a COMMITTED offset counts as applied.
    switch (offsetSink_(offsetSinkCtx_, group, tp, offset, lastNowNs_, completionToken_)) {
        case rune::coordination::TxnOffsetApply::Applied:     return true;
        case rune::coordination::TxnOffsetApply::Replicating: return false;
        case rune::coordination::TxnOffsetApply::Refused:     break;
    }
    offsetRefusedThisPass_ = true;
    refusalThisPass_ = offsetRefusalFn_ != nullptr ? offsetRefusalFn_(offsetSinkCtx_)
                                                   : rune::coordination::TxnOffsetRefusal::Unknown;   // #359
    return false;
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
    for (const auto& [pid, a] : awaiting_) releaseOffsets_(a.offsetToken);
    awaiting_.clear();
    dropDeferred_();       // nothing deferred survives a load (unload answered it)
    lastPutOffset_.clear();   // the loaded log is committed
    rebuildHolds_();          // #438: from the loaded (committed) state
    stuckOffsetTxns_ = 0;     // #359: awaiting_ is empty
    offsetsPendingTxns_ = 0;
    queuedRetries_ = 0;
    stuckMarkers_  = 0;
    nextStuckScanNs_ = 0;
    publishStuckMarkers_();
    if (gateway_) (void)gateway_->purgeQueuedMarkers(coordinatorEpoch_);   // older epochs' leftovers
    if (txnState_ != nullptr) {
        // I7: the next pid lies at or above every ceiling a previous coordinator
        // persisted (idempotent pids are recorded nowhere else) and above every
        // pid in the log.  Nothing is handed out until a fresh ceiling commits.
        // #122: a ceiling is in counter units; one persisted by a coordinator that reseeded by a bound id's RAW value
        // (an older binary over bound ids) is converted like an id, so it never runs the counter past its 38 bits.
        tx::ProducerId base = rune::protocol::producerIdCounter(txnState_->persistedPidCeiling());
        // #122: the ceiling is in allocator (counter) units; a recovered id is converted (bound ids carry an owner).
        const tx::ProducerId maxCounter = rune::protocol::producerIdCounter(maxPid);
        if (maxCounter + 1 > base) base = maxCounter + 1;
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
    dropDeferred_();   // #358 class: so is every answer still waiting for its record, and every gated EndTxn
    for (const auto& [pid, a] : awaiting_) releaseOffsets_(a.offsetToken);   // their offset waiters go
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
    clearHolds_();            // #438: unknown until the next load (the probe answers Unknown)
    stuckOffsetTxns_ = 0;     // #359
    offsetsPendingTxns_ = 0;
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
    int work = releaseParked_();   // #358 class: answers whose record committed
    if (completingHolds_ != 0) work += releaseCommittedHolds_();   // #438
    // Drive coordinator timeout aborts (sets markersPending on timed-out txns).
    work += coordinator_.doWork(nowNs);
    work += retryRefusedMarkers_(nowNs);
    work += retryDeferredCompletions_(nowNs);
    // Observability only, rate-limited (review LOW-1): never a per-pass walk.
    if ((!awaiting_.empty() && nowNs >= nextStuckScanNs_) || (awaiting_.empty() && stuckMarkers_ != 0)) [[unlikely]] {
        nextStuckScanNs_ = nowNs + kStuckScanIntervalNs;
        updateStuckMarkers_(nowNs);
    }
    if (stuckOffsetTxns_ != 0 || stuckOffsetWarned_ || stuckOffsetsPublished_.first > 0) [[unlikely]]
        updateStuckOffsets_(nowNs);   // #359

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
            // #358 class: a timeout abort / fencing abort is persisted and committed before its markers (a loaded
            // Prepare* is committed already).
            const auto* meta = coordinator_.metadata(p.txnId);
            if (meta == nullptr) continue;
            prepareGates_.push_back(makeGate_(*meta, p.commit, p.partitions, /*respond=*/false, 0, 0));
            ++work;
        }
    }
    work += processPrepareGates_();

    return work;
}

// ---------------------------------------------------------------------------
// No-hang fix round 2 (I3): a handler returns 0 ONLY for a deferred answer
// (EndTxn waiting for its markers — ITxnRequestHandler contract).  A response
// that does not fit the caller's buffer is answered MessageTooLarge (its own
// response type, encodeRejectionFor) — never a 0 the gateway would read as
// "deferred" and never answer.
// ---------------------------------------------------------------------------
// Fix round 3 (I3): never 0.  outCap >= kTxnMinOutCap is the caller contract
// (it holds every rejection, kMaxRejectionBytes); a breach halts loudly — a 0
// here would be read as EndTxn's "deferred" and the request never answered.
static_assert(rune::gateway::kTxnMinOutCap >= rune::gateway::kMaxRejectionBytes);
static_assert(rune::gateway::kTxnMinOutCap >= rune::protocol::client::EndTxnResponse::ENCODED_SIZE);
static_assert(rune::gateway::kTxnMinOutCap >= rune::protocol::client::InitProducerIdResponse::ENCODED_SIZE);
static_assert(rune::gateway::kTxnMinOutCap >= rune::protocol::client::AddOffsetsToTxnResponse::ENCODED_SIZE);
[[noreturn]] [[gnu::cold]] static void haltTxnOutCapBreach_(size_t outCap) noexcept {
    spdlog::critical("TransactionCoordinationService: answer buffer of {} B < kTxnMinOutCap ({} B) — "
                     "caller contract breach; halting rather than leaving a request unanswered",
                     outCap, rune::gateway::kTxnMinOutCap);
    std::fprintf(stderr, "TransactionCoordinationService: answer buffer of %zu B < kTxnMinOutCap — halting\n",
                 outCap);
    std::abort();
}
static size_t answerCannotFit_(const uint8_t* buf, size_t len, uint8_t* out, size_t outCap) noexcept {
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);
    const size_t n = rune::gateway::encodeRejectionFor(
        buf, len, static_cast<uint16_t>(rune::protocol::client::ErrorCode::MessageTooLarge), out, outCap);
    if (n == 0) [[unlikely]] haltTxnOutCapBreach_(outCap);   // header undecodable: unreachable (gateway checked it)
    return n;
}

// ---------------------------------------------------------------------------
// Issue #122: producer ids are bound to the principal they were issued to.
// ---------------------------------------------------------------------------
bool TransactionCoordinationService::bindsProducerIds() const noexcept
{
    return featureLevel_ < 0 || featureLevel_ >= rune::replication::MetadataFeature::kBoundProducerIds;
}

uint64_t TransactionCoordinationService::principalOf_(int64_t sessionId) const
{
    if (principalFn_ != nullptr) return principalFn_(principalCtx_, sessionId);
    return gateway_ != nullptr ? gateway_->sessionPrincipalId(sessionId) : 0;
}

int64_t TransactionCoordinationService::requestPid_(int64_t pid, int64_t sessionId) const
{
    // Another principal's id acts as an id that matches no transaction: the coordinator answers it exactly like a
    // wrong producer id (nothing to tell "someone else's" from "nobody's").  Never a valid id (ids are >= 0).
    constexpr int64_t kNoSuchProducerId = INT64_MIN;
    return rune::protocol::producerIdUsableBy(pid, principalOf_(sessionId)) ? pid : kNoSuchProducerId;
}


// ---------------------------------------------------------------------------
// #358 class: answers and markers wait for the __transaction_state records they depend on to COMMIT.
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleInitProducerId(const uint8_t* buf, size_t len, int64_t nowNs,
                                                            uint8_t* out, size_t outCap)
{
    beginRequest_();
    return finishRequest_(buf, len, out, outCap, handleInitProducerId_(buf, len, nowNs, out, outCap));
}
size_t TransactionCoordinationService::handleAddPartitions(const uint8_t* buf, size_t len, int64_t nowNs,
                                                           uint8_t* out, size_t outCap)
{
    beginRequest_();
    return finishRequest_(buf, len, out, outCap, handleAddPartitions_(buf, len, nowNs, out, outCap));
}
size_t TransactionCoordinationService::handleAddOffsets(const uint8_t* buf, size_t len, int64_t nowNs,
                                                        uint8_t* out, size_t outCap)
{
    beginRequest_();
    return finishRequest_(buf, len, out, outCap, handleAddOffsets_(buf, len, nowNs, out, outCap));
}
size_t TransactionCoordinationService::handleTxnOffsetCommit(const uint8_t* buf, size_t len, int64_t nowNs,
                                                             uint8_t* out, size_t outCap)
{
    beginRequest_();
    return finishRequest_(buf, len, out, outCap, handleTxnOffsetCommit_(buf, len, nowNs, out, outCap));
}
size_t TransactionCoordinationService::handleEndTxn(const uint8_t* buf, size_t len, int64_t nowNs,
                                                    uint8_t* out, size_t outCap)
{
    beginRequest_();
    return finishRequest_(buf, len, out, outCap, handleEndTxn_(buf, len, nowNs, out, outCap));
}

bool TransactionCoordinationService::persist_(const tx::TransactionMetadata& meta)
{
    if (txnState_ == nullptr) return true;
    const int64_t at = txnState_->putAt(meta, coordinatorEpoch_);
    if (at < 0) {
        requestPutRefused_ = true;
        return false;
    }
    if (at > requestPutOffset_) requestPutOffset_ = at;
    lastPutOffset_[meta.txnId] = at;
    return true;
}

bool TransactionCoordinationService::committed_(int64_t recordOffset) const
{
    return txnState_ == nullptr || txnState_->replica()->highWatermark() > recordOffset;
}

void TransactionCoordinationService::sendDeferred_(int64_t sessionId, const uint8_t* frame, std::size_t n)
{
    if (gateway_ != nullptr) (void)gateway_->sendControlFrame(sessionId, frame, n);
    else if (answerSink_ != nullptr) answerSink_(answerSinkCtx_, sessionId, frame, n);
}

void TransactionCoordinationService::sendEndTxnAnswer_(int64_t sessionId, int64_t correlationId, int16_t errorCode)
{
    pc::EndTxnResponse resp;
    resp.sessionId     = sessionId;
    resp.correlationId = correlationId;
    resp.errorCode     = errorCode;
    std::array<uint8_t, pc::EndTxnResponse::ENCODED_SIZE> buf{};
    const std::size_t n = resp.encode(buf.data());
    sendDeferred_(sessionId, buf.data(), n);
}

size_t TransactionCoordinationService::finishRequest_(const uint8_t* buf, size_t len, uint8_t* out, size_t outCap,
                                                      size_t n)
{
    if (n == 0) return 0;   // already deferred (EndTxn)
    if (requestPutRefused_) [[unlikely]] {
        // Not durable: never an OK.  NotCoordinator is retriable - the client re-discovers and retries, and the
        // retry appends the (idempotent) state again.
        spdlog::warn("TCS: a __transaction_state append was refused (min-ISR) - the request is answered "
                     "NotCoordinator and retried by the client");
        const size_t r = rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::NotCoordinator), out, outCap);
        return r != 0 ? r : n;
    }
    if (requestPutOffset_ < 0 || committed_(requestPutOffset_)) return n;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    ParkedAnswer pa;
    pa.recordOffset = requestPutOffset_;
    pa.sessionId    = hdr ? hdr->sessionId : 0;
    const auto take = [&](std::vector<uint8_t>& v, const uint8_t* p, std::size_t k) {
        if (!spareBufs_.empty()) { v = std::move(spareBufs_.back()); spareBufs_.pop_back(); }
        v.assign(p, p + k);
    };
    take(pa.request, buf, len);
    take(pa.answer, out, n);
    parked_.push_back(std::move(pa));
    return 0;   // sent by releaseParked_ once the record is committed
}

int TransactionCoordinationService::releaseParked_()
{
    int n = 0;
    while (!parked_.empty() && committed_(parked_.front().recordOffset)) {
        ParkedAnswer& pa = parked_.front();
        sendDeferred_(pa.sessionId, pa.answer.data(), pa.answer.size());
        spareBufs_.push_back(std::move(pa.request));
        spareBufs_.push_back(std::move(pa.answer));
        parked_.pop_front();
        ++n;
    }
    return n;
}

TransactionCoordinationService::PrepareGate TransactionCoordinationService::makeGate_(
    const tx::TransactionMetadata& meta, bool commit, std::vector<tx::TopicPartition> parts, bool respond,
    int64_t sessionId, int64_t correlationId)
{
    PrepareGate g;
    g.txnId            = meta.txnId;
    g.producerId       = meta.producerId;
    g.producerEpoch    = meta.producerEpoch;
    g.coordinatorEpoch = coordinatorEpoch_;
    g.commit           = commit;
    g.parts            = std::move(parts);
    g.respond          = respond;
    g.sessionId        = sessionId;
    g.correlationId    = correlationId;
    if (txnState_ == nullptr) {
        g.durable = true;
        return g;
    }
    // The record of THIS Prepare* is the store's last append for the id when it matches the state; a loaded one
    // (no append since the load) is committed.  Otherwise (a timeout abort is never appended by the coordinator
    // itself) it is appended by gateOpen_.
    const auto* stored = txnState_->get(meta.txnId);
    const bool same = stored != nullptr && stored->state == meta.state && stored->producerId == meta.producerId &&
                      stored->producerEpoch == meta.producerEpoch;
    if (same) {
        const auto it = lastPutOffset_.find(meta.txnId);
        if (it == lastPutOffset_.end()) g.durable = true;
        else g.recordOffset = it->second;
    }
    return g;
}

bool TransactionCoordinationService::gateOpen_(PrepareGate& g)
{
    if (g.durable) return true;
    if (g.recordOffset == kNeedsPut) {
        const auto* meta = coordinator_.metadata(g.txnId);
        if (meta == nullptr || !persist_(*meta)) return false;   // refused: retried on the next doWork
        g.recordOffset = lastPutOffset_[g.txnId];
    }
    return committed_(g.recordOffset);
}

int TransactionCoordinationService::processPrepareGates_()
{
    int work = 0;
    for (std::size_t i = 0; i < prepareGates_.size();) {
        PrepareGate& g = prepareGates_[i];
        const auto* meta = coordinator_.metadata(g.txnId);
        const bool stale = meta == nullptr || meta->producerId != g.producerId ||
                           meta->producerEpoch != g.producerEpoch ||
                           (meta->state != tx::TxnState::PrepareCommit && meta->state != tx::TxnState::PrepareAbort);
        if (!stale && !gateOpen_(g)) { ++i; continue; }
        PrepareGate open = std::move(g);
        prepareGates_.erase(prepareGates_.begin() + static_cast<std::ptrdiff_t>(i));
        ++work;
        if (stale) {   // the transaction moved on meanwhile: nothing to write; a waiting EndTxn retries
            if (open.respond) sendEndTxnAnswer_(open.sessionId, open.correlationId,
                                                static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
            continue;
        }
        const bool deferred = dispatchMarkers_(open.txnId, open.producerId, open.producerEpoch, open.coordinatorEpoch,
                                               open.commit, open.parts, open.respond ? open.sessionId : 0,
                                               open.respond ? open.correlationId : 0);
        if (!deferred && open.respond) sendEndTxnAnswer_(open.sessionId, open.correlationId, 0);   // completed now
    }
    return work;
}

void TransactionCoordinationService::dropDeferred_()
{
    for (ParkedAnswer& pa : parked_) {
        std::array<uint8_t, rune::gateway::kMaxRejectionBytes> rej{};
        const std::size_t n = rune::gateway::encodeRejectionFor(
            pa.request.data(), pa.request.size(), static_cast<uint16_t>(pc::ErrorCode::NotCoordinator),
            rej.data(), rej.size());
        if (n != 0) sendDeferred_(pa.sessionId, rej.data(), n);
    }
    parked_.clear();
    for (const PrepareGate& g : prepareGates_)
        if (g.respond) sendEndTxnAnswer_(g.sessionId, g.correlationId, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    prepareGates_.clear();
}

// ---------------------------------------------------------------------------
// handleInitProducerId (templateId 45)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleInitProducerId_(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);   // fix round 3 (I3)
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

    // Call coordinator.  Issue #122: the id is issued to (bound to) the session's principal - once every member
    // supports bound ids (below the feature level: a raw id, nothing rebound).
    const auto result = coordinator_.initProducerId(
        txnIdOpt, msg.transactionTimeoutMs, nowNs,
        bindsProducerIds() ? std::optional<uint64_t>(principalOf_(msg.sessionId)) : std::nullopt);

    // F15 I7: the allocation may have crossed half a block — prefetch the next.
    advancePidBlock_();

    // Persist metadata BEFORE responding (persist-before-respond).
    if (result.ok() && txnIdOpt.has_value()) {
        const auto* meta = coordinator_.metadata(*txnIdOpt);
        if (meta && txnState_) {
            // Ignore persist failure here: the coordinator already updated
            // in-memory state; a log failure is a degraded-durability condition
            // not a protocol error (minIsr-gate on startup would have prevented this).
            (void)persist_(*meta);   // #358 class: the answer waits for the record (finishRequest_)
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
size_t TransactionCoordinationService::handleAddPartitions_(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);   // fix round 3 (I3)
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
        txnIdScratch_, requestPid_(msg.producerId, msg.sessionId), msg.producerEpoch,
        partitionScratch_, nowNs);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            (void)persist_(*meta);   // #358 class: the answer waits for the record (finishRequest_)
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
size_t TransactionCoordinationService::handleAddOffsets_(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);   // fix round 3 (I3)
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
        txnIdScratch_, requestPid_(msg.producerId, msg.sessionId), msg.producerEpoch,
        groupIdScratch_, offsetsScratch_, nowNs, /*beginsTxn=*/true);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            (void)persist_(*meta);   // #358 class: the answer waits for the record (finishRequest_)
        }
    }

    resp.errorCode = txnErrorToProtocol(concurrentIfPreparing_(result.errorCode));
    if (outCap < pc::AddOffsetsToTxnResponse::ENCODED_SIZE) return answerCannotFit_(buf, len, out, outCap);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleTxnOffsetCommit (templateId 51)
// ---------------------------------------------------------------------------
size_t TransactionCoordinationService::handleTxnOffsetCommit_(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);   // fix round 3 (I3)
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

    // #125 (KIP-447): a member block is fenced exactly like an OffsetCommit; a refusal adds nothing.  A legacy frame
    // (no block) has no member to check (Kafka compatibility).
    const auto answerAll = [&](int16_t code) -> size_t {
        const size_t needed = pc::TxnOffsetCommitResponse::encodedSizeFor(msg.entries.size());
        if (outCap < needed) return answerCannotFit_(buf, len, out, outCap);
        partResultScratch_.clear();
        for (const auto& e : msg.entries) partResultScratch_.push_back({e.topicId, e.partition, code});
        pc::TxnOffsetCommitResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::TxnPartitionResult>(partResultScratch_));
    };
    if (msg.hasMemberBlock && memberFence_ != nullptr) {
        memberIdScratch_.assign(reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
        const pc::ErrorCode fenced =
            memberFence_(memberFenceCtx_, groupIdScratch_, memberIdScratch_, msg.generationId, msg.sessionId);
        if (fenced != pc::ErrorCode::OK) return answerAll(static_cast<int16_t>(fenced));
    }

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

    // #438: a key another unfinished transaction holds is not touched - retriable, nothing added (all or nothing:
    // the client re-sends the whole request).
    if (heldByOther_(groupIdScratch_, txnIdScratch_))
        return answerAll(static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));

    const auto result = coordinator_.addOffsetsToTxn(
        txnIdScratch_, requestPid_(msg.producerId, msg.sessionId), msg.producerEpoch,
        groupIdScratch_, offsetsScratch_, nowNs);

    // Persist before responding (pass the real coordinatorEpoch, not literal 0).
    if (result.ok()) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            (void)persist_(*meta);   // #358 class: the answer waits for the record (finishRequest_)
        }
        for (const auto& [tp, off] : offsetsScratch_) addHold_(groupIdScratch_, tp, txnIdScratch_);   // #438
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
size_t TransactionCoordinationService::handleEndTxn_(
    const uint8_t* buf, size_t len, int64_t nowNs,
    uint8_t* out, size_t outCap)
{
    if (outCap < rune::gateway::kTxnMinOutCap) [[unlikely]] haltTxnOutCapBreach_(outCap);   // fix round 3 (I3)
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
        txnIdScratch_, requestPid_(msg.producerId, msg.sessionId), msg.producerEpoch, commit, nowNs);

    // F15 §5.4: an OK for a same-direction retry of a COMPLETED transaction is
    // the idempotent answer — no transition, nothing persisted, no markers.
    const bool transitioned = result.ok() && !result.alreadyComplete;

    // Persist before responding (endTxn sets PREPARE state — durable).
    if (transitioned) {
        const auto* meta = coordinator_.metadata(txnIdScratch_);
        if (meta && txnState_) {
            (void)persist_(*meta);   // #358 class: the answer waits for the record (finishRequest_)
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
            const bool isCommit = (m->state == tx::TxnState::PrepareCommit);
            PrepareGate g = makeGate_(*m, isCommit, std::vector<tx::TopicPartition>(m->partitions.begin(),
                                                                                   m->partitions.end()),
                                      /*respond=*/true, msg.sessionId, msg.correlationId);
            // #358 class: the markers are written only once the Prepare* record is committed; until then the
            // EndTxn answer waits with them (doWork opens the gate).
            if (!gateOpen_(g)) {
                prepareGates_.push_back(std::move(g));
                return 0;
            }
            if (dispatchMarkers_(g.txnId, g.producerId, g.producerEpoch, g.coordinatorEpoch,
                                 g.commit, g.parts, msg.sessionId, msg.correlationId)) {
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
    if (respSessionId != 0 || respCorrelationId != 0) {
        a.respPending   = true;
        a.sessionId     = respSessionId;
        a.correlationId = respCorrelationId;
    }
    if (a.outstanding == 0 && completeAwaiting_(producerId, /*respond=*/false)) {
        return false;                                  // completed synchronously
    }
    return true;   // markers outstanding, or the completion deferred on a refused offset apply: answered later
}

void TransactionCoordinationService::resolveMarker_(int64_t producerId)
{
    auto it = awaiting_.find(producerId);
    if (it == awaiting_.end() || it->second.outstanding <= 0) return;
    if (--it->second.outstanding == 0) completeAwaiting_(producerId, /*respond=*/true);
}

bool TransactionCoordinationService::completeAwaiting_(int64_t producerId, bool respond)
{
    auto it = awaiting_.find(producerId);
    if (it == awaiting_.end()) return true;
    AwaitingMarkers& cur = it->second;
    offsetRefusedThisPass_ = false;
    refusalThisPass_ = rune::coordination::TxnOffsetRefusal::Unknown;
    completionToken_ = {};
    if (const auto* m = coordinator_.metadata(cur.txnId)) {
        // F2: one token per transaction (a producer's next transaction starts after this one completed) and per
        // load (another load re-drives with its own record), compared field by field.
        completionToken_ = rune::coordination::TxnOffsetToken{m->producerId, m->producerEpoch, m->txnStartNs,
                                                               coordinatorEpoch_};
    }
    cur.offsetToken = completionToken_;
    const auto r = coordinator_.completeTransaction(cur.txnId);
    if (r.errorCode == tx::CompleteTransactionResult::kOffsetsPending && !offsetRefusedThisPass_) {
        // #358: every offset is appended, some not committed yet on the offsets replica.  CompleteCommit waits for
        // them (a failover of the offsets partition could otherwise lose an offset the transaction log says is
        // committed); re-checked on the next doWork - the wait is on replication, not on a refusal: no backoff.
        if (!cur.offsetsPending) {
            cur.offsetsPending = true;
            ++offsetsPendingTxns_;
        }
        if (cur.offsetRefusedSinceNs >= 0) {   // #359: the group coordinator took them - no longer stuck
            cur.offsetRefusedSinceNs = -1;
            --stuckOffsetTxns_;
        }
        cur.offsetBackoffNs = 0;
        cur.offsetDueNs     = lastNowNs_;
        ++offsetReplicationWaits_;
        return false;
    }
    if (r.errorCode == tx::CompleteTransactionResult::kOffsetsPending) {
        // The group coordinator refused a committed offset (not loaded after a failover / gated): the txn STAYS
        // PrepareCommit (nothing persisted, EndTxn not answered); retried from doWork with backoff.
        cur.offsetRefusal = refusalThisPass_;   // #359
        if (cur.offsetRefusedSinceNs < 0) {
            cur.offsetRefusedSinceNs = lastNowNs_;
            if (stuckOffsetTxns_++ == 0) nextStuckOffsetWarnNs_ = lastNowNs_ + kStuckTxnOffsetWarnNs;
        }
        if (!cur.offsetsPending || cur.offsetBackoffNs == 0) {
            if (!cur.offsetsPending) ++offsetsPendingTxns_;
            cur.offsetsPending  = true;
            cur.offsetBackoffNs = kOffsetRetryMinNs;
            spdlog::warn("TCS: committed offsets of txn '{}' not applied yet (the group coordinator refused: {}) - "
                         "the commit stays PrepareCommit and is retried", cur.txnId,
                         rune::coordination::toString(cur.offsetRefusal));
        } else {
            cur.offsetBackoffNs = std::min(cur.offsetBackoffNs * 2, kOffsetRetryMaxNs);
        }
        cur.offsetDueNs = lastNowNs_ + cur.offsetBackoffNs;
        ++offsetApplyRetries_;
        return false;
    }
    const AwaitingMarkers a = std::move(cur);
    awaiting_.erase(it);
    releaseOffsets_(a.offsetToken);   // nothing waits on its offsets any more
    if (a.offsetsPending && offsetsPendingTxns_ > 0) --offsetsPendingTxns_;
    if (a.offsetRefusedSinceNs >= 0) --stuckOffsetTxns_;   // #359
    ++transactionsCompleted_;
    // F15 m4: persist the resting Complete* (offsets were applied first, inside
    // completeTransaction).  It need not commit before the EndTxn OK: if it is
    // lost, the next coordinator loads Prepare*, re-drives the (idempotent)
    // markers and offsets, and completes again.
    // #438: the transaction's (group, tp) holds are released only once this record IS committed - until then a
    // reload could still find it Prepare* and re-drive it.
    if (r.ok()) {
        int64_t record = -3;   // no store: nothing to wait for
        if (txnState_ != nullptr) {
            record = kHoldNeedsPut;
            if (const auto* meta = coordinator_.metadata(a.txnId)) {
                const int64_t at = txnState_->putAt(*meta, coordinatorEpoch_);
                if (at >= 0) {
                    record = at;
                    lastPutOffset_[a.txnId] = at;
                }
            }
        }
        completeHolds_(a.txnId, record);
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
    return true;
}

int TransactionCoordinationService::retryDeferredCompletions_(int64_t nowNs)
{
    if (offsetsPendingTxns_ == 0) return 0;
    // Completing erases from awaiting_, so the due ids are collected first (a reused member: #358 re-checks a
    // replicating commit on every doWork, which must not allocate per cycle).
    dueScratch_.clear();
    for (const auto& [pid, a] : awaiting_)
        if (a.offsetsPending && a.offsetDueNs <= nowNs) dueScratch_.push_back(pid);
    for (const int64_t pid : dueScratch_) (void)completeAwaiting_(pid, /*respond=*/true);
    return static_cast<int>(dueScratch_.size());
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

namespace rune::node {
// ---------------------------------------------------------------------------
// #438: the (group, tp) hold index (see the header).
// ---------------------------------------------------------------------------
rune::coordination::TxnOffsetHold TransactionCoordinationService::txnOffsetHold(
    const std::string& group, const rune::coordination::TopicPartition& tp) const
{
    using H = rune::coordination::TxnOffsetHold;
    if (loadState_ != LoadState::Loaded || !stillLoaded_()) return H::Unknown;   // not loaded here: unknown = held
    if (tp.partition < 0) {   // any key of the group (a fetch-all)
        const auto it = holds_.lower_bound(HoldKeyView{group, std::string_view{}, INT32_MIN});
        return it != holds_.end() && it->first.group == group ? H::Held : H::Free;
    }
    return holds_.find(HoldKeyView{group, tp.topic, tp.partition}) != holds_.end() ? H::Held : H::Free;
}

bool TransactionCoordinationService::heldByOther_(const std::string& group, const std::string& txnId) const
{
    if (holds_.empty()) return false;
    for (const auto& [tp, off] : offsetsScratch_) {
        const auto it = holds_.find(HoldKeyView{group, tp.topic, tp.partition});
        if (it == holds_.end()) continue;
        // Another transaction - or this id's PREVIOUS transaction, completed but not yet durable as such.
        if (it->second.txnId != txnId || it->second.completeRecord != kHoldOpen) return true;
    }
    return false;
}

void TransactionCoordinationService::addHold_(const std::string& group, const rune::coordination::TopicPartition& tp,
                                              const std::string& txnId)
{
    if (holds_.find(HoldKeyView{group, tp.topic, tp.partition}) != holds_.end()) return;   // already (its own)
    if (spareHoldNodes_.empty()) {
        holds_.emplace(HoldKey{group, tp.topic, tp.partition}, Holder{txnId, kHoldOpen});   // warm-up only
        return;
    }
    HoldMap::node_type n = std::move(spareHoldNodes_.back());   // recycled: strings keep their capacity
    spareHoldNodes_.pop_back();
    n.key().group.assign(group);
    n.key().topic.assign(tp.topic);
    n.key().partition = tp.partition;
    n.mapped().txnId.assign(txnId);
    n.mapped().completeRecord = kHoldOpen;
    holds_.insert(std::move(n));
}

void TransactionCoordinationService::eraseHold_(HoldMap::iterator it)
{
    if (it->second.completeRecord != kHoldOpen) --completingHolds_;
    spareHoldNodes_.push_back(holds_.extract(it));
}

void TransactionCoordinationService::clearHolds_()
{
    while (!holds_.empty()) eraseHold_(holds_.begin());
    completingHolds_ = 0;
}

void TransactionCoordinationService::rebuildHolds_()
{
    clearHolds_();
    for (const auto& [id, m] : (txnState_ != nullptr ? txnState_->all() : noSnapshot_)) {
        const bool unfinished = m.state == tx::TxnState::Ongoing || m.state == tx::TxnState::PrepareCommit ||
                                m.state == tx::TxnState::PrepareAbort || m.state == tx::TxnState::PrepareEpochFence;
        if (!unfinished) continue;
        for (const auto& po : m.pendingOffsets) {
            const auto it = holds_.find(HoldKeyView{po.group, po.tp.topic, po.tp.partition});
            if (it != holds_.end() && it->second.txnId != id) {
                // Two unfinished transactions on one key (state written before #438): the first one keeps it; the
                // other's TxnOffsetCommits are refused until it is free.
                spdlog::warn("TCS: group '{}' {}-{} has pending offsets of two unfinished transactions ('{}' and "
                             "'{}') - held by the first", po.group, po.tp.topic, po.tp.partition, it->second.txnId, id);
                continue;
            }
            addHold_(po.group, po.tp, id);
        }
    }
}

void TransactionCoordinationService::completeHolds_(const std::string& txnId, int64_t record)
{
    for (auto it = holds_.begin(); it != holds_.end();) {
        Holder& h = it->second;
        if (h.txnId != txnId || h.completeRecord != kHoldOpen) { ++it; continue; }
        if (record == -3) {   // no store: nothing to wait for
            auto next = std::next(it);
            eraseHold_(it);
            it = next;
            continue;
        }
        h.completeRecord = record;
        ++completingHolds_;
        ++it;
    }
}

int TransactionCoordinationService::releaseCommittedHolds_()
{
    int n = 0;
    for (auto it = holds_.begin(); it != holds_.end();) {
        Holder& h = it->second;
        if (h.completeRecord == kHoldNeedsPut) {
            // The Complete* append was refused (min-ISR): append it again while the id still rests there; once the
            // id moved on, its newest record (appended after the completion) stands in for it.
            const auto* m = coordinator_.metadata(h.txnId);
            if (m != nullptr && (m->state == tx::TxnState::CompleteCommit || m->state == tx::TxnState::CompleteAbort)) {
                const int64_t at = txnState_->putAt(*m, coordinatorEpoch_);
                if (at >= 0) lastPutOffset_[h.txnId] = at;
            }
            if (const auto lp = lastPutOffset_.find(h.txnId); lp != lastPutOffset_.end()) {
                for (auto& [k, other] : holds_)   // every key of that completion waits on the same record
                    if (other.txnId == h.txnId && other.completeRecord == kHoldNeedsPut) other.completeRecord = lp->second;
            }
        }
        if (h.completeRecord >= 0 && committed_(h.completeRecord)) {
            auto next = std::next(it);
            eraseHold_(it);
            it = next;
            ++n;
            continue;
        }
        ++it;
    }
    return n;
}

// ---------------------------------------------------------------------------
// #359: PrepareCommit transactions stuck on a refused offset apply.
// ---------------------------------------------------------------------------
std::size_t TransactionCoordinationService::txnOffsetCommitsStuck() const noexcept { return stuckOffsetTxns_; }

int64_t TransactionCoordinationService::oldestStuckTxnOffsetAgeNs(int64_t nowNs) const noexcept
{
    if (stuckOffsetTxns_ == 0) return 0;
    int64_t oldest = INT64_MAX;
    for (const auto& [pid, a] : awaiting_)
        if (a.offsetRefusedSinceNs >= 0 && a.offsetRefusedSinceNs < oldest) oldest = a.offsetRefusedSinceNs;
    return oldest == INT64_MAX ? 0 : std::max<int64_t>(0, nowNs - oldest);
}

void TransactionCoordinationService::updateStuckOffsets_(int64_t nowNs)
{
    if (stuckOffsetTxns_ == 0) {
        if (stuckOffsetWarned_) {
            spdlog::info("TCS: no transaction commit is stuck on a refused consumer-group offset any more");
            stuckOffsetWarned_ = false;
        }
        publishStuckOffsets_(0, 0);
        return;
    }
    if (nowNs >= nextStuckOffsetWarnNs_) {   // at most once per kStuckTxnOffsetWarnNs of the injected clock
        nextStuckOffsetWarnNs_ = nowNs + kStuckTxnOffsetWarnNs;
        const AwaitingMarkers* oldest = nullptr;
        for (const auto& [pid, a] : awaiting_)
            if (a.offsetRefusedSinceNs >= 0 && (oldest == nullptr || a.offsetRefusedSinceNs < oldest->offsetRefusedSinceNs))
                oldest = &a;
        if (oldest != nullptr) {
            spdlog::warn("TCS: {} transaction commit(s) stuck on a refused consumer-group offset - the oldest, txn "
                         "'{}', for {} s (last refusal: {}); they stay PrepareCommit and are retried until the group "
                         "coordinator takes the offsets", stuckOffsetTxns_, oldest->txnId,
                         (nowNs - oldest->offsetRefusedSinceNs) / 1'000'000'000,
                         rune::coordination::toString(oldest->offsetRefusal));
            ++stuckOffsetWarns_;
            stuckOffsetWarned_ = true;
        }
    }
    if (nowNs >= nextStuckOffsetScanNs_) {   // gauge publication, rate-limited like the stuck markers
        nextStuckOffsetScanNs_ = nowNs + kStuckScanIntervalNs;
        publishStuckOffsets_(static_cast<int64_t>(stuckOffsetTxns_), oldestStuckTxnOffsetAgeNs(nowNs) / 1'000'000);
    }
}

void TransactionCoordinationService::publishStuckOffsets_(int64_t stuck, int64_t oldestAgeMs)
{
    const std::pair<int64_t, int64_t> v{stuck, oldestAgeMs};
    if (v == stuckOffsetsPublished_) return;
    if (stuckOffsetsSink_ == nullptr) { stuckOffsetsPublished_ = v; return; }
    if (stuckOffsetsSink_(stuckOffsetsSinkCtx_, stuck, oldestAgeMs)) stuckOffsetsPublished_ = v;
}
}  // namespace rune::node
