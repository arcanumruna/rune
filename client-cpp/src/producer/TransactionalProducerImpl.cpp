// src/producer/TransactionalProducerImpl.cpp
//
// TX-5 / F8c C5 / F14 — TransactionalProducerImpl: the duty-side transactional
// producer.  See the header for the threading model and the F14 retry rules.
//
// Idempotent sequence (CRITICAL 1 fix, unchanged): TxnPartitionState::
// nextBaseSequence advances monotonically per partition; every Produce frame
// carries a real correlationId and is retained (InFlightBatch) so a retriable
// ProduceResponse error re-sends the SAME bytes (same baseSequence) — the
// broker deduplicates.  OutOfOrderSequence / InvalidProducerEpoch /
// ProducerFenced fence the producer.
//
// Re-entrancy rule: a completion is delivered only AFTER its op was moved out
// of ops_ (completeAndErase_), and no loop holds a reference into ops_ /
// reqs_ across a delivery — a handler on the duty thread may issue a new txn
// op, which is applied directly (applyCommand) and appends to those vectors.

#include <rune/client/producer/TransactionalProducerImpl.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;

namespace {

inline int64_t wallMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

inline std::span<const uint8_t> bytesOf(const std::string& s) noexcept {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

constexpr int16_t kOk                     = 0;
constexpr int16_t kNotCoordinator         = static_cast<int16_t>(rpc::ErrorCode::NotCoordinator);
constexpr int16_t kInvalidProducerEpoch   = static_cast<int16_t>(rpc::ErrorCode::InvalidProducerEpoch);
constexpr int16_t kProducerFenced         = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);
constexpr int16_t kInvalidTxnState        = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr int16_t kOutOfOrderSequence     = static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence);
constexpr int16_t kConcurrentTransactions = static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions);
constexpr int16_t kNotLeader              = static_cast<int16_t>(rpc::ErrorCode::NotLeader);

} // namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

TransactionalProducerImpl::TransactionalProducerImpl(
    std::string transactionalId,
    ProducerConfig cfg,
    int64_t& nextCorrId,
    int32_t transactionTimeoutMs)
    : transactionalId_(std::move(transactionalId))
    , cfg_(cfg)
    , transactionTimeoutMs_(transactionTimeoutMs)
    , nextCorrId_(nextCorrId)
{
    batchScratch_.reserve(4096);
    ops_.reserve(8);
    reqs_.reserve(16);
}

TransactionalProducerImpl::~TransactionalProducerImpl() {
    if (detail::testOnImplFree != nullptr) detail::testOnImplFree(detail::HandleKind::TxnProducer, this);
}

// ---------------------------------------------------------------------------
// Commands (duty thread)
// ---------------------------------------------------------------------------

bool TransactionalProducerImpl::isTxnOp(detail::HubCommand::Op op) noexcept {
    using Op = detail::HubCommand::Op;
    return op == Op::TxnInit || op == Op::TxnBegin || op == Op::TxnSend ||
           op == Op::TxnSendOffsets || op == Op::TxnEnd || op == Op::TxnClose ||
           op == Op::TxnCloseNow;
}

void TransactionalProducerImpl::discardCommand(const detail::HubCommand& cmd,
                                               const detail::CompletionSink& sink) {
    using Op = detail::HubCommand::Op;
    OpKind kind = OpKind::InitTransactions;
    switch (cmd.op) {
    case Op::TxnSend:
        delete reinterpret_cast<TxnSendRecord*>(static_cast<uintptr_t>(cmd.d));
        return;
    case Op::TxnBegin:
        return;
    case Op::TxnSendOffsets:
        delete reinterpret_cast<TxnOffsetsPayload*>(static_cast<uintptr_t>(cmd.d));
        kind = OpKind::SendOffsets;
        break;
    case Op::TxnEnd:
        kind = cmd.d != 0 ? OpKind::CommitTransaction : OpKind::AbortTransaction;
        break;
    case Op::TxnInit:
        break;
    case Op::TxnClose:
        kind = OpKind::TxnClose;
        break;
    case Op::TxnCloseNow:
        return;
    default:
        return;
    }
    Completion c;
    c.token  = static_cast<Token>(cmd.a);
    c.op     = kind;
    c.status = OpStatus::Closed;
    sink.deliver(detail::decodeTarget(cmd), c);
}

void TransactionalProducerImpl::refuse_(const detail::HubCommand& cmd, OpKind kind, OpStatus st,
                                        uint16_t code, std::string_view why) {
    Completion c;
    c.token     = static_cast<Token>(cmd.a);
    c.op        = kind;
    c.status    = st;
    c.errorCode = code;
    completions_.deliver(detail::decodeTarget(cmd), c,
                         std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(why.data()), why.size()));
}

void TransactionalProducerImpl::applyCommand(const detail::HubCommand& cmd, int64_t nowNs) {
    using Op = detail::HubCommand::Op;
    if (cmd.op == Op::TxnCloseNow) {  // F8c C7: closeImmediately (idempotent)
        failAll();
        return;
    }
    if (closed_) {
        discardCommand(cmd, completions_);
        return;
    }
    lastNowNs_ = nowNs;
    if (cmd.op == Op::TxnClose) {  // F8c C7
        beginClose(detail::decodeTarget(cmd), static_cast<Token>(cmd.a), nowNs);
        return;
    }
    if (closingDuty()) {
        // Closing: no new work (Kafka: no operation after close()).
        discardCommand(cmd, completions_);
        return;
    }
    const uint16_t kStateErr = static_cast<uint16_t>(kInvalidTxnState);
    // FatalError is terminal: every op completes Fenced, nothing is sent.
    if (const auto* f = std::get_if<SFatalError>(&state_)) {
        switch (cmd.op) {
        case Op::TxnSend:
            delete reinterpret_cast<TxnSendRecord*>(static_cast<uintptr_t>(cmd.d));
            ++refusedSends_;
            return;
        case Op::TxnBegin:
            ++refusedBegins_;
            return;
        case Op::TxnSendOffsets:
            delete reinterpret_cast<TxnOffsetsPayload*>(static_cast<uintptr_t>(cmd.d));
            refuse_(cmd, OpKind::SendOffsets, OpStatus::Fenced, f->errorCode, "producer fenced (fatal)");
            return;
        case Op::TxnInit:
            refuse_(cmd, OpKind::InitTransactions, OpStatus::Fenced, f->errorCode, "producer fenced (fatal)");
            return;
        case Op::TxnEnd:
            refuse_(cmd, cmd.d != 0 ? OpKind::CommitTransaction : OpKind::AbortTransaction, OpStatus::Fenced,
                    f->errorCode, "producer fenced (fatal)");
            return;
        default:
            return;
        }
    }
    switch (cmd.op) {
    case Op::TxnBegin: {
        if (!std::holds_alternative<SReady>(state_)) {
            ++refusedBegins_;  // Kafka: begin only in Ready (the next sends / end op are refused)
            return;
        }
        state_ = SInTransaction{};  // not started until an AddPartitions / AddOffsets is OK
        publishStatus_();
        return;
    }
    case Op::TxnSend: {
        std::unique_ptr<TxnSendRecord> rec(reinterpret_cast<TxnSendRecord*>(static_cast<uintptr_t>(cmd.d)));
        if (!rec) return;
        if (!std::holds_alternative<SInTransaction>(state_)) {
            ++refusedSends_;  // outside a transaction: never produced (no frame, no txn bit)
            return;
        }
        partition_(rec->topicId, rec->partition).waiting.push_back(std::move(*rec));
        return;
    }
    case Op::TxnInit: {
        if (!std::holds_alternative<SUninitialized>(state_)) {
            refuse_(cmd, OpKind::InitTransactions, OpStatus::BrokerError, kStateErr,
                    "initTransactions: only once, from Uninitialized");
            return;
        }
        UserOp op;
        op.id         = nextOpId_++;
        op.kind       = OpKind::InitTransactions;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = opDeadline_(nowNs);
        const uint64_t id = op.id;
        const int64_t  dl = op.deadlineNs;
        ops_.push_back(std::move(op));
        const int64_t corr = addRequest_(ReqKind::InitProducerId, id, dl);
        if (UserOp* o = findOp_(id)) o->phase = UserOp::Init{corr};
        state_ = SInitializing{id};
        publishStatus_();
        return;
    }
    case Op::TxnSendOffsets: {
        std::unique_ptr<TxnOffsetsPayload> pl(
            reinterpret_cast<TxnOffsetsPayload*>(static_cast<uintptr_t>(cmd.d)));
        if (const auto* ae = std::get_if<SAbortableError>(&state_)) {
            refuse_(cmd, OpKind::SendOffsets, ae->status, ae->errorCode,
                    "sendOffsetsToTransaction: the transaction failed — abort it");
            return;
        }
        if (!std::holds_alternative<SInTransaction>(state_)) {
            refuse_(cmd, OpKind::SendOffsets, OpStatus::BrokerError, kStateErr,
                    "sendOffsetsToTransaction: no transaction in progress");
            return;
        }
        UserOp op;
        op.id         = nextOpId_++;
        op.kind       = OpKind::SendOffsets;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = opDeadline_(nowNs);
        if (pl) op.offsets = std::move(*pl);
        const uint64_t id = op.id;
        const int64_t  dl = op.deadlineNs;
        ops_.push_back(std::move(op));
        const int64_t corr = addRequest_(ReqKind::AddOffsets, id, dl);
        if (UserOp* o = findOp_(id)) o->phase = UserOp::AddOffsets{corr};
        return;
    }
    case Op::TxnEnd: {
        const bool commit = cmd.d != 0;
        const OpKind kind = commit ? OpKind::CommitTransaction : OpKind::AbortTransaction;
        if (const auto* ae = std::get_if<SAbortableError>(&state_); ae != nullptr && commit) {
            // Kafka: a failed transaction can only be aborted.
            refuse_(cmd, kind, ae->status, ae->errorCode,
                    "commitTransaction: the transaction failed — abortTransaction() it");
            return;
        }
        UserOp op;
        op.id         = nextOpId_++;
        op.commit     = commit;
        op.kind       = kind;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = opDeadline_(nowNs);
        op.phase      = UserOp::AwaitData{};  // TX-6: EndTxn only once the data settled
        const uint64_t id = op.id;
        if (commit) {
            if (const auto* it = std::get_if<SInTransaction>(&state_)) {
                SCommitting next;
                next.opId         = id;
                next.started      = it->started;
                next.maybeStarted = it->maybeStarted;
                ops_.push_back(std::move(op));
                state_ = next;
            } else if (auto* cj = std::get_if<SCommitting>(&state_); cj != nullptr && cj->opId != 0 &&
                                                                    findOp_(cj->opId) != nullptr) {
                // m13: joins the pending commit (one EndTxn, the same completion).
                findOp_(cj->opId)->joiners.emplace_back(detail::decodeTarget(cmd), static_cast<Token>(cmd.a));
                return;
            } else if (auto* c = std::get_if<SCommitting>(&state_); c != nullptr && c->opId == 0) {
                // B-2 (Kafka m1): the in-doubt commit is retried — EndTxn(commit)
                // is re-sent against the same transaction.
                ops_.push_back(std::move(op));
                c->opId = id;
            } else {
                refuse_(cmd, kind, OpStatus::BrokerError, kStateErr,
                        "commitTransaction: no transaction to commit (or a commit is in progress)");
                return;
            }
        } else {
            if (auto* a = std::get_if<SAborting>(&state_)) {
                if (a->opId != 0) {
                    // m5: joins the pending abort (one EndTxn, the same completion).
                    if (UserOp* pending = findOp_(a->opId)) {
                        pending->joiners.emplace_back(detail::decodeTarget(cmd), static_cast<Token>(cmd.a));
                        return;
                    }
                }
                ops_.push_back(std::move(op));  // B-2: the in-doubt abort is retried
                a->opId = id;
            } else if (const auto* it = std::get_if<SInTransaction>(&state_)) {
                SAborting next;
                next.opId         = id;
                next.started      = it->started;
                next.maybeStarted = it->maybeStarted;
                ops_.push_back(std::move(op));
                dropWaiting_();  // aborted records are never produced
                state_ = next;
            } else if (const auto* ae = std::get_if<SAbortableError>(&state_)) {
                SAborting next;
                next.opId         = id;
                next.started      = ae->started;
                next.bumpEpoch    = ae->bumpEpoch;
                next.maybeStarted = ae->maybeStarted;
                ops_.push_back(std::move(op));
                dropWaiting_();
                state_ = next;
            } else {
                // Includes Committing (Kafka: COMMITTING -> ABORTING is invalid).
                refuse_(cmd, kind, OpStatus::BrokerError, kStateErr,
                        "abortTransaction: no transaction to abort (or it is committing)");
                return;
            }
        }
        publishStatus_();
        return;
    }
    default:
        return;
    }
}

// ---------------------------------------------------------------------------
// Ops / requests
// ---------------------------------------------------------------------------

TransactionalProducerImpl::UserOp* TransactionalProducerImpl::findOp_(uint64_t id) noexcept {
    for (auto& op : ops_) {
        if (op.id == id) return &op;
    }
    return nullptr;
}

TransactionalProducerImpl::TxnRequest* TransactionalProducerImpl::findReq_(int64_t corrId) noexcept {
    for (auto& r : reqs_) {
        if (r.corrId == corrId) return &r;
    }
    return nullptr;
}

void TransactionalProducerImpl::eraseReq_(int64_t corrId) noexcept {
    for (std::size_t i = 0; i < reqs_.size(); ++i) {
        if (reqs_[i].corrId == corrId) {
            reqs_.erase(reqs_.begin() + static_cast<std::ptrdiff_t>(i));
            return;
        }
    }
}

void TransactionalProducerImpl::eraseReqsOf_(uint64_t opId) noexcept {
    std::erase_if(reqs_, [&](const TxnRequest& r) { return r.opId == opId; });
}

void TransactionalProducerImpl::complete_(UserOp& op, OpStatus st, uint16_t errorCode, std::string_view text) {
    if (op.internal) {
        // F8c C7: the close's own abort — its outcome is the close's, never
        // delivered on its own.  Aborts that joined it are delivered as usual.
        if (auto* cl = std::get_if<LifeClosing>(&life_); cl != nullptr && cl->abortOpId == op.id) {
            cl->abortDone   = true;
            cl->abortStatus = st;
            cl->abortCode   = errorCode;
        }
        Completion j;
        j.op        = OpKind::AbortTransaction;
        j.status    = st;
        j.errorCode = errorCode;
        const std::span<const uint8_t> payload(reinterpret_cast<const uint8_t*>(text.data()), text.size());
        for (const auto& [target, token] : op.joiners) {
            j.token = token;
            completions_.deliver(target, j, payload);
        }
        return;
    }
    Completion c;
    c.token     = op.token;
    c.op        = op.kind;
    c.status    = st;
    c.errorCode = errorCode;
    if (st == OpStatus::Ok && op.kind == OpKind::InitTransactions) {
        c.value = producerId_;
        c.aux   = producerEpoch_;
    }
    const std::span<const uint8_t> payload(reinterpret_cast<const uint8_t*>(text.data()), text.size());
    completions_.deliver(op.target, c, payload);
    for (const auto& [target, token] : op.joiners) {  // m5: aborts that joined this one
        c.token = token;
        completions_.deliver(target, c, payload);
    }
}

void TransactionalProducerImpl::completeAndErase_(uint64_t opId, OpStatus st, uint16_t errorCode,
                                                  std::string_view text) {
    for (std::size_t i = 0; i < ops_.size(); ++i) {
        if (ops_[i].id != opId) continue;
        UserOp op = std::move(ops_[i]);
        ops_.erase(ops_.begin() + static_cast<std::ptrdiff_t>(i));
        complete_(op, st, errorCode, text);  // after removal: the handler may issue a new op
        return;
    }
}

int64_t TransactionalProducerImpl::addRequest_(ReqKind kind, uint64_t opId, int64_t deadlineNs) {
    TxnRequest r;
    r.kind       = kind;
    r.corrId     = nextCorrId_++;
    r.opId       = opId;
    r.deadlineNs = deadlineNs;
    r.state      = TxnRequest::Unsent{};
    (void)encode_(r, findOp_(opId));
    const int64_t corr = r.corrId;
    reqs_.push_back(std::move(r));
    return corr;
}

bool TransactionalProducerImpl::encode_(TxnRequest& r, const UserOp* op) {
    const auto txnId = bytesOf(transactionalId_);
    switch (r.kind) {
    case ReqKind::InitProducerId: {
        rpc::InitProducerId msg;
        msg.sessionId            = 0;  // stamped at each (re)send
        msg.correlationId        = r.corrId;
        msg.transactionTimeoutMs = transactionTimeoutMs_;
        msg.transactionalId      = txnId;
        r.frame.resize(msg.encodedSize());
        r.frame.resize(msg.encode(r.frame.data()));
        break;
    }
    case ReqKind::AddPartitions: {
        rpc::AddPartitionsToTxn msg;
        msg.sessionId       = 0;
        msg.correlationId   = r.corrId;
        msg.producerId      = producerId_;
        msg.producerEpoch   = producerEpoch_;
        msg.transactionalId = txnId;
        const rpc::TxnTopicPartition part{r.tp.topicId, r.tp.partition};
        r.frame.resize(rpc::AddPartitionsToTxn::encodedSizeFor(txnId, 1));
        r.frame.resize(msg.encode(r.frame.data(), std::span<const rpc::TxnTopicPartition>(&part, 1)));
        break;
    }
    case ReqKind::AddOffsets: {
        if (op == nullptr) return false;
        rpc::AddOffsetsToTxn msg;
        msg.sessionId       = 0;
        msg.correlationId   = r.corrId;
        msg.producerId      = producerId_;
        msg.producerEpoch   = producerEpoch_;
        msg.transactionalId = txnId;
        msg.groupId         = bytesOf(op->offsets.groupId);
        r.frame.resize(msg.encodedSize());
        r.frame.resize(msg.encode(r.frame.data()));
        break;
    }
    case ReqKind::TxnOffsetCommit: {
        if (op == nullptr) return false;
        std::vector<rpc::TxnOffsetCommitEntry> entries;
        entries.reserve(op->offsets.offsets.size());
        for (const auto& [tpk, off] : op->offsets.offsets) {
            rpc::TxnOffsetCommitEntry e;
            e.topicId   = tpk.topicId;
            e.partition = tpk.partition;
            e.offset    = off;
            entries.push_back(e);
        }
        rpc::TxnOffsetCommit msg;
        msg.sessionId       = 0;
        msg.correlationId   = r.corrId;
        msg.producerId      = producerId_;
        msg.producerEpoch   = producerEpoch_;
        msg.transactionalId = txnId;
        msg.groupId         = bytesOf(op->offsets.groupId);
        const std::span<const rpc::TxnOffsetCommitEntry> es(entries);
        r.frame.resize(rpc::TxnOffsetCommit::encodedSizeFor(txnId, msg.groupId, es));
        r.frame.resize(msg.encode(r.frame.data(), es));
        break;
    }
    case ReqKind::EndTxn: {
        rpc::EndTxn msg;
        msg.sessionId       = 0;
        msg.correlationId   = r.corrId;
        msg.producerId      = producerId_;
        msg.producerEpoch   = producerEpoch_;
        msg.commit          = (op != nullptr && op->commit) ? uint8_t{1} : uint8_t{0};
        msg.transactionalId = txnId;
        r.frame.resize(msg.encodedSize());
        r.frame.resize(msg.encode(r.frame.data()));
        break;
    }
    }
    return !r.frame.empty();
}

bool TransactionalProducerImpl::sendFrame_(std::vector<uint8_t>& frame, int64_t sessionId,
                                           const SendFn& sendFn) {
    if (frame.size() < rpc::MessageHeader::ENCODED_SIZE) return false;
    rpc::detail::putLE64(frame.data(), 8, sessionId);
    return sendFn(0, frame.data(), frame.size());
}

TransactionalProducerImpl::Verdict TransactionalProducerImpl::classify_(ReqKind kind, int16_t ec) noexcept {
    if (ec == kOk) return Verdict::Ok;
    if (ec == kConcurrentTransactions) return Verdict::Retry;        // F14: retriable everywhere
    if (ec == kNotCoordinator) return Verdict::Rediscover;           // F14: re-discover, then retry
    if (ec == kProducerFenced || ec == kInvalidProducerEpoch) return Verdict::Fenced;
    if (kind == ReqKind::EndTxn) {
        // F14: EndTxn is retried until 0 / 19 / 17 / 18.
        return ec == kInvalidTxnState ? Verdict::Fail : Verdict::Retry;
    }
    return Verdict::Fail;
}

void TransactionalProducerImpl::retry_(TxnRequest& r, Verdict v) {
    if (v == Verdict::Rediscover) {
        r.state = TxnRequest::AwaitCoordinator{discoveryGen_};
        // One re-discovery request per generation, however many ops wait on it.
        if (rediscoverRequestedGen_ != discoveryGen_) {
            rediscoverRequestedGen_ = discoveryGen_;
            if (rediscover_ != nullptr) rediscover_(rediscoverCtx_);
        }
        return;
    }
    const uint32_t shift = std::min<uint32_t>(r.attempt, 30u);
    r.state = TxnRequest::Backoff{std::min<uint32_t>(1u << shift, kRetryBackoffCapCycles)};
    ++r.attempt;
}

// ---------------------------------------------------------------------------
// Transaction state machine
// ---------------------------------------------------------------------------

TxnPartitionState& TransactionalProducerImpl::partition_(int32_t topicId, int32_t partition) {
    const rune::client::TopicPartitionKey k{topicId, partition};
    auto it = partitions_.find(k);
    if (it == partitions_.end()) it = partitions_.emplace(k, TxnPartitionState(topicId, partition)).first;
    return it->second;
}

void TransactionalProducerImpl::dropWaiting_() noexcept {
    for (auto& [k, ps] : partitions_) ps.waiting.clear();
}

void TransactionalProducerImpl::poison_(OpStatus st, uint16_t errorCode, bool bumpEpoch) {
    // The transaction can no longer commit: AbortableError (the app aborts).
    if (const auto* it = std::get_if<SInTransaction>(&state_)) {
        state_ = SAbortableError{st, errorCode, it->started, bumpEpoch, it->maybeStarted};
        dropWaiting_();
        publishStatus_();
        return;
    }
    if (const auto* c = std::get_if<SCommitting>(&state_)) {
        if (c->endSent) return;  // the data settled before EndTxn: nothing left to poison
        const uint64_t id = c->opId;
        state_ = SAbortableError{st, errorCode, c->started, bumpEpoch, c->maybeStarted};
        dropWaiting_();
        publishStatus_();
        // TX-6b: never a commit marker over this data — the commit fails and
        // the app must abort (which sends EndTxn(abort)).
        if (id != 0) {
            completeAndErase_(id, st, errorCode, "commitTransaction: the transaction failed — abortTransaction() it");
        }
        return;
    }
    // The first error is kept; a sequence gap still requires the epoch bump.
    if (auto* ae = std::get_if<SAbortableError>(&state_)) ae->bumpEpoch = ae->bumpEpoch || bumpEpoch;
    if (auto* a = std::get_if<SAborting>(&state_)) a->bumpEpoch = a->bumpEpoch || bumpEpoch;
}

void TransactionalProducerImpl::markStarted_() noexcept {
    if (auto* it = std::get_if<SInTransaction>(&state_)) it->started = true;
    else if (auto* c = std::get_if<SCommitting>(&state_)) c->started = true;
    else if (auto* a = std::get_if<SAborting>(&state_)) a->started = true;
    else if (auto* ae = std::get_if<SAbortableError>(&state_)) ae->started = true;
}

void TransactionalProducerImpl::markMaybeStarted_() noexcept {
    if (auto* it = std::get_if<SInTransaction>(&state_)) it->maybeStarted = true;
    else if (auto* c = std::get_if<SCommitting>(&state_)) c->maybeStarted = true;
    else if (auto* a = std::get_if<SAborting>(&state_)) a->maybeStarted = true;
    else if (auto* ae = std::get_if<SAbortableError>(&state_)) ae->maybeStarted = true;
}

void TransactionalProducerImpl::endDone_(uint64_t opId, int64_t nowNs) {
    if (auto* a = std::get_if<SAborting>(&state_); a != nullptr && a->bumpEpoch) {
        // B-3 (KIP-360): the aborted transaction left a sequence gap — bump
        // the epoch (InitProducerId for this transactionalId) and restart
        // every sequence before Ready.  The abort completes after the bump.
        // B-4: from here the transaction is ended on the broker — a retried
        // abort resumes at the bump, never re-sends EndTxn (stale epoch).
        a->ended = true;
        UserOp* op = findOp_(opId);
        // m14: the bump is driven by its op; without one it could never be
        // re-issued (a silent sequence gap).  Unreachable by construction.
        assert(op != nullptr && "KIP-360 bump pending without its abort op");
        if (op != nullptr) {
            op->deadlineNs = opDeadline_(nowNs);
            const int64_t dl   = op->deadlineNs;
            const int64_t corr = addRequest_(ReqKind::InitProducerId, opId, dl);
            if (UserOp* o = findOp_(opId)) o->phase = UserOp::Bumping{corr};
            return;
        }
    }
    finishTxn_();  // the final answer: only now is the transaction closed locally
    completeAndErase_(opId, OpStatus::Ok);
}

void TransactionalProducerImpl::finishTxn_() {
    // EndTxn's final answer: only now is the transaction closed locally.
    for (auto& [k, ps] : partitions_) ps.add = TxnPartitionState::Add::NotAdded;
    state_ = SReady{};
    publishStatus_();
}

void TransactionalProducerImpl::publishStatus_() {
    TxnStatus s;
    s.producerId    = producerId_;
    s.producerEpoch = producerEpoch_;
    s.state         = static_cast<uint8_t>(stateDuty());
    s.txnActive     = txnOpen_(stateDuty()) ? 1 : 0;
    if (!statusDirty_ && s.producerId == statusLast_.producerId &&
        s.producerEpoch == statusLast_.producerEpoch && s.state == statusLast_.state) {
        return;
    }
    statusLast_  = s;
    statusDirty_ = !status.tryPublish(s) && !status.closed();
}

void TransactionalProducerImpl::enterFatal_(uint16_t errorCode, uint64_t exceptOpId) {
    // Terminal (Kafka ProducerFencedException): nothing is ever sent again.
    state_ = SFatalError{errorCode};
    clearBatches_();
    partitions_.clear();
    reqs_.clear();
    publishStatus_();
    // Every pending op completes Fenced — after the tables are emptied (a
    // handler may issue a new op: it is refused Fenced).
    std::vector<UserOp> ops;
    for (std::size_t i = 0; i < ops_.size();) {
        if (ops_[i].id == exceptOpId) { ++i; continue; }
        ops.push_back(std::move(ops_[i]));
        ops_.erase(ops_.begin() + static_cast<std::ptrdiff_t>(i));
    }
    for (UserOp& op : ops) {
        complete_(op, OpStatus::Fenced, errorCode, "TransactionalProducer: fenced — fatal");
    }
}

std::size_t TransactionalProducerImpl::testWaitingRecords() const noexcept {
    std::size_t n = 0;
    for (const auto& [k, ps] : partitions_) n += ps.waiting.size();
    return n;
}

int64_t TransactionalProducerImpl::testRequestCorrId(uint8_t kind) const noexcept {
    for (const auto& r : reqs_) {
        if (static_cast<uint8_t>(r.kind) == kind) return r.corrId;
    }
    return 0;
}
// ---------------------------------------------------------------------------
// Duty cycle
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::produce_(TxnPartitionState& ps, TxnSendRecord& rec, int64_t nowNs,
                                         int64_t sessionId, const SendFn& sendFn, int& work) {
    rune::protocol::RecordBatchBuilder builder(0, wallMs());
    const std::string_view kv(reinterpret_cast<const char*>(rec.key.bytes().data()), rec.key.size());
    const std::string_view vv(reinterpret_cast<const char*>(rec.value.data()), rec.value.size());
    builder.append(kv, vv, wallMs());
    builder.setProducerId(producerId_);
    builder.setProducerEpoch(producerEpoch_);
    // TX-6: mark the DATA batch transactional (attributes bit 0x10) — the
    // broker's LsoTracker / read_committed filter gate on it.
    builder.setTransactional(true);
    const int32_t baseSeq = ps.nextBaseSequence;
    builder.setBaseSequence(baseSeq);
    builder.buildInto(batchScratch_);
    // Advance NOW: the retained frame re-sends the same baseSeq on a retry.
    ps.nextBaseSequence += builder.recordCount();

    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = nextCorrId_++;
    msg.topicId        = rec.topicId;
    msg.partition      = rec.partition;
    msg.partitionKnown = 1;
    msg.acks           = (cfg_.acks != Acks::None) ? uint8_t{1} : uint8_t{0};
    msg.key            = {};
    // TX-6: the idempotent / transactional identity on the FRAME as well.
    msg.producerId     = producerId_;
    msg.producerEpoch  = producerEpoch_;
    msg.baseSequence   = baseSeq;
    msg.recordBatch    = std::span<const uint8_t>(batchScratch_);

    InFlightBatch ib;
    ib.topicId      = rec.topicId;
    ib.partition    = rec.partition;
    ib.baseSequence = baseSeq;
    ib.recordCount  = builder.recordCount();
    ib.deadlineNs   = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
    ib.wireFrame.resize(msg.encodedSize());
    ib.wireFrame.resize(msg.encode(ib.wireFrame.data()));
    if (ib.wireFrame.empty()) return;
    // Retained even when the transport refuses it now: re-offered every cycle
    // (never silently lost) until acked or its deadline poisons the txn.
    ib.sentSessionId = sendFrame_(ib.wireFrame, sessionId, sendFn) ? sessionId : 0;
    ib.offered       = ib.sentSessionId != 0;
    ib.sendTick      = ++sendTick_;
    ++ps.inFlight;
    batches_.emplace(msg.correlationId, std::move(ib));
    ++work;
}

bool TransactionalProducerImpl::dataSettled_() const noexcept {
    if (!batches_.empty()) return false;
    for (const auto& [k, ps] : partitions_) {
        if (!ps.waiting.empty()) return false;
    }
    for (const auto& r : reqs_) {
        if (r.kind == ReqKind::AddPartitions) return false;
    }
    for (const auto& op : ops_) {
        if (std::holds_alternative<UserOp::AddOffsets>(op.phase) ||
            std::holds_alternative<UserOp::CommitOffsets>(op.phase)) {
            return false;  // Kafka: the offsets are part of the transaction being ended
        }
    }
    return true;
}

void TransactionalProducerImpl::gateEndTxn_(int64_t nowNs) {
    uint64_t id = 0;
    bool started = false;
    bool* endSent = nullptr;
    bool ended = false;
    if (auto* c = std::get_if<SCommitting>(&state_)) {
        id = c->opId; started = c->started || c->maybeStarted; endSent = &c->endSent;
    } else if (auto* a = std::get_if<SAborting>(&state_)) {
        id = a->opId; started = a->started || a->maybeStarted; endSent = &a->endSent; ended = a->ended;
    }
    if (id == 0) return;  // none, or in doubt (waiting for the app's retry)
    UserOp* op = findOp_(id);
    if (op == nullptr || !std::holds_alternative<UserOp::AwaitData>(op->phase) || !dataSettled_()) return;
    if (ended) {
        // B-4: EndTxn(abort) was already answered Ok; only the epoch bump is
        // left — re-send InitProducerId, never EndTxn under the stale epoch.
        endDone_(id, nowNs);
        return;
    }
    if (!started) {
        // B-1 (Kafka): nothing was ever added — no EndTxn (the coordinator
        // would answer 19 for a transaction that is not Ongoing).  End locally.
        endDone_(id, nowNs);
        return;
    }
    *endSent = true;
    // I-4: EndTxn's retry budget starts now (the data wait does not consume it).
    op->deadlineNs = opDeadline_(nowNs);
    const int64_t deadline = op->deadlineNs;
    const int64_t corr = addRequest_(ReqKind::EndTxn, id, deadline);
    if (UserOp* o = findOp_(id)) o->phase = UserOp::Ending{corr};
    // The local transaction state resets only on EndTxn's final answer.
}

void TransactionalProducerImpl::sweep_(int64_t nowNs) {
    // User ops past their deadline: Timeout (exactly once; their requests go).
    for (;;) {
        uint64_t expired = 0;
        OpKind   kind    = OpKind::InitTransactions;
        for (const auto& op : ops_) {
            if (nowNs >= op.deadlineNs) { expired = op.id; kind = op.kind; break; }
        }
        if (expired == 0) break;
        // m15: an AddOffsets sent but unanswered may have opened the
        // transaction on the coordinator.
        for (const auto& r : reqs_) {
            if (r.opId == expired && r.kind == ReqKind::AddOffsets && r.sent) { markMaybeStarted_(); break; }
        }
        eraseReqsOf_(expired);
        if (const auto* in = std::get_if<SInitializing>(&state_); in != nullptr && in->opId == expired) {
            state_ = SUninitialized{};  // init may be retried
            publishStatus_();
        } else if (auto* c = std::get_if<SCommitting>(&state_); c != nullptr && c->opId == expired) {
            // B-2 (Kafka m1): in doubt (EndTxn may or may not have landed) —
            // stay Committing; the app retries commitTransaction (abort refused).
            c->opId = 0;
        } else if (auto* a = std::get_if<SAborting>(&state_); a != nullptr && a->opId == expired) {
            a->opId = 0;  // in doubt: stay Aborting; the app retries abortTransaction
        }
        completeAndErase_(expired, OpStatus::Timeout);
        if (kind == OpKind::SendOffsets) poison_(OpStatus::Timeout, 0, false);  // offsets in doubt
    }
    // AddPartitions past its deadline: the partition is refused, the txn poisoned.
    for (std::size_t i = 0; i < reqs_.size();) {
        TxnRequest& r = reqs_[i];
        if (r.kind == ReqKind::AddPartitions && nowNs >= r.deadlineNs) {
            TxnPartitionState& ps = partition_(r.tp.topicId, r.tp.partition);
            ps.add = TxnPartitionState::Add::Failed;
            ps.waiting.clear();
            if (r.sent) markMaybeStarted_();  // m15: it may have landed (Ongoing, no data)
            reqs_.erase(reqs_.begin() + static_cast<std::ptrdiff_t>(i));
            poison_(OpStatus::DeliveryFailed, 0, false);  // never produced: no sequence gap
        } else {
            ++i;
        }
    }
    // Produce batches past their deadline: dropped, the txn poisoned (TX-6b).
    bool lost = false;
    for (auto it = batches_.begin(); it != batches_.end();) {
        if (nowNs >= it->second.deadlineNs) {
            const auto dead = it++;
            eraseBatch_(dead);
            lost = true;
        } else {
            ++it;
        }
    }
    if (lost) poison_(OpStatus::DeliveryFailed, 0, /*bumpEpoch=*/true);  // a sequence gap
}

int TransactionalProducerImpl::doWork(int64_t nowNs, int64_t sessionId, const SendFn& sendFn) {
    if (closed_) return 0;
    if (closingDuty()) [[unlikely]] {
        closeStep_(nowNs);  // may close the producer
        if (closed_) return 1;
    }
    if (std::holds_alternative<SFatalError>(state_)) return 0;
    lastNowNs_ = nowNs;
    int work = 0;
    if (statusDirty_) [[unlikely]] publishStatus_();  // a snapshot the ring refused: retried

    sweep_(nowNs);

    // A new session: whatever was in flight on the old one gets no answer —
    // re-send it (re-stamped) on this one.
    if (sessionId != 0 && sessionId != lastSessionId_) {
        for (auto& r : reqs_) {
            if (const auto* f = std::get_if<TxnRequest::InFlight>(&r.state); f != nullptr && f->sessionId != sessionId) {
                r.state = TxnRequest::Unsent{};
            }
        }
        for (auto& [corr, b] : batches_) {
            if (b.sentSessionId != 0 && b.sentSessionId != sessionId) b.sentSessionId = 0;
        }
        lastSessionId_ = sessionId;
    }

    // F14 backoff / re-discovery, counted in duty cycles (no wall clock; the
    // cap's duration therefore follows the runner's idle strategy).
    for (auto& r : reqs_) {
        if (auto* b = std::get_if<TxnRequest::Backoff>(&r.state)) {
            if (b->cyclesLeft <= 1) r.state = TxnRequest::Unsent{};
            else --b->cyclesLeft;
        } else if (const auto* a = std::get_if<TxnRequest::AwaitCoordinator>(&r.state);
                   a != nullptr && a->discoveryGen != discoveryGen_) {
            const uint32_t shift = std::min<uint32_t>(r.attempt, 30u);
            r.state = TxnRequest::Backoff{std::min<uint32_t>(1u << shift, kRetryBackoffCapCycles)};
            ++r.attempt;
        }
    }

    // F14 gate: AddPartitions first; a partition's records are produced only
    // once its AddPartitions result is OK.  Records exist only in an open
    // transaction (InTransaction, or Committing — flushed before EndTxn).
    const bool flowing = std::holds_alternative<SInTransaction>(state_) ||
                         std::holds_alternative<SCommitting>(state_);
    if (flowing) {
        for (auto& [k, ps] : partitions_) {
            if (ps.waiting.empty()) continue;
            if (ps.add == TxnPartitionState::Add::NotAdded) {
                ps.add = TxnPartitionState::Add::Adding;
                TxnRequest r;
                r.kind       = ReqKind::AddPartitions;
                r.corrId     = nextCorrId_++;
                r.tp         = k;
                r.deadlineNs = opDeadline_(nowNs);  // m7: a coordinator op (27 retries)
                (void)encode_(r, nullptr);
                reqs_.push_back(std::move(r));
                ++work;
            }
            if (ps.add != TxnPartitionState::Add::Added || sessionId == 0) continue;
            // M-2: at most the broker's dedup window in flight per partition;
            // I-1: nothing new while batches are parked (they would meet the
            // same gap — Kafka stops draining a partition with a retry outstanding).
            constexpr uint32_t kWindow = static_cast<uint32_t>(rpc::kIdempotentMaxInFlightBatches);
            // NEW-3: nor while a batch waits for the partition's new route.
            while (!ps.waiting.empty() && ps.parked == 0 && ps.awaitingRoute == 0 && ps.inFlight < kWindow) {
                // int32 sequence space (one record per batch): the client
                // chooses never to wrap (N-c: since rune-broker 37564ba the broker
                // accepts the Kafka wrap INT32_MAX -> 0 in order, and refuses a
                // batch crossing INT32_MAX; not wrapping stays compatible and
                // conservative).  The epoch belongs to the coordinator, so the
                // transaction becomes abortable with an epoch bump (InitProducerId
                // restarts every sequence at 0) BEFORE the counter could overflow
                // — after 2^31 records on one partition within one epoch.
                if (ps.nextBaseSequence == INT32_MAX) [[unlikely]] {
                    poison_(OpStatus::DeliveryFailed, 0, /*bumpEpoch=*/true);
                    break;
                }
                produce_(ps, ps.waiting.front(), nowNs, sessionId, sendFn, work);
                ps.waiting.pop_front();
            }
        }
    }

    // TX-6: EndTxn once the data settled (may add the EndTxn request).
    gateEndTxn_(nowNs);
    // F8c C7: a close whose abort just ended locally completes in this cycle.
    if (closingDuty()) [[unlikely]] {
        closeStep_(nowNs);
        if (closed_) return work + 1;
    }

    if (sessionId == 0) return work;

    // Re-offer refused / re-sent produce batches, per partition in sequence
    // order.  A batch that was on the wire before goes out as a NEW attempt
    // (fresh correlationId; I-2).
    {
        std::vector<int64_t>& due = dueScratch_;  // reused: no per-cycle allocation
        due.clear();
        for (auto& [corr, b] : batches_) {
            // NEW-3: a NotLeader'd batch waits for a MetadataResponse that
            // arrived after its NotLeader (the new route is installed by then).
            if (b.awaitRoute && b.awaitGen != discoveryGen_) {
                b.awaitRoute = false;
                if (auto pit = partitions_.find(rune::client::TopicPartitionKey{b.topicId, b.partition});
                    pit != partitions_.end() && pit->second.awaitingRoute > 0) {
                    --pit->second.awaitingRoute;
                }
            }
            if (b.sentSessionId == 0 && !b.parked && !b.awaitRoute) due.push_back(corr);
        }
        std::sort(due.begin(), due.end(), [&](int64_t x, int64_t y) {
            const InFlightBatch& a = batches_.at(x);
            const InFlightBatch& c = batches_.at(y);
            if (a.topicId != c.topicId) return a.topicId < c.topicId;
            if (a.partition != c.partition) return a.partition < c.partition;
            return a.baseSequence < c.baseSequence;
        });
        for (int64_t corr : due) {
            auto it = batches_.find(corr);
            if (it->second.offered) {
                const int64_t next = nextCorrId_++;
                auto node = batches_.extract(it);
                node.key() = next;
                InFlightBatch& b = node.mapped();
                rpc::detail::putLE64(b.wireFrame.data(), 16, next);  // MessageHeader.correlationId
                b.priorCorrIds.push_back(corr);
                for (int64_t old : b.priorCorrIds) superseded_[old] = next;
                it = batches_.insert(std::move(node)).position;
            }
            InFlightBatch& b = it->second;
            if (sendFrame_(b.wireFrame, sessionId, sendFn)) {
                b.sentSessionId = sessionId;
                b.offered       = true;
                b.sendTick      = ++sendTick_;
                ++work;
            }
        }
    }
    // Send every Unsent request (a refused offer is re-offered next cycle).
    for (auto& r : reqs_) {
        if (!std::holds_alternative<TxnRequest::Unsent>(r.state)) continue;
        if (sendFrame_(r.frame, sessionId, sendFn)) {
            r.sent  = true;
            r.state = TxnRequest::InFlight{sessionId};
            ++work;
        }
    }
    return work;
}

// ---------------------------------------------------------------------------
// Responses (duty thread)
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::onInitProducerIdResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::InitProducerIdResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::InitProducerId) return;
    const uint64_t opId = r->opId;
    const UserOp*  op   = findOp_(opId);
    const bool     bump = op != nullptr && std::holds_alternative<UserOp::Bumping>(op->phase);
    switch (const Verdict v = classify_(ReqKind::InitProducerId, opt->errorCode)) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        producerId_    = opt->producerId;
        producerEpoch_ = opt->producerEpoch;
        for (auto& [k, ps] : partitions_) ps.nextBaseSequence = 0;  // a fresh / bumped epoch
        if (bump) {
            finishTxn_();  // KIP-360: the abort is complete under the bumped epoch
        } else {
            state_ = SReady{};
            publishStatus_();  // published BEFORE the completion: the app reads it after
        }
        completeAndErase_(opId, OpStatus::Ok);
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(opt->errorCode));
        return;
    case Verdict::Fail:
        // m3 (Kafka InitProducerIdHandler): anything not retriable is fatal.
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(opt->errorCode), /*except=*/opId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          bump ? "InitProducerId (epoch bump) failed (fatal)" : "InitProducerId failed (fatal)");
        return;
    }
}

void TransactionalProducerImpl::onAddPartitionsToTxnResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::AddPartitionsToTxnResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::AddPartitions) return;
    int16_t ec = 0;
    for (std::size_t i = 0; i < opt->results.size(); ++i) {
        if (opt->results[i].errorCode != 0) { ec = opt->results[i].errorCode; break; }
    }
    const rune::client::TopicPartitionKey tp = r->tp;
    switch (const Verdict v = classify_(ReqKind::AddPartitions, ec)) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        partition_(tp.topicId, tp.partition).add = TxnPartitionState::Add::Added;  // produced next cycle
        markStarted_();  // B-1: the transaction exists on the coordinator now
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(ec));
        return;
    case Verdict::Fail: {
        // F14: no longer ignored — the partition is not in the transaction:
        // its records are never produced, the transaction is abortable only.
        eraseReq_(opt->correlationId);
        TxnPartitionState& ps = partition_(tp.topicId, tp.partition);
        ps.add = TxnPartitionState::Add::Failed;
        ps.waiting.clear();
        poison_(OpStatus::BrokerError, static_cast<uint16_t>(ec), false);
        return;
    }
    }
}

void TransactionalProducerImpl::onAddOffsetsToTxnResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::AddOffsetsToTxnResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::AddOffsets) return;
    const uint64_t opId = r->opId;
    switch (const Verdict v = classify_(ReqKind::AddOffsets, opt->errorCode)) {
    case Verdict::Ok: {
        eraseReq_(opt->correlationId);
        markStarted_();  // B-1: AddOffsetsToTxn OK — the transaction is started
        UserOp* op = findOp_(opId);
        if (op == nullptr) return;
        // Phase 2: the TxnOffsetCommit carrying the offsets.
        const int64_t corr = addRequest_(ReqKind::TxnOffsetCommit, opId, op->deadlineNs);
        if (UserOp* o = findOp_(opId)) o->phase = UserOp::CommitOffsets{corr};
        return;
    }
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(opt->errorCode));
        return;
    case Verdict::Fail:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          "AddOffsetsToTxn failed");
        poison_(OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode), false);
        return;
    }
}

void TransactionalProducerImpl::onTxnOffsetCommitResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::TxnOffsetCommitResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::TxnOffsetCommit) return;
    const uint64_t opId = r->opId;
    // M-12: the most severe per-partition verdict decides (fenced > failed >
    // re-discover > retry > ok) — a 27 never masks a non-retriable error.
    Verdict v = Verdict::Ok;
    int16_t ec = 0;
    auto rank = [](Verdict x) {
        switch (x) {
        case Verdict::Fenced:     return 4;
        case Verdict::Fail:       return 3;
        case Verdict::Rediscover: return 2;
        case Verdict::Retry:      return 1;
        default:                  return 0;
        }
    };
    for (std::size_t i = 0; i < opt->results.size(); ++i) {
        const int16_t code = opt->results[i].errorCode;
        const Verdict x    = classify_(ReqKind::TxnOffsetCommit, code);
        if (rank(x) > rank(v)) { v = x; ec = code; }
    }
    switch (v) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::Ok);
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(ec));
        return;
    case Verdict::Fail:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(ec), "TxnOffsetCommit failed");
        poison_(OpStatus::BrokerError, static_cast<uint16_t>(ec), false);
        return;
    }
}

void TransactionalProducerImpl::onEndTxnResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::EndTxnResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::EndTxn) return;
    const uint64_t opId = r->opId;
    switch (const Verdict v = classify_(ReqKind::EndTxn, opt->errorCode)) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        endDone_(opId, lastNowNs_);  // Ready (or the KIP-360 epoch bump first)
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        eraseReq_(opt->correlationId);
        enterFatal_(static_cast<uint16_t>(opt->errorCode));
        return;
    case Verdict::Fail:  // 19 InvalidTxnState: fatal in Kafka
        eraseReq_(opt->correlationId);
        if (const auto* a = std::get_if<SAborting>(&state_);
            a != nullptr && a->maybeStarted && !a->started) {
            // m15: the transaction was only POSSIBLY started (a timed-out
            // AddPartitions / AddOffsets): 19 = nothing open on the broker —
            // the abort is done.
            endDone_(opId, lastNowNs_);
            return;
        }
        enterFatal_(static_cast<uint16_t>(opt->errorCode), /*except=*/opId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          "EndTxn failed: InvalidTxnState (fatal)");
        return;
    }
}

void TransactionalProducerImpl::eraseBatch_(std::map<int64_t, InFlightBatch>::iterator it) {
    const InFlightBatch b = std::move(it->second);
    batches_.erase(it);
    for (int64_t old : b.priorCorrIds) superseded_.erase(old);
    auto pit = partitions_.find(rune::client::TopicPartitionKey{b.topicId, b.partition});
    if (pit == partitions_.end()) return;
    TxnPartitionState& ps = pit->second;
    if (ps.inFlight > 0) --ps.inFlight;
    if (b.parked && ps.parked > 0) --ps.parked;
    if (b.awaitRoute && ps.awaitingRoute > 0) --ps.awaitingRoute;
    armParked_(ps);
}

void TransactionalProducerImpl::clearBatches_() noexcept {
    batches_.clear();
    superseded_.clear();
    for (auto& [k, ps] : partitions_) {
        ps.inFlight      = 0;
        ps.parked        = 0;
        ps.awaitingRoute = 0;
    }
}

void TransactionalProducerImpl::armParked_(TxnPartitionState& ps) {
    if (ps.parked == 0) return;
    // The first parked batch (lowest sequence) and whether something not yet
    // known stored lies before it (an OK proves every lower sequence stored).
    int32_t firstParked = INT32_MAX;
    for (const auto& [c, b] : batches_) {
        if (b.topicId == ps.topicId && b.partition == ps.partition && b.parked) {
            firstParked = std::min(firstParked, b.baseSequence);
        }
    }
    for (const auto& [c, b] : batches_) {
        if (b.topicId != ps.topicId || b.partition != ps.partition || b.parked) continue;
        if (b.baseSequence < firstParked && b.baseSequence > ps.lastAckedSeq) return;  // the gap is still open
    }
    for (auto& [c, b] : batches_) {
        if (b.topicId == ps.topicId && b.partition == ps.partition && b.parked) {
            b.parked        = false;
            b.sentSessionId = 0;  // re-sent next cycle (a new attempt), in sequence order
        }
    }
    ps.parked = 0;
}

void TransactionalProducerImpl::restartSequences_(TxnPartitionState& ps) {
    std::vector<int64_t>& mine = restartScratch_;
    mine.clear();
    for (const auto& [c, b] : batches_) {
        if (b.topicId == ps.topicId && b.partition == ps.partition) mine.push_back(c);
    }
    std::sort(mine.begin(), mine.end(),
              [&](int64_t x, int64_t y) { return batches_.at(x).baseSequence < batches_.at(y).baseSequence; });
    int32_t seq = 0;
    for (int64_t c : mine) {
        InFlightBatch& b = batches_.at(c);
        auto& f = b.wireFrame;
        const auto view = rpc::Produce::decode(f.data(), f.size());
        namespace rb = rune::protocol::batch;
        if (!view || view->recordBatch.size() < static_cast<std::size_t>(rb::kRecordBatchOverhead)) [[unlikely]] {
            // Unreachable (this producer encoded the frame): never re-send a
            // frame whose identity could not be re-stamped — abortable instead.
            poison_(OpStatus::DeliveryFailed, 0, /*bumpEpoch=*/true);
            return;
        }
        {
            constexpr std::size_t kFrameSeqOff = rpc::MessageHeader::ENCODED_SIZE + 20;  // Produce.baseSequence
            uint8_t* batch = f.data() + (view->recordBatch.data() - f.data());
            const std::size_t len = view->recordBatch.size();
            rpc::detail::putLE32(f.data(), static_cast<int>(kFrameSeqOff), seq);
            rpc::detail::putLE32(batch, rb::kBaseSequenceOffset, seq);
            rpc::detail::putLE32(batch, rb::kCrcOffset,
                                 static_cast<int32_t>(rune::protocol::crc32c(
                                     batch + rb::kAttributesOffset, len - static_cast<std::size_t>(rb::kAttributesOffset))));
        }
        b.baseSequence  = seq;
        seq += b.recordCount;
        b.parked        = false;
        b.sentSessionId = 0;  // re-sent next cycle
        if (b.offered) {
            // A NEW attempt id NOW (I-2): a late answer of an attempt under the
            // old sequence (typically 28 for the batches behind the head)
            // arrives as a superseded attempt — it never decides or parks.
            const int64_t next = nextCorrId_++;
            auto node = batches_.extract(c);
            node.key() = next;
            InFlightBatch& nb = node.mapped();
            rpc::detail::putLE64(nb.wireFrame.data(), 16, next);  // MessageHeader.correlationId
            nb.priorCorrIds.push_back(c);
            for (int64_t old : nb.priorCorrIds) superseded_[old] = next;
            nb.offered = false;  // goes out under `next` as is (no second re-key)
            batches_.insert(std::move(node));
        }
    }
    ps.parked           = 0;
    ps.lastAckedSeq     = -1;
    ps.nextBaseSequence = seq;
}

void TransactionalProducerImpl::onProduceResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::ProduceResponse::decode(buf, len);
    if (!opt) return;
    // Re-review 1 NEW-4(a): DuplicateSequence (Kafka DUPLICATE_SEQUENCE_NUMBER) =
    // already stored, offset unknown: an OK.
    // D6: UnknownProducerId (the broker forgot this producer: its batch was NOT
    // appended) shares OutOfOrderSequence's parking (behind an older unresolved
    // batch / an answer that may predate an OK).  On the next-in-line batch,
    // KIP-360 (D6 review M2, Kafka TransactionManager.canRetry): retention
    // (lastAckedOffset < logStartOffset) restarts the partition's sequences and
    // KEEPS the transaction; otherwise it is abortable with code 28.
    const int16_t ec = opt->errorCode == static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence) ? kOk
                                                                                                 : opt->errorCode;
    const bool unknownPid = ec == static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);
    auto it = batches_.find(opt->correlationId);
    if (it == batches_.end()) {
        // I-2: an earlier attempt's answer.  Its OK still decides the batch
        // (stored is stored); its error never does.
        const auto sup = superseded_.find(opt->correlationId);
        if (sup == superseded_.end() || ec != kOk) return;
        it = batches_.find(sup->second);
        if (it == batches_.end()) return;
    }
    TxnPartitionState* ps = nullptr;
    if (auto pit = partitions_.find(rune::client::TopicPartitionKey{it->second.topicId, it->second.partition});
        pit != partitions_.end()) {
        ps = &pit->second;
    }
    if (ec == kOk) {
        if (ps != nullptr) {
            ps->lastAckedSeq = std::max(ps->lastAckedSeq, it->second.baseSequence);
            ps->lastAckTick  = sendTick_;
            // D6 review M2: the max acked LAST offset (a DuplicateSequence OK -1 carries none).
            if (opt->baseOffset >= 0)
                ps->lastAckedOffset = std::max(ps->lastAckedOffset, opt->baseOffset + it->second.recordCount - 1);
        }
        eraseBatch_(it);
        return;
    }
    if (ec == kInvalidProducerEpoch || ec == kProducerFenced) {
        eraseBatch_(it);
        enterFatal_(static_cast<uint16_t>(ec));
        return;
    }
    InFlightBatch& ib = it->second;
    if (ec == kNotLeader) {
        // Re-review 1 NEW-3 (Kafka NOT_LEADER_OR_FOLLOWER parity, as ProducerImpl
        // C1): the partition moved — retriable, never a reason to abort.  No
        // retry budget is spent and the identity is unchanged.  The batch waits
        // for a MetadataResponse (the Client requests one on every
        // ProduceResponse{NotLeader}, rate-limited) so the re-offer takes the
        // partition's NEW route; then it goes out again as a new attempt (fresh
        // correlationId; the old owner's late answers are superseded).  The
        // partition produces nothing new meanwhile.  Bounded by the batch
        // deadline (sweep_).  Only the CURRENT attempt's NotLeader gets here: a
        // superseded attempt's error never does (I-2).
        if (!ib.awaitRoute) {
            ib.awaitRoute = true;
            if (ps != nullptr) ++ps->awaitingRoute;
        }
        ib.awaitGen      = discoveryGen_;
        ib.sentSessionId = 0;
        return;
    }
    if ((ec == kOutOfOrderSequence || unknownPid) && ps != nullptr) {
        // I-1 (Kafka parity, ruling 3): an older batch of this partition is
        // still unresolved -> an earlier frame was lost: park, retry later.
        const InFlightBatch* gap = nullptr;
        bool parkedBetween = false, olderLive = false;
        for (const auto& [c, o] : batches_) {
            if (&o == &ib || o.topicId != ib.topicId || o.partition != ib.partition) continue;
            if (o.baseSequence >= ib.baseSequence) continue;
            olderLive = true;
            if (gap == nullptr || o.baseSequence > gap->baseSequence) {
                if (o.parked) {
                    parkedBetween = true;
                    gap = &o;
                } else {
                    parkedBetween = false;
                    gap = &o;
                }
            }
        }
        if (olderLive) {
            if (!ib.parked) {
                ib.parked = true;
                ++ps->parked;
            }
            // M-1: fast retransmit of the frame right before the gap (once:
            // only if its latest offer went out before this batch's).
            if (gap != nullptr && !parkedBetween && gap->sendTick <= ib.sendTick) {
                for (auto& [c, o] : batches_) {
                    if (&o == gap) o.sentSessionId = 0;
                }
            }
            return;
        }
        if (ib.sendTick <= ps->lastAckTick) {  // may predate the latest OK (reordered answers): retry
            ib.sentSessionId = 0;
            return;
        }
        if (unknownPid && opt->logStartOffset >= 0 && ps->lastAckedOffset < opt->logStartOffset) {
            // D6 review M2 (KIP-360): retention removed every record of ours —
            // the broker accepts this producer again from sequence 0; nothing
            // of ours on this partition is stored at or above the log start,
            // so the restarted batches are not duplicates (the accepted
            // KIP-360 window: an earlier attempt below the log start whose OK
            // was lost outright).  The transaction is kept.
            restartSequences_(*ps);
            return;
        }
        // M-2: a sequence error on the next-in-line batch, not a fence — the
        // transaction is abortable (Kafka: transactional OOS -> abortable;
        // UNKNOWN_PRODUCER_ID that is not retention -> abortable, the code the
        // app sees stays 28 — D6 review M2).
        eraseBatch_(it);
        poison_(OpStatus::BrokerError, static_cast<uint16_t>(ec), /*bumpEpoch=*/true);
        return;
    }
    if (ib.retryCount >= InFlightBatch::kMaxRetries) {
        // Exhausted: this data never became durable — the txn must abort (TX-6b).
        eraseBatch_(it);
        poison_(OpStatus::DeliveryFailed, 0, /*bumpEpoch=*/true);  // a sequence gap
        return;
    }
    ++ib.retryCount;
    ib.sentSessionId = 0;  // re-sent next cycle: same bytes and baseSequence, a NEW attempt id
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::failAll() {
    if (closed_) return;
    closed_ = true;
    // F8c C7: a pending close token completes Closed — after every op.
    std::optional<LifeClosing> closing;
    if (auto* cl = std::get_if<LifeClosing>(&life_)) closing = *cl;
    life_ = LifeOpen{};
    reqs_.clear();
    clearBatches_();
    dropWaiting_();
    // The final snapshot, then the channel is finished (handles stop calling in).
    TxnStatus s;
    s.producerId    = producerId_;
    s.producerEpoch = producerEpoch_;
    s.state         = static_cast<uint8_t>(stateDuty());
    s.txnActive     = txnOpen_(stateDuty()) ? 1 : 0;
    (void)status.publishFinal(s);
    status.close();
    std::vector<UserOp> ops = std::move(ops_);
    ops_.clear();
    for (UserOp& op : ops) complete_(op, OpStatus::Closed);
    if (closing) {
        Completion c;
        c.token  = closing->token;
        c.op     = OpKind::TxnClose;
        c.status = OpStatus::Closed;
        completions_.deliver(closing->target, c);
    }
}

// ---------------------------------------------------------------------------
// F8c C7: close — abort an open broker transaction first, then close
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::beginClose(const detail::OpTarget& target, Token token, int64_t nowNs) {
    if (closed_ || closingDuty()) {
        // A second close (or one after closeImmediately): exactly one
        // completion, Closed — the first close owns the outcome.
        Completion c;
        c.token  = token;
        c.op     = OpKind::TxnClose;
        c.status = OpStatus::Closed;
        completions_.deliver(target, c);
        return;
    }
    LifeClosing cl;
    cl.target     = target;
    cl.token      = token;
    cl.deadlineNs = opDeadline_(nowNs);
    life_         = cl;
    closeStep_(nowNs);
}

uint64_t TransactionalProducerImpl::startCloseAbort_(int64_t nowNs) {
    UserOp op;
    op.id         = nextOpId_++;
    op.kind       = OpKind::AbortTransaction;
    op.internal   = true;
    op.deadlineNs = opDeadline_(nowNs);
    op.phase      = UserOp::AwaitData{};  // EndTxn once the data settled (TX-6)
    const uint64_t id = op.id;
    if (auto* a = std::get_if<SAborting>(&state_)) {
        if (a->opId != 0) return 0;      // an abort is already running: wait for it
        ops_.push_back(std::move(op));   // B-2: the in-doubt abort is retried
        a->opId = id;
    } else if (const auto* it = std::get_if<SInTransaction>(&state_)) {
        SAborting next;
        next.opId         = id;
        next.started      = it->started;
        next.maybeStarted = it->maybeStarted;
        ops_.push_back(std::move(op));
        dropWaiting_();  // aborted records are never produced
        state_ = next;
    } else if (const auto* ae = std::get_if<SAbortableError>(&state_)) {
        SAborting next;
        next.opId         = id;
        next.started      = ae->started;
        next.bumpEpoch    = ae->bumpEpoch;
        next.maybeStarted = ae->maybeStarted;
        ops_.push_back(std::move(op));
        dropWaiting_();
        state_ = next;
    } else {
        return 0;
    }
    publishStatus_();
    return id;
}

void TransactionalProducerImpl::closeStep_(int64_t nowNs) {
    auto* cl = std::get_if<LifeClosing>(&life_);
    if (cl == nullptr) return;
    const bool pastDeadline = nowNs >= cl->deadlineNs;
    const TxnState st = stateDuty();
    // The close's own abort is still running: wait (it has its own deadline,
    // equal to the close's — the sweep completes it Timeout).
    if (cl->abortOpId != 0 && !cl->abortDone) {
        if (pastDeadline) finishClose_(OpStatus::Timeout, 0);
        return;
    }
    switch (st) {
    case TxnState::InTransaction:
    case TxnState::AbortableError:
        if (cl->abortOpId == 0) {
            cl->abortOpId = startCloseAbort_(nowNs);  // EndTxn(abort) (or a local end: never started)
            return;
        }
        // Our abort completed yet the transaction is still open: it failed.
        finishClose_(cl->abortStatus, cl->abortCode);
        return;
    case TxnState::AbortingTransaction: {
        const auto& a = std::get<SAborting>(state_);
        if (a.opId != 0) {  // a user abort is running (ours completed): wait for it
            if (pastDeadline) finishClose_(OpStatus::Timeout, 0);
            return;
        }
        // In doubt (an abort timed out): retry it once as the close's own.
        if (cl->abortOpId == 0) {
            cl->abortOpId = startCloseAbort_(nowNs);
            return;
        }
        finishClose_(cl->abortStatus, cl->abortCode);
        return;
    }
    case TxnState::CommittingTransaction:
        // A commit is running: wait for its final answer (never aborted —
        // Kafka: COMMITTING -> ABORTING is invalid).  In doubt (it timed
        // out): nothing a close may do — report Timeout.
        if (std::get<SCommitting>(state_).opId == 0 || pastDeadline) finishClose_(OpStatus::Timeout, 0);
        return;
    case TxnState::Initializing:
        if (pastDeadline) finishClose_(OpStatus::Timeout, 0);
        return;
    case TxnState::Uninitialized:
    case TxnState::Ready:
    case TxnState::FatalError:
        // Nothing open on the broker (FatalError: fenced — the coordinator
        // aborts it).  The close reports its abort's outcome if it ran one.
        if (cl->abortOpId != 0) finishClose_(cl->abortStatus, cl->abortCode);
        else finishClose_(OpStatus::Ok, 0);
        return;
    }
}

void TransactionalProducerImpl::finishClose_(OpStatus st, uint16_t errorCode) {
    const LifeClosing cl = std::get<LifeClosing>(life_);
    life_ = LifeOpen{};  // failAll() must not complete the token Closed
    failAll();           // closed_: every remaining op Closed, status finished
    Completion c;
    c.token     = cl.token;
    c.op        = OpKind::TxnClose;
    c.status    = st;
    c.errorCode = errorCode;
    completions_.deliver(cl.target, c);
}

} // namespace rune::client::producer
