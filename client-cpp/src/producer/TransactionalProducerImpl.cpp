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
#include <chrono>
#include <cstring>
#include <memory>
#include <utility>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;

namespace {

inline int64_t wallNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
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

// ---------------------------------------------------------------------------
// Commands (duty thread)
// ---------------------------------------------------------------------------

bool TransactionalProducerImpl::isTxnOp(detail::HubCommand::Op op) noexcept {
    using Op = detail::HubCommand::Op;
    return op == Op::TxnInit || op == Op::TxnBegin || op == Op::TxnSend ||
           op == Op::TxnSendOffsets || op == Op::TxnEnd;
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
    default:
        return;
    }
    Completion c;
    c.token  = static_cast<Token>(cmd.a);
    c.op     = kind;
    c.status = OpStatus::Closed;
    sink.deliver(detail::decodeTarget(cmd), c);
}

void TransactionalProducerImpl::applyCommand(const detail::HubCommand& cmd, int64_t nowNs) {
    using Op = detail::HubCommand::Op;
    if (closed_) {
        discardCommand(cmd, completions_);
        return;
    }
    lastNowNs_ = nowNs;
    const int64_t deadline = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
    switch (cmd.op) {
    case Op::TxnBegin: {
        // TX-6b: a new transaction always starts clean (no stuck poison).
        txnFailed_ = false;
        failCode_  = 0;
        for (auto& [k, ps] : partitions_) {
            if (ps.add != TxnPartitionState::Add::Adding) ps.add = TxnPartitionState::Add::NotAdded;
        }
        txnActive_ = true;
        publishStatus_();
        return;
    }
    case Op::TxnSend: {
        std::unique_ptr<TxnSendRecord> rec(reinterpret_cast<TxnSendRecord*>(static_cast<uintptr_t>(cmd.d)));
        if (!rec) return;
        TxnPartitionState& ps = partition_(rec->topicId, rec->partition);
        if (txnActive_ && ps.add == TxnPartitionState::Add::Failed) return;  // poisoned txn: dropped
        ps.waiting.push_back(std::move(*rec));
        return;
    }
    case Op::TxnInit: {
        UserOp op;
        op.id         = nextOpId_++;
        op.kind       = OpKind::InitTransactions;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = deadline;
        const uint64_t id = op.id;
        ops_.push_back(std::move(op));
        const int64_t corr = addRequest_(ReqKind::InitProducerId, id, deadline);
        if (UserOp* o = findOp_(id)) o->phase = UserOp::Init{corr};
        return;
    }
    case Op::TxnSendOffsets: {
        std::unique_ptr<TxnOffsetsPayload> pl(
            reinterpret_cast<TxnOffsetsPayload*>(static_cast<uintptr_t>(cmd.d)));
        UserOp op;
        op.id         = nextOpId_++;
        op.kind       = OpKind::SendOffsets;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = deadline;
        if (pl) op.offsets = std::move(*pl);
        const uint64_t id = op.id;
        ops_.push_back(std::move(op));
        if (producerId_ == kNoPid) {
            completeAndErase_(id, OpStatus::BrokerError, static_cast<uint16_t>(kInvalidTxnState),
                              "sendOffsetsToTransaction: no producerId — call initTransactions first");
            return;
        }
        const int64_t corr = addRequest_(ReqKind::AddOffsets, id, deadline);
        if (UserOp* o = findOp_(id)) o->phase = UserOp::AddOffsets{corr};
        return;
    }
    case Op::TxnEnd: {
        UserOp op;
        op.id         = nextOpId_++;
        op.commit     = cmd.d != 0;
        op.kind       = op.commit ? OpKind::CommitTransaction : OpKind::AbortTransaction;
        op.target     = detail::decodeTarget(cmd);
        op.token      = static_cast<Token>(cmd.a);
        op.deadlineNs = deadline;
        op.phase      = UserOp::AwaitData{};  // TX-6: EndTxn only after every record is acked
        ops_.push_back(std::move(op));
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
    Completion c;
    c.token     = op.token;
    c.op        = op.kind;
    c.status    = st;
    c.errorCode = errorCode;
    if (st == OpStatus::Ok && op.kind == OpKind::InitTransactions) {
        c.value = producerId_;
        c.aux   = producerEpoch_;
    }
    completions_.deliver(op.target, c,
                         std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
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
// Transaction state
// ---------------------------------------------------------------------------

TxnPartitionState& TransactionalProducerImpl::partition_(int32_t topicId, int32_t partition) {
    const rune::client::TopicPartitionKey k{topicId, partition};
    auto it = partitions_.find(k);
    if (it == partitions_.end()) it = partitions_.emplace(k, TxnPartitionState(topicId, partition)).first;
    return it->second;
}

void TransactionalProducerImpl::poison_(uint16_t errorCode) {
    if (!txnActive_) return;
    if (!txnFailed_) {
        txnFailed_ = true;
        failCode_  = errorCode;
    }
}

void TransactionalProducerImpl::resetTxn_() {
    txnActive_ = false;
    txnFailed_ = false;
    failCode_  = 0;
    for (auto& [k, ps] : partitions_) {
        if (ps.add != TxnPartitionState::Add::Adding) ps.add = TxnPartitionState::Add::NotAdded;
    }
    publishStatus_();
}

void TransactionalProducerImpl::publishStatus_() {
    TxnStatus s;
    s.producerId    = producerId_;
    s.producerEpoch = producerEpoch_;
    s.txnActive     = txnActive_ ? 1 : 0;
    if (!statusDirty_ && s.producerId == statusLast_.producerId &&
        s.producerEpoch == statusLast_.producerEpoch && s.txnActive == statusLast_.txnActive) {
        return;
    }
    statusLast_  = s;
    statusDirty_ = !status.tryPublish(s) && !status.closed();
}

void TransactionalProducerImpl::handleFencing_() {
    producerId_    = kNoPid;
    producerEpoch_ = 0;
    txnActive_     = false;
    txnFailed_     = false;
    failCode_      = 0;
    partitions_.clear();  // sequences restart under the next identity
    batches_.clear();     // cannot be retried after the identity reset
    reqs_.clear();
    publishStatus_();
    // Every pending op completes Fenced — after the table is emptied (a
    // handler may issue a new op, e.g. initTransactions).
    std::vector<UserOp> ops = std::move(ops_);
    ops_.clear();
    for (UserOp& op : ops) {
        complete_(op, OpStatus::Fenced, static_cast<uint16_t>(kProducerFenced),
                  "TransactionalProducer: ProducerFenced — re-initialize");
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
    rune::protocol::RecordBatchBuilder builder(0, wallNs());
    const std::string_view kv(reinterpret_cast<const char*>(rec.key.bytes().data()), rec.key.size());
    const std::string_view vv(reinterpret_cast<const char*>(rec.value.data()), rec.value.size());
    builder.append(kv, vv, wallNs());
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
    ib.deadlineNs   = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
    ib.wireFrame.resize(msg.encodedSize());
    ib.wireFrame.resize(msg.encode(ib.wireFrame.data()));
    if (ib.wireFrame.empty()) return;
    // Retained even when the transport refuses it now: re-offered every cycle
    // (never silently lost) until acked or its deadline poisons the txn.
    ib.sentSessionId = sendFrame_(ib.wireFrame, sessionId, sendFn) ? sessionId : 0;
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
    (void)nowNs;
    for (const auto& op : ops_) {
        if (std::holds_alternative<UserOp::Ending>(op.phase)) return;  // one EndTxn at a time
    }
    uint64_t id = 0;
    bool commit = false;
    int64_t deadline = 0;
    for (const auto& op : ops_) {
        if (std::holds_alternative<UserOp::AwaitData>(op.phase)) {
            id = op.id;
            commit = op.commit;
            deadline = op.deadlineNs;
            break;
        }
    }
    if (id == 0 || !dataSettled_()) return;
    if (commit && txnFailed_) {
        // TX-6b: a record of this txn never became durable (or its partition
        // was refused): never write a commit marker over it.
        const uint16_t code = failCode_;
        resetTxn_();
        if (code != 0) {
            completeAndErase_(id, OpStatus::BrokerError, code,
                              "EndTxn: commit refused — AddPartitionsToTxn failed for a partition");
        } else {
            completeAndErase_(id, OpStatus::DeliveryFailed, 0,
                              "EndTxn: commit aborted — a data batch was not durably acknowledged");
        }
        return;
    }
    if (producerId_ == kNoPid) {
        completeAndErase_(id, OpStatus::BrokerError, static_cast<uint16_t>(kInvalidTxnState),
                          "EndTxn: no producerId — call initTransactions first");
        return;
    }
    const int64_t corr = addRequest_(ReqKind::EndTxn, id, deadline);
    if (UserOp* o = findOp_(id)) o->phase = UserOp::Ending{corr};
    resetTxn_();  // the transaction is decided: no longer active
}

void TransactionalProducerImpl::sweep_(int64_t nowNs) {
    // User ops past their deadline: Timeout (exactly once; their requests go).
    for (;;) {
        uint64_t expired = 0;
        for (const auto& op : ops_) {
            if (nowNs >= op.deadlineNs) { expired = op.id; break; }
        }
        if (expired == 0) break;
        eraseReqsOf_(expired);
        completeAndErase_(expired, OpStatus::Timeout);
    }
    // AddPartitions past its deadline: the partition is refused, the txn poisoned.
    for (std::size_t i = 0; i < reqs_.size();) {
        TxnRequest& r = reqs_[i];
        if (r.kind == ReqKind::AddPartitions && nowNs >= r.deadlineNs) {
            TxnPartitionState& ps = partition_(r.tp.topicId, r.tp.partition);
            ps.add = TxnPartitionState::Add::Failed;
            ps.waiting.clear();
            poison_(0);
            reqs_.erase(reqs_.begin() + static_cast<std::ptrdiff_t>(i));
        } else {
            ++i;
        }
    }
    // Produce batches past their deadline: dropped, the txn poisoned (TX-6b).
    for (auto it = batches_.begin(); it != batches_.end();) {
        if (nowNs >= it->second.deadlineNs) {
            it = batches_.erase(it);
            poison_(0);
        } else {
            ++it;
        }
    }
}

int TransactionalProducerImpl::doWork(int64_t nowNs, int64_t sessionId, const SendFn& sendFn) {
    if (closed_) return 0;
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

    // F14 backoff / re-discovery (duty cycles, no wall clock).
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
    // once its AddPartitions result is OK (outside a transaction: at once).
    for (auto& [k, ps] : partitions_) {
        if (ps.waiting.empty()) continue;
        if (txnActive_) {
            if (ps.add == TxnPartitionState::Add::NotAdded) {
                ps.add = TxnPartitionState::Add::Adding;
                TxnRequest r;
                r.kind       = ReqKind::AddPartitions;
                r.corrId     = nextCorrId_++;
                r.tp         = k;
                r.deadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
                (void)encode_(r, nullptr);
                reqs_.push_back(std::move(r));
                ++work;
            }
            if (ps.add != TxnPartitionState::Add::Added) continue;
        }
        if (sessionId == 0) continue;
        while (!ps.waiting.empty()) {
            produce_(ps, ps.waiting.front(), nowNs, sessionId, sendFn, work);
            ps.waiting.pop_front();
        }
    }

    // TX-6: EndTxn once every record is acked (may add the EndTxn request).
    gateEndTxn_(nowNs);

    if (sessionId == 0) return work;

    // Re-offer refused / re-sent produce batches.
    for (auto& [corr, b] : batches_) {
        if (b.sentSessionId != 0) continue;
        if (sendFrame_(b.wireFrame, sessionId, sendFn)) {
            b.sentSessionId = sessionId;
            ++work;
        }
    }
    // Send every Unsent request (a refused offer is re-offered next cycle).
    for (auto& r : reqs_) {
        if (!std::holds_alternative<TxnRequest::Unsent>(r.state)) continue;
        if (sendFrame_(r.frame, sessionId, sendFn)) {
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
    switch (const Verdict v = classify_(ReqKind::InitProducerId, opt->errorCode)) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        producerId_    = opt->producerId;
        producerEpoch_ = opt->producerEpoch;
        // A fresh identity restarts every partition's sequence.
        for (auto& [k, ps] : partitions_) ps.nextBaseSequence = 0;
        publishStatus_();  // published BEFORE the completion: the app reads it after
        completeAndErase_(opId, OpStatus::Ok);
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        handleFencing_();
        return;
    case Verdict::Fail:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          "InitProducerId failed");
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
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        handleFencing_();
        return;
    case Verdict::Fail: {
        // F14: no longer ignored — the partition is not in the transaction:
        // its records are never produced, and a commit must fail.
        eraseReq_(opt->correlationId);
        TxnPartitionState& ps = partition_(tp.topicId, tp.partition);
        ps.add = TxnPartitionState::Add::Failed;
        ps.waiting.clear();
        poison_(static_cast<uint16_t>(ec));
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
        handleFencing_();
        return;
    case Verdict::Fail:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          "AddOffsetsToTxn failed");
        return;
    }
}

void TransactionalProducerImpl::onTxnOffsetCommitResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::TxnOffsetCommitResponse::decode(buf, len);
    if (!opt) return;
    TxnRequest* r = findReq_(opt->correlationId);
    if (r == nullptr || r->kind != ReqKind::TxnOffsetCommit) return;
    const uint64_t opId = r->opId;
    int16_t ec = 0;
    for (std::size_t i = 0; i < opt->results.size(); ++i) {
        if (opt->results[i].errorCode != 0) { ec = opt->results[i].errorCode; break; }
    }
    switch (const Verdict v = classify_(ReqKind::TxnOffsetCommit, ec)) {
    case Verdict::Ok:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::Ok);
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        handleFencing_();
        return;
    case Verdict::Fail:
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(ec), "TxnOffsetCommit failed");
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
        completeAndErase_(opId, OpStatus::Ok);
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        retry_(*r, v);
        return;
    case Verdict::Fenced:
        handleFencing_();
        return;
    case Verdict::Fail:  // 19 InvalidTxnState
        eraseReq_(opt->correlationId);
        completeAndErase_(opId, OpStatus::BrokerError, static_cast<uint16_t>(opt->errorCode),
                          "EndTxn failed: InvalidTxnState");
        return;
    }
}

void TransactionalProducerImpl::onProduceResponse(const uint8_t* buf, std::size_t len) {
    auto opt = rpc::ProduceResponse::decode(buf, len);
    if (!opt) return;
    auto it = batches_.find(opt->correlationId);
    if (it == batches_.end()) return;  // not our produce
    const int16_t ec = opt->errorCode;
    if (ec == kOk) {
        batches_.erase(it);
        return;
    }
    if (ec == kOutOfOrderSequence || ec == kInvalidProducerEpoch || ec == kProducerFenced) {
        batches_.erase(it);
        handleFencing_();
        return;
    }
    InFlightBatch& ib = it->second;
    if (ib.retryCount >= InFlightBatch::kMaxRetries) {
        // Exhausted: this data never became durable — a commit must fail (TX-6b).
        batches_.erase(it);
        poison_(0);
        return;
    }
    ++ib.retryCount;
    ib.sentSessionId = 0;  // re-sent unchanged (same bytes, same baseSequence) next cycle
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

void TransactionalProducerImpl::failAll() {
    if (closed_) return;
    closed_ = true;
    reqs_.clear();
    batches_.clear();
    for (auto& [k, ps] : partitions_) ps.waiting.clear();
    // The final snapshot, then the channel is finished (handles stop calling in).
    TxnStatus s;
    s.producerId    = producerId_;
    s.producerEpoch = producerEpoch_;
    s.txnActive     = txnActive_ ? 1 : 0;
    (void)status.publishFinal(s);
    status.close();
    std::vector<UserOp> ops = std::move(ops_);
    ops_.clear();
    for (UserOp& op : ops) complete_(op, OpStatus::Closed);
}

} // namespace rune::client::producer
