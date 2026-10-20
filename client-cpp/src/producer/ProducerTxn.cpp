// src/producer/ProducerTxn.cpp - Phase 2b F Task 5: the transaction state machine of ProducerImpl (duty side).
//
// See ProducerImpl.h ("the transaction machine") and CoordinatorLink.h (the states).  Rules ported from the old
// TransactionalProducerImpl (Kafka TransactionManager):
//   * gating: begin only in Ready; commit / sendOffsets only InTransaction; abort in InTransaction /
//     AbortableError; init only Uninitialized; FatalError: every op completes with its status; misuse completes
//     BrokerError(19 InvalidTxnState);
//   * B-1: a transaction the coordinator never saw (TxnStarted::No) ends locally - no EndTxn;
//   * B-2: a commit / abort that times out is InDoubt; the SAME call re-sends EndTxn (abort refused while
//     committing); the local state resets only on EndTxn's final answer;
//   * B-3 / B-4 (KIP-360): a sequence gap makes the abort bump the epoch (InitProducerId after EndTxn(abort));
//     once EndTxn(abort) is answered the transaction counts as not started, so a retried abort re-sends ONLY the
//     bump;
//   * m15: TxnStarted::Maybe + 19 on EndTxn(abort) = nothing was open: the abort is done;
//   * FD6: 17 / 18 -> FatalError(Fenced); 19 or 45 / 47 on EndTxn, any non-retriable InitProducerId ->
//     FatalError(BrokerError code); 39 on EndTxn -> AbortableError(39); a leg's 19 / 45 / unknown ->
//     AbortableError; SessionRevoked -> FatalError(BrokerError 46);
//   * m5 / m13: a second commit / abort while one is pending joins it.
//
// Re-entrancy: a completion is delivered only AFTER the state transition it reports is complete (its op copied
// out first), so a handler that issues the next op (Invoker: applied in place) sees the new state.  Coordinator
// slots are acquired only in txnStep, never from an apply or an answer.  ZERO allocation: ops, joiners and
// pending flushes live in fixed arrays inside the states / the producer.
#include <rune/client/producer/ProducerImpl.h>

#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/TxnMessages.h>

#include <algorithm>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;

namespace {
constexpr uint16_t kInvalidTxnState = static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr int16_t  kInvalidTxnState16 = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr uint16_t kInvalidRequest  = static_cast<uint16_t>(rpc::ErrorCode::InvalidRequest);

inline std::span<const uint8_t> textBytes(std::string_view t) noexcept {
    return {reinterpret_cast<const uint8_t*>(t.data()), t.size()};
}
/// The close's own abort carries no target (CoordinatorLink.h, TxnOp).
inline bool internalOp(const TxnOp& op) noexcept {
    return op.target.handler.fn == nullptr && op.target.cqRef == kNoCqRef;
}
inline OpKind opKindOf(TxnControl::Kind k) noexcept {
    switch (k) {
    case TxnControl::Kind::Init:        return OpKind::InitTransactions;
    case TxnControl::Kind::SendOffsets: return OpKind::SendOffsets;
    case TxnControl::Kind::Commit:      return OpKind::CommitTransaction;
    case TxnControl::Kind::Abort:       return OpKind::AbortTransaction;
    case TxnControl::Kind::Flush:       return OpKind::Flush;
    case TxnControl::Kind::Begin:       break;
    }
    return OpKind::InitTransactions;   // Begin never completes
}
/// The started-ness that survives both legs: Yes dominates Maybe dominates No.
inline TxnStarted upgrade(TxnStarted cur, TxnStarted s) noexcept {
    return static_cast<uint8_t>(s) > static_cast<uint8_t>(cur) ? s : cur;
}
/// EndTxn may have reached the coordinator (sent, or its re-send pending): a commit there may have landed.
inline bool endMayHaveLanded(const EndPhase& p) noexcept {
    if (std::holds_alternative<EndSent>(p) || std::holds_alternative<ResendEnd>(p)) return true;
    const auto* d = std::get_if<InDoubt>(&p);
    return d != nullptr && d->at == InDoubtAt::AfterEnd;
}
/// EndTxn(abort) was answered OK; only the KIP-360 bump is left (the old `ended`).
inline bool abortEnded(const EndPhase& p) noexcept {
    if (std::holds_alternative<Bumping>(p)) return true;
    const auto* d = std::get_if<InDoubt>(&p);
    return d != nullptr && d->at == InDoubtAt::Bump;
}
/// What an in-doubt timeout of `p` leaves (B-2): the leg that is still unknown.
inline InDoubt inDoubtFor(const EndPhase& p) noexcept {
    if (const auto* d = std::get_if<InDoubt>(&p)) return *d;
    if (std::holds_alternative<Bumping>(p)) return InDoubt{InDoubtAt::Bump};
    if (std::holds_alternative<EndSent>(p) || std::holds_alternative<ResendEnd>(p)) return InDoubt{InDoubtAt::AfterEnd};
    return InDoubt{InDoubtAt::BeforeEnd};
}
/// The retry of an in-doubt end op resumes at the leg that is left: the data wait, the EndTxn re-send, or the bump.
inline EndPhase resumeFrom(const InDoubt& d) noexcept {
    switch (d.at) {
    case InDoubtAt::BeforeEnd: return AwaitData{};
    case InDoubtAt::AfterEnd:  return ResendEnd{};
    case InDoubtAt::Bump:      return Bumping{nullptr};
    }
    return AwaitData{};
}
inline bool join(TxnJoiners& j, const TxnOp& op) noexcept {
    if (j.n >= j.ops.size()) return false;
    j.ops[j.n++] = op;
    return true;
}
}  // namespace

// ---------------------------------------------------------------------------------------------------------------
// Completions
// ---------------------------------------------------------------------------------------------------------------

void ProducerImpl::completeTxn_(const TxnOp& op, OpKind kind, OpStatus st, uint16_t errorCode, std::string_view text,
                                int64_t value, int64_t aux) noexcept {
    if (internalOp(op)) {
        // The close's own abort: its outcome is the close's (closeTxnStep_ reads it), never delivered alone.
        if (auto* e = std::get_if<LifeEndingTxn>(&life_)) e->own = CloseAbortDone{st, errorCode};
        return;
    }
    Completion c;
    c.token     = op.token;
    c.op        = kind;
    c.status    = st;
    c.errorCode = errorCode;
    c.value     = value;
    c.aux       = aux;
    hooks_.sink.deliver(op.target, c, textBytes(text));
}

void ProducerImpl::completeJoined_(const TxnJoiners& j, OpKind kind, OpStatus st, uint16_t errorCode,
                                   std::string_view text) noexcept {
    for (uint8_t i = 0; i < j.n; ++i) completeTxn_(j.ops[i], kind, st, errorCode, text);
}

void ProducerImpl::refuseTxn_(const TxnControl& c, OpStatus st, uint16_t errorCode, std::string_view text) noexcept {
    if (c.kind == TxnControl::Kind::Begin) {
        ++txnRefusedBegins_;   // void beginTransaction(): no completion, no side effect
        return;
    }
    completeTxn_(TxnOp{c.target, c.token, 0}, opKindOf(c.kind), st, errorCode, text);
}

// ---------------------------------------------------------------------------------------------------------------
// Ops (duty thread)
// ---------------------------------------------------------------------------------------------------------------

void ProducerImpl::applyTxnControl(const TxnControl& c, int64_t nowNs) {
    if (!transactional_()) {
        refuseTxn_(c, OpStatus::InvalidArgument, 0, "producer has no transactionalId");
        return;
    }
    if (!lifeOpen()) {
        refuseTxn_(c, OpStatus::Closed, 0, "producer closed");
        return;
    }
    if (const auto* f = std::get_if<FatalError>(&txn_)) {
        refuseTxn_(c, f->status, f->errorCode, "transactional producer in FatalError (terminal)");
        return;
    }
    const TxnOp op{c.target, c.token, nowNs + txnOpTimeoutNs_()};
    switch (c.kind) {
    case TxnControl::Kind::Init:
        if (!std::holds_alternative<Uninitialized>(txn_)) {
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "initTransactions: only once, from Uninitialized");
            return;
        }
        txn_ = Initializing{op, nullptr};   // txnStep sends the InitProducerId
        publishStatus_();
        return;

    case TxnControl::Kind::Begin:
        if (!std::holds_alternative<Ready>(txn_)) {
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, {});   // Kafka: begin only in Ready
            return;
        }
        txn_ = InTransaction{TxnStarted::No};   // not started until a leg is acknowledged (B-1)
        publishStatus_();
        return;

    case TxnControl::Kind::SendOffsets:
        if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
            refuseTxn_(c, ae->status, ae->errorCode, "sendOffsetsToTransaction: the transaction failed - abort it");
            return;
        }
        if (!std::holds_alternative<InTransaction>(txn_)) {
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "sendOffsetsToTransaction: no transaction in progress");
            return;
        }
        // The AddOffsetsToTxn / TxnOffsetCommit legs arrive with Task 7.
        refuseTxn_(c, OpStatus::InvalidArgument, 0, "sendOffsetsToTransaction: not supported by this build yet");
        return;

    case TxnControl::Kind::Flush:
        if (txnFlushN_ >= txnFlushes_.size()) {
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "flush: too many pending flushes");
            return;
        }
        txnFlushes_[txnFlushN_++] = op;   // decided by txnStep (data settled / poison / deadline)
        dutyFlushPending_ = true;
        return;

    case TxnControl::Kind::Commit:
        if (const auto* it = std::get_if<InTransaction>(&txn_)) {
            const TxnStarted started = it->started;
            abandonOpenClaimForClose();   // Part B: a claim open across the end of its transaction never commits
            txn_ = Committing{op, AwaitData{}, started, {}};
            dutyFlushPending_ = true;     // the transaction's records leave now (no linger)
            publishStatus_();
            return;
        }
        if (auto* cm = std::get_if<Committing>(&txn_)) {
            if (const auto* d = std::get_if<InDoubt>(&cm->phase)) {
                cm->op    = op;           // B-2: the in-doubt commit is retried - EndTxn(commit) (re-)sent
                cm->phase = resumeFrom(*d);
                return;
            }
            if (join(cm->joined, op)) return;   // m13: one EndTxn, the same completion
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "commitTransaction: too many concurrent calls");
            return;
        }
        if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
            refuseTxn_(c, ae->status, ae->errorCode, "commitTransaction: the transaction failed - abortTransaction() it");
            return;
        }
        refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "commitTransaction: no transaction to commit");
        return;

    case TxnControl::Kind::Abort:
        if (const auto* it = std::get_if<InTransaction>(&txn_)) {
            const TxnStarted started = it->started;
            abandonOpenClaimForClose();
            txn_ = Aborting{op, AwaitData{}, started, AbortKind::Plain, {}};
            dutyFlushPending_ = true;
            publishStatus_();
            return;
        }
        if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
            const TxnStarted started = ae->started;
            const AbortKind  kind    = ae->kind;
            abandonOpenClaimForClose();
            txn_ = Aborting{op, AwaitData{}, started, kind, {}};
            dutyFlushPending_ = true;
            publishStatus_();
            return;
        }
        if (auto* ab = std::get_if<Aborting>(&txn_)) {
            if (const auto* d = std::get_if<InDoubt>(&ab->phase)) {
                ab->op    = op;           // B-2 / B-4: the in-doubt abort is retried (EndTxn, or only the bump)
                ab->phase = resumeFrom(*d);
                return;
            }
            if (join(ab->joined, op)) return;   // m5
            refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "abortTransaction: too many concurrent calls");
            return;
        }
        // Includes Committing (Kafka: COMMITTING -> ABORTING is invalid).
        refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState,
                   "abortTransaction: no transaction to abort (or it is committing)");
        return;
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The duty pass
// ---------------------------------------------------------------------------------------------------------------

bool ProducerImpl::txnHasLiveOp_() const noexcept {
    if (txnFlushN_ != 0) return true;
    if (std::holds_alternative<Initializing>(txn_)) return true;
    if (const auto* cm = std::get_if<Committing>(&txn_)) return !std::holds_alternative<InDoubt>(cm->phase);
    if (const auto* ab = std::get_if<Aborting>(&txn_)) return !std::holds_alternative<InDoubt>(ab->phase);
    return false;
}

bool ProducerImpl::txnDataSettled_() const noexcept {
    if (!deferred_.empty()) return false;
    for (const auto& up : partitions_) {
        const PartitionState& ps = *up;
        if ((ps.builder && ps.builder->recordCount() > 0) || ps.hasTier2() || !ps.inflight.empty()) return false;
    }
    return true;
}

int ProducerImpl::txnStepCold_(int64_t nowNs) noexcept {
    if (discarding()) return 0;   // closeImmediately detached every op
    retryStatus_();               // a snapshot the ring refused: re-offered (coalesced; only the newest matters)
    int work = 0;
    if (auto* in = std::get_if<Initializing>(&txn_)) {
        if (nowNs >= in->op.deadlineNs) {
            initExpired_(/*releaseSlot=*/true);
            work = 1;
        } else if (in->slot == nullptr && coord_) {
            if (CoordinatorLink::Slot* s = sendInitProducerId_(in->op.deadlineNs)) {
                in->slot = s;
                work = 1;
            }
        }
    } else if (std::holds_alternative<Committing>(txn_) || std::holds_alternative<Aborting>(txn_)) {
        work += endStep_(nowNs);
    }
    if (txnFlushN_ != 0) work += flushStep_(nowNs);
    return work;
}

int ProducerImpl::endStep_(int64_t nowNs) noexcept {
    auto* cm = std::get_if<Committing>(&txn_);
    auto* ab = std::get_if<Aborting>(&txn_);
    TxnOp&     op      = cm != nullptr ? cm->op : ab->op;
    EndPhase&  phase   = cm != nullptr ? cm->phase : ab->phase;
    const TxnStarted started = cm != nullptr ? cm->started : ab->started;
    if (std::holds_alternative<InDoubt>(phase)) return 0;   // waits for the app's retry
    if (nowNs >= op.deadlineNs) {
        endExpired_(/*releaseSlot=*/true);
        return 1;
    }
    if (std::holds_alternative<AwaitData>(phase)) {
        // TX-6: EndTxn only once every record of the transaction is acked.
        if (!txnDataSettled_()) {
            dutyFlushPending_ = true;
            return 0;
        }
        if (started == TxnStarted::No) {   // B-1: the coordinator never saw it - no EndTxn
            endDone_(nowNs);
            return 1;
        }
    }
    if (std::holds_alternative<AwaitData>(phase) || std::holds_alternative<ResendEnd>(phase)) {
        const auto* id = std::get_if<IdAssigned>(&identity_);
        if (!coord_ || id == nullptr) return 0;   // unreachable in practice: the op deadline bounds it
        const int64_t deadline = nowNs + txnOpTimeoutNs_();   // I-4: EndTxn's own budget starts now
        CoordinatorLink::Slot* s = coord_->acquire(ReqKind::EndTxn, deadline);
        if (s == nullptr) return 0;   // no free slot: next pass
        rpc::EndTxn m;
        m.correlationId   = s->corrId;
        m.producerId      = id->producerId;
        m.producerEpoch   = id->epoch;
        m.commit          = cm != nullptr ? uint8_t{1} : uint8_t{0};
        m.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(cfg_.transactionalId.data()),
                                                     cfg_.transactionalId.size());
        s->frameLen = static_cast<uint16_t>(m.encode(s->frame.data()));
        coord_->submit(*s);
        op.deadlineNs = deadline;
        phase         = EndSent{s};
        return 1;
    }
    if (auto* b = std::get_if<Bumping>(&phase); b != nullptr && b->slot == nullptr && coord_) {
        if (CoordinatorLink::Slot* s = sendInitProducerId_(op.deadlineNs)) {
            b->slot = s;
            return 1;
        }
    }
    return 0;
}

int ProducerImpl::flushStep_(int64_t nowNs) noexcept {
    // E3 (Kafka flush()): a flush completes once every record accepted so far is acked, with the transaction's
    // error once it is poisoned, Timeout at its deadline (never a poison).  One decision per scan: a completion
    // handler may push a new flush, so the scan restarts after each delivery.
    int work = 0;
    for (;;) {
        const auto* ae      = std::get_if<AbortableError>(&txn_);
        const bool  settled = txnDataSettled_();
        std::size_t i = 0;
        OpStatus    st = OpStatus::Ok;
        uint16_t    code = 0;
        std::string_view text;
        for (; i < txnFlushN_; ++i) {
            if (ae != nullptr) { st = ae->status; code = ae->errorCode; text = "flush: the transaction failed"; break; }
            if (nowNs >= txnFlushes_[i].deadlineNs) { st = OpStatus::Timeout; break; }
            if (settled) { st = OpStatus::Ok; break; }
        }
        if (i == txnFlushN_) {
            if (txnFlushN_ != 0) dutyFlushPending_ = true;   // keep the records moving (no linger)
            return work;
        }
        const TxnOp op = txnFlushes_[i];
        for (std::size_t k = i + 1; k < txnFlushN_; ++k) txnFlushes_[k - 1] = txnFlushes_[k];
        --txnFlushN_;
        completeTxn_(op, OpKind::Flush, st, code, text);
        ++work;
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------------------------------------------

void ProducerImpl::markStarted_(TxnStarted s) noexcept {
    if (auto* it = std::get_if<InTransaction>(&txn_)) it->started = upgrade(it->started, s);
    else if (auto* cm = std::get_if<Committing>(&txn_)) cm->started = upgrade(cm->started, s);
    // A late leg answer after EndTxn(abort) was answered changes nothing: only the bump is left (B-4).
    else if (auto* ab = std::get_if<Aborting>(&txn_); ab != nullptr && !abortEnded(ab->phase)) ab->started = upgrade(ab->started, s);
    else if (auto* ae = std::get_if<AbortableError>(&txn_)) ae->started = upgrade(ae->started, s);
}

void ProducerImpl::poison_(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept {
    // The transaction can no longer commit: AbortableError (the app must abort).
    if (const auto* it = std::get_if<InTransaction>(&txn_)) {
        txn_ = AbortableError{st, errorCode, it->started, kind};
        publishStatus_();
        return;
    }
    if (auto* cm = std::get_if<Committing>(&txn_)) {
        // EndTxn(commit) went out (or its re-send is pending, or it timed out unanswered): the data settled before
        // it and the commit may have landed - never turned into an abort (B-2; the old `endSent`).
        if (endMayHaveLanded(cm->phase)) return;
        const bool        live   = std::holds_alternative<AwaitData>(cm->phase);
        const TxnOp       op     = cm->op;
        const TxnJoiners  joined = cm->joined;
        txn_ = AbortableError{st, errorCode, cm->started, kind};
        publishStatus_();
        // TX-6b: never a commit marker over this data - the commit fails and the app must abort.
        if (live) {
            constexpr std::string_view kWhy = "commitTransaction: the transaction failed - abortTransaction() it";
            completeTxn_(op, OpKind::CommitTransaction, st, errorCode, kWhy);
            completeJoined_(joined, OpKind::CommitTransaction, st, errorCode, kWhy);
        }
        return;
    }
    // The first error is kept; a sequence gap still requires the epoch bump.
    if (auto* ae = std::get_if<AbortableError>(&txn_); ae != nullptr && kind == AbortKind::BumpEpoch) ae->kind = kind;
    if (auto* ab = std::get_if<Aborting>(&txn_); ab != nullptr && kind == AbortKind::BumpEpoch) ab->kind = kind;
}

void ProducerImpl::onTxnLegAnswer_(ReqKind kind, int16_t errorCode) noexcept {
    const uint16_t code = static_cast<uint16_t>(errorCode);
    switch (CoordinatorLink::classify(kind, errorCode)) {
    case Verdict::Ok:
        markStarted_(TxnStarted::Yes);   // B-1: the transaction exists on the coordinator now
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        return;   // the link re-sends; never handed over
    case Verdict::Fenced:
        enterFatal_(OpStatus::Fenced, code, "producer fenced (fatal)");
        return;
    case Verdict::Fatal:
        enterFatal_(OpStatus::BrokerError, code, "transaction leg failed (fatal)");
        return;
    case Verdict::Abortable:
    case Verdict::Reject:
        poison_(OpStatus::BrokerError, code, AbortKind::Plain);   // never produced: no sequence gap
        return;
    }
}

void ProducerImpl::enterFatal_(OpStatus st, uint16_t errorCode, std::string_view text) noexcept {
    // Already terminal (or closeImmediately detached everything): a late answer changes nothing.
    if (std::holds_alternative<FatalError>(txn_) || discarding()) return;
    // Terminal (Kafka ProducerFencedException): nothing is ever sent again.  The pending ops are copied out
    // first and completed after the state is FatalError (a handler's next op is refused with this status).
    std::array<TxnOp, 1 + kMaxTxnJoiners> ops{};
    std::array<OpKind, 1 + kMaxTxnJoiners> kinds{};
    std::size_t n = 0;
    auto take = [&](const TxnOp& op, OpKind k) { ops[n] = op; kinds[n] = k; ++n; };
    if (const auto* in = std::get_if<Initializing>(&txn_)) {
        take(in->op, OpKind::InitTransactions);
    } else if (const auto* cm = std::get_if<Committing>(&txn_); cm != nullptr && !std::holds_alternative<InDoubt>(cm->phase)) {
        take(cm->op, OpKind::CommitTransaction);
        for (uint8_t i = 0; i < cm->joined.n; ++i) take(cm->joined.ops[i], OpKind::CommitTransaction);
    } else if (const auto* ab = std::get_if<Aborting>(&txn_); ab != nullptr && !std::holds_alternative<InDoubt>(ab->phase)) {
        take(ab->op, OpKind::AbortTransaction);
        for (uint8_t i = 0; i < ab->joined.n; ++i) take(ab->joined.ops[i], OpKind::AbortTransaction);
    }
    const std::array<TxnOp, kMaxTxnFlushes> flushes = txnFlushes_;
    const std::size_t nFlushes = txnFlushN_;
    txnFlushN_ = 0;
    if (coord_) coord_->releaseAll();   // never offered, never re-sent (a session change re-sends nothing)
    txn_      = FatalError{st, errorCode};
    identity_ = IdFailed{st, errorCode};   // the identity machine fails every pending and future record
    publishStatus_();
    failAllPending(st, errorCode);
    for (std::size_t i = 0; i < n; ++i) completeTxn_(ops[i], kinds[i], st, errorCode, text);
    for (std::size_t i = 0; i < nFlushes; ++i) completeTxn_(flushes[i], OpKind::Flush, st, errorCode, text);
}

void ProducerImpl::endDone_(int64_t nowNs) noexcept {
    if (auto* ab = std::get_if<Aborting>(&txn_); ab != nullptr && ab->kind == AbortKind::BumpEpoch) {
        // B-3 (KIP-360): the aborted transaction left a sequence gap - bump the epoch (InitProducerId for this
        // transactional id) and restart every sequence before Ready; the abort completes after the bump.  B-4:
        // from here nothing is open on the broker: Bumping / InDoubt{Bump} - a retried abort re-sends only the bump.
        ab->op.deadlineNs = nowNs + txnOpTimeoutNs_();
        ab->phase         = Bumping{nullptr};   // txnStep sends it
        return;
    }
    finishTxn_();
}

void ProducerImpl::finishTxn_() noexcept {
    const bool       commit = std::holds_alternative<Committing>(txn_);
    TxnOp            op{};
    TxnJoiners       joined{};
    if (const auto* cm = std::get_if<Committing>(&txn_)) { op = cm->op; joined = cm->joined; }
    else if (const auto* ab = std::get_if<Aborting>(&txn_)) { op = ab->op; joined = ab->joined; }
    else return;
    txn_ = Ready{};   // EndTxn's final answer: only now is the transaction closed locally
    publishStatus_();
    const OpKind kind = commit ? OpKind::CommitTransaction : OpKind::AbortTransaction;
    completeTxn_(op, kind, OpStatus::Ok, 0, {});
    completeJoined_(joined, kind, OpStatus::Ok, 0, {});
}

void ProducerImpl::endExpired_(bool releaseSlot) noexcept {
    auto* cm = std::get_if<Committing>(&txn_);
    auto* ab = std::get_if<Aborting>(&txn_);
    if (cm == nullptr && ab == nullptr) return;
    EndPhase& phase = cm != nullptr ? cm->phase : ab->phase;
    if (std::holds_alternative<InDoubt>(phase)) return;
    CoordinatorLink::Slot* slot = nullptr;
    if (const auto* e = std::get_if<EndSent>(&phase)) slot = e->slot;
    if (const auto* b = std::get_if<Bumping>(&phase)) {
        slot = b->slot;
        if (std::holds_alternative<IdRequested>(identity_)) identity_ = IdUnassigned{};   // the bump is re-sent on retry
    }
    if (releaseSlot && slot != nullptr && coord_) coord_->release(*slot);
    const TxnOp      op     = cm != nullptr ? cm->op : ab->op;
    TxnJoiners&      jref   = cm != nullptr ? cm->joined : ab->joined;
    const TxnJoiners joined = jref;
    jref  = TxnJoiners{};
    phase = inDoubtFor(phase);   // B-2: never Ready over a transaction that may be open, never an abort over a commit
    publishStatus_();
    const OpKind kind = cm != nullptr ? OpKind::CommitTransaction : OpKind::AbortTransaction;
    constexpr std::string_view kWhy = "the transaction's outcome is in doubt (timed out): retry the same call";
    completeTxn_(op, kind, OpStatus::Timeout, 0, kWhy);
    completeJoined_(joined, kind, OpStatus::Timeout, 0, kWhy);
}

void ProducerImpl::initExpired_(bool releaseSlot) noexcept {
    const auto* in = std::get_if<Initializing>(&txn_);
    if (in == nullptr) return;
    const TxnOp op = in->op;
    if (releaseSlot && in->slot != nullptr && coord_) coord_->release(*in->slot);
    if (std::holds_alternative<IdRequested>(identity_)) identity_ = IdUnassigned{};
    txn_ = Uninitialized{};   // init may be retried
    publishStatus_();
    completeTxn_(op, OpKind::InitTransactions, OpStatus::Timeout, 0, "initTransactions: timed out");
}

void ProducerImpl::onTxnSlotExpired_(CoordinatorLink::Slot& s) noexcept {
    if (const auto* in = std::get_if<Initializing>(&txn_); in != nullptr && in->slot == &s) {
        initExpired_(/*releaseSlot=*/false);
        return;
    }
    const EndPhase* phase = nullptr;
    if (const auto* cm = std::get_if<Committing>(&txn_)) phase = &cm->phase;
    if (const auto* ab = std::get_if<Aborting>(&txn_)) phase = &ab->phase;
    if (phase == nullptr) return;
    const auto* e = std::get_if<EndSent>(phase);
    const auto* b = std::get_if<Bumping>(phase);
    if ((e != nullptr && e->slot == &s) || (b != nullptr && b->slot == &s)) endExpired_(/*releaseSlot=*/false);
}

void ProducerImpl::onTxnInitAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s, int64_t) noexcept {
    // An ErrorResponse carrying code 0 is a protocol error: InvalidRequest (39), never a code-less success.
    const int16_t code = (a.anyKind && a.errorCode == 0) ? static_cast<int16_t>(kInvalidRequest) : a.errorCode;
    int64_t pid = -1;
    int16_t epoch = 0;
    Verdict v = CoordinatorLink::classify(ReqKind::InitProducerId, code);
    if (v == Verdict::Ok) {
        const auto m = rpc::InitProducerIdResponse::decode(a.frame.data(), a.frame.size());
        if (m && m->producerId >= 0) {
            pid   = m->producerId;
            epoch = m->producerEpoch;
        } else {
            v = Verdict::Fatal;   // undecodable / no id: never a guessed identity
        }
    }
    const uint16_t ucode = code != 0 ? static_cast<uint16_t>(code) : kInvalidRequest;
    if (const auto* in = std::get_if<Initializing>(&txn_); in != nullptr && in->slot == &s) {
        if (v == Verdict::Ok) {
            const TxnOp op = in->op;
            assignIdentity_(pid, epoch);
            txn_ = Ready{};
            publishStatus_();   // published BEFORE the completion: the app reads it after
            completeTxn_(op, OpKind::InitTransactions, OpStatus::Ok, 0, {}, pid, epoch);
        } else if (v == Verdict::Fenced) {
            enterFatal_(OpStatus::Fenced, ucode, "initTransactions: producer fenced (fatal)");
        } else {
            enterFatal_(OpStatus::BrokerError, ucode, "initTransactions: InitProducerId failed (fatal)");   // m3
        }
        return;
    }
    const auto* ab = std::get_if<Aborting>(&txn_);
    const auto* b  = ab != nullptr ? std::get_if<Bumping>(&ab->phase) : nullptr;
    if (b == nullptr || b->slot != &s) return;   // stale
    if (v == Verdict::Ok) {
        // KIP-360: every partition restarts its sequences under the bumped epoch (assignIdentity_ adopts it;
        // a partition with stored batches - none after a settled abort - is re-stamped there).
        for (auto& up : partitions_) {
            PartitionState& ps = *up;
            ps.producerId = -1;
            if (ps.inflight.empty()) {
                ps.nextBaseSequence = 0;
                ps.lastAckedSeq     = -1;
            }
        }
        assignIdentity_(pid, epoch);
        finishTxn_();   // the abort is complete under the bumped epoch
    } else if (v == Verdict::Fenced) {
        enterFatal_(OpStatus::Fenced, ucode, "abortTransaction: producer fenced on the epoch bump (fatal)");
    } else {
        enterFatal_(OpStatus::BrokerError, ucode, "abortTransaction: the epoch bump failed (fatal)");
    }
}

void ProducerImpl::onEndTxnAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s, int64_t nowNs) noexcept {
    auto* cm = std::get_if<Committing>(&txn_);
    auto* ab = std::get_if<Aborting>(&txn_);
    if (cm == nullptr && ab == nullptr) return;
    const EndPhase& phase = cm != nullptr ? cm->phase : ab->phase;
    const auto*     sent  = std::get_if<EndSent>(&phase);
    if (sent == nullptr || sent->slot != &s) return;   // stale: a timed-out attempt's late answer (m12)
    const int16_t  code  = (a.anyKind && a.errorCode == 0) ? static_cast<int16_t>(kInvalidRequest) : a.errorCode;
    const uint16_t ucode = static_cast<uint16_t>(code);
    switch (CoordinatorLink::classify(ReqKind::EndTxn, code)) {
    case Verdict::Ok:
        endDone_(nowNs);   // Ready (or the KIP-360 epoch bump first)
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        return;            // the link re-sends; never handed over
    case Verdict::Fenced:
        enterFatal_(OpStatus::Fenced, ucode, "EndTxn: producer fenced (fatal)");
        return;
    case Verdict::Fatal:
        if (ab != nullptr && code == kInvalidTxnState16 && ab->started == TxnStarted::Maybe) {
            // m15: the transaction was only POSSIBLY started: 19 = nothing open on the broker - the abort is done.
            endDone_(nowNs);
            return;
        }
        enterFatal_(OpStatus::BrokerError, ucode, "EndTxn failed (fatal)");   // 19 / 45 / 47 on EndTxn
        return;
    case Verdict::Abortable:
    case Verdict::Reject: {
        // 39 InvalidRequest (FD6): never re-sent; the transaction can only be aborted (a decode failure is not a
        // fence).  The end op completes with it.
        const bool       commit  = cm != nullptr;
        const TxnOp      op      = commit ? cm->op : ab->op;
        const TxnJoiners joined  = commit ? cm->joined : ab->joined;
        const TxnStarted started = commit ? cm->started : ab->started;
        const AbortKind  kind    = commit ? AbortKind::Plain : ab->kind;
        txn_ = AbortableError{OpStatus::BrokerError, ucode, started, kind};
        publishStatus_();
        const OpKind k = commit ? OpKind::CommitTransaction : OpKind::AbortTransaction;
        completeTxn_(op, k, OpStatus::BrokerError, ucode, "EndTxn: InvalidRequest");
        completeJoined_(joined, k, OpStatus::BrokerError, ucode, "EndTxn: InvalidRequest");
        return;
    }
    }
}

void ProducerImpl::onSessionRevoked(uint16_t errorCode) noexcept {
    failInFlightBroker(errorCode);
    if (!transactional_() || std::holds_alternative<FatalError>(txn_) || discarding()) return;
    enterFatal_(OpStatus::BrokerError, errorCode, "the session was revoked (SessionRevoked)");
}

void ProducerImpl::txnDiscard_(OpStatus st, uint16_t errorCode) noexcept {
    std::array<TxnOp, 1 + kMaxTxnJoiners> ops{};
    std::array<OpKind, 1 + kMaxTxnJoiners> kinds{};
    std::size_t n = 0;
    auto take = [&](const TxnOp& op, OpKind k) { ops[n] = op; kinds[n] = k; ++n; };
    if (const auto* in = std::get_if<Initializing>(&txn_)) {
        take(in->op, OpKind::InitTransactions);
        txn_ = Uninitialized{};
    } else if (auto* cm = std::get_if<Committing>(&txn_)) {
        if (!std::holds_alternative<InDoubt>(cm->phase)) {
            take(cm->op, OpKind::CommitTransaction);
            for (uint8_t i = 0; i < cm->joined.n; ++i) take(cm->joined.ops[i], OpKind::CommitTransaction);
        }
        cm->phase  = inDoubtFor(cm->phase);
        cm->joined = TxnJoiners{};
    } else if (auto* ab = std::get_if<Aborting>(&txn_)) {
        if (!std::holds_alternative<InDoubt>(ab->phase)) {
            take(ab->op, OpKind::AbortTransaction);
            for (uint8_t i = 0; i < ab->joined.n; ++i) take(ab->joined.ops[i], OpKind::AbortTransaction);
        }
        ab->phase  = inDoubtFor(ab->phase);
        ab->joined = TxnJoiners{};
    }
    const std::array<TxnOp, kMaxTxnFlushes> flushes = txnFlushes_;
    const std::size_t nFlushes = txnFlushN_;
    txnFlushN_ = 0;
    if (coord_) coord_->releaseAll();
    if (std::holds_alternative<IdRequested>(identity_)) identity_ = IdUnassigned{};
    publishStatus_();
    for (std::size_t i = 0; i < n; ++i) completeTxn_(ops[i], kinds[i], st, errorCode, "producer closed");
    for (std::size_t i = 0; i < nFlushes; ++i) completeTxn_(flushes[i], OpKind::Flush, st, errorCode, "producer closed");
}

// ---------------------------------------------------------------------------------------------------------------
// close(): end the open transaction first (old TransactionalProducer::close semantics)
// ---------------------------------------------------------------------------------------------------------------

bool ProducerImpl::txnOpenForClose_() const noexcept {
    if (!transactional_()) return false;
    switch (txnStateDuty()) {
    case TxnState::Initializing:
    case TxnState::InTransaction:
    case TxnState::CommittingTransaction:
    case TxnState::AbortingTransaction:
    case TxnState::AbortableError:
        return true;
    case TxnState::Uninitialized:
    case TxnState::Ready:
    case TxnState::FatalError:   // fenced: the coordinator aborts it
        return false;
    }
    return false;
}

void ProducerImpl::startCloseAbort_(int64_t nowNs) noexcept {
    auto& e = std::get<LifeEndingTxn>(life_);
    const TxnOp own{detail::OpTarget{}, 0, nowNs + txnOpTimeoutNs_()};   // empty target: the close's own abort
    if (const auto* it = std::get_if<InTransaction>(&txn_)) {
        const TxnStarted started = it->started;
        txn_ = Aborting{own, AwaitData{}, started, AbortKind::Plain, {}};
    } else if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
        const TxnStarted started = ae->started;
        const AbortKind  kind    = ae->kind;
        txn_ = Aborting{own, AwaitData{}, started, kind, {}};
    } else if (auto* ab = std::get_if<Aborting>(&txn_); ab != nullptr && std::holds_alternative<InDoubt>(ab->phase)) {
        ab->op    = own;   // in doubt (an abort timed out): retried once as the close's own, from the leg left
        ab->phase = resumeFrom(std::get<InDoubt>(ab->phase));
    } else {
        return;
    }
    abandonOpenClaimForClose();
    e.own = CloseAbortRunning{};
    publishStatus_();
}

void ProducerImpl::finishTxnClose_(OpStatus st, uint16_t errorCode) noexcept {
    const LifeEndingTxn e = std::get<LifeEndingTxn>(life_);
    life_ = LifeClosed{};   // the intake is closed; records still in flight complete normally
    Completion c;
    c.token     = e.token;
    c.op        = OpKind::ProducerClose;
    c.status    = st;
    c.errorCode = errorCode;
    hooks_.sink.deliver(e.target, c);
}

int ProducerImpl::closeTxnStep_(int64_t nowNs) noexcept {
    auto&          e    = std::get<LifeEndingTxn>(life_);
    const bool     past = nowNs >= e.deadlineNs;
    const bool     none = std::holds_alternative<CloseAbortNone>(e.own);
    const auto*    done = std::get_if<CloseAbortDone>(&e.own);
    const OpStatus ownSt   = done != nullptr ? done->status : OpStatus::Timeout;
    const uint16_t ownCode = done != nullptr ? done->errorCode : uint16_t{0};
    switch (txnStateDuty()) {
    case TxnState::InTransaction:
    case TxnState::AbortableError:
        if (none) {
            startCloseAbort_(nowNs);   // EndTxn(abort) (or a local end: never started)
            return 1;
        }
        finishTxnClose_(ownSt, ownCode);   // our abort completed yet the transaction is still open: it failed
        return 1;
    case TxnState::AbortingTransaction: {
        const auto& ab = std::get<Aborting>(txn_);
        if (!std::holds_alternative<InDoubt>(ab.phase)) {   // an abort is running (ours or the app's): wait
            if (!past) return 0;
            finishTxnClose_(OpStatus::Timeout, 0);
            return 1;
        }
        if (none) {
            startCloseAbort_(nowNs);
            return 1;
        }
        finishTxnClose_(ownSt, ownCode);
        return 1;
    }
    case TxnState::CommittingTransaction:
        // A commit is running: wait for its final answer (never aborted - COMMITTING -> ABORTING is invalid).
        // In doubt: nothing a close may do - Timeout.
        if (!std::holds_alternative<InDoubt>(std::get<Committing>(txn_).phase) && !past) return 0;
        finishTxnClose_(OpStatus::Timeout, 0);
        return 1;
    case TxnState::Initializing:
        if (!past) return 0;
        finishTxnClose_(OpStatus::Timeout, 0);
        return 1;
    case TxnState::Uninitialized:
    case TxnState::Ready:
    case TxnState::FatalError:
        // Nothing open on the broker (FatalError: the coordinator aborts it).  A failed own abort is the close's
        // outcome; otherwise the producer drains as any close.
        if (done != nullptr && done->status != OpStatus::Ok) {
            finishTxnClose_(done->status, done->errorCode);
            return 1;
        }
        {
            const detail::OpTarget target = e.target;
            const Token            token  = e.token;
            return beginDrain_(target, token, nowNs) + 1;
        }
    }
    return 0;
}

}  // namespace rune::client::producer
