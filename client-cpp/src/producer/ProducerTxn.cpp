// src/producer/ProducerTxn.cpp - Phase 2b F Tasks 5 / 6: the transaction state machine of ProducerImpl (duty side)
// and the transaction's data (AddPartitionsToTxn gating per partition, the duty re-check, poisoning).
//
// See ProducerImpl.h ("the transaction machine") and CoordinatorLink.h (the states).  Rules ported from the old
// the removed separate transactional impl (Kafka TransactionManager):
//   * gating: begin only in Ready; commit / sendOffsets only InTransaction; abort in InTransaction /
//     AbortableError; init only Uninitialized; FatalError: every op completes with its status; misuse completes
//     BrokerError(19 InvalidTxnState);
//   * B-1: a transaction the coordinator never saw (TxnStarted::No) ends locally - no EndTxn;
//   * B-2: a commit / abort that times out is InDoubt; the SAME call re-sends EndTxn (abort refused while
//     committing); the local state resets only on EndTxn's final answer;
//   * B-3 / B-4 (KIP-360): a sequence gap makes the abort bump the epoch (InitProducerId after EndTxn(abort));
//     once EndTxn(abort) is answered the end phase is Bumping (then InDoubt{Bump} if the bump times out), so a
//     retried abort re-sends ONLY the bump;
//   * m15: TxnStarted::Maybe + 19 on EndTxn(abort) = nothing was open: the abort is done;
//   * FD6: 17 / 18 -> FatalError(Fenced); 19 or 45 / 47 on EndTxn, any non-retriable InitProducerId ->
//     FatalError(BrokerError code); 39 on the FIRST EndTxn -> AbortableError(39); a leg's 19 / 45 / unknown ->
//     AbortableError; SessionRevoked -> FatalError(BrokerError 46);
//   * Task 6: once an EndTxn(commit) may have landed (it was re-sent after an in-doubt timeout), only a definite
//     answer ends it - a non-retriable one (incl. 39, and a code-0 ErrorResponse read as 39) is FatalError with
//     that code, never AbortableError (the app could otherwise abort over a commit that landed);
//   * Task 6 data: a partition's batches are sent only once its AddPartitionsToTxn is OK (PartitionState::txn);
//     every record failure poisons the transaction (AbortableError: a commit never sends EndTxn(commit) over lost
//     data); a record reaching the duty side while the machine is not InTransaction is never produced (FD2); an
//     abort discards every record not yet sent (Kafka abortUndrainedBatches) - never produced;
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
#include <cassert>
#include <cstdio>
#include <cstring>
#include <optional>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;

namespace {
constexpr uint16_t kInvalidTxnState = static_cast<uint16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr int16_t  kInvalidTxnState16 = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr uint16_t kInvalidRequest  = static_cast<uint16_t>(rpc::ErrorCode::InvalidRequest);
constexpr uint16_t kInvalidProducerEpoch = static_cast<uint16_t>(rpc::ErrorCode::InvalidProducerEpoch);
constexpr uint16_t kProducerFenced       = static_cast<uint16_t>(rpc::ErrorCode::ProducerFenced);

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
// Phase 2b F Task 8: the handle side - kMsgTxnControl on the producer ring (FD1), the gate (FD2)
// ---------------------------------------------------------------------------------------------------------------

static_assert(sizeof(OffsetEntry) == 16 && alignof(OffsetEntry) <= 8 && std::is_trivially_copyable_v<OffsetEntry>,
              "kMsgTxnControl carries OffsetEntry as 16 raw bytes (topicId i32, partition i32, offset i64)");

namespace {
inline void putU32(uint8_t* p, uint32_t v) noexcept { std::memcpy(p, &v, 4); }
inline void putI64(uint8_t* p, int64_t v) noexcept { std::memcpy(p, &v, 8); }
inline uint32_t getU32(const uint8_t* p) noexcept { uint32_t v; std::memcpy(&v, p, 4); return v; }
inline int64_t  getI64(const uint8_t* p) noexcept { int64_t v; std::memcpy(&v, p, 8); return v; }
inline std::size_t align8(std::size_t n) noexcept { return (n + 7u) & ~std::size_t{7}; }
}  // namespace

OpResult ProducerImpl::writeTxnControl_(const TxnControl& c, bool overCap) noexcept {
    alignas(8) uint8_t buf[kTxnCtlMaxBytes];   // stack only: a control op allocates nothing
    std::memset(buf, 0, kTxnCtlHeaderBytes);
    buf[0] = static_cast<uint8_t>(c.kind);
    putU32(buf + 4, static_cast<uint32_t>(c.groupId.size()));
    putI64(buf + 8, static_cast<int64_t>(c.token));
    detail::HubCommand words;
    detail::encodeTarget(c.target, words);
    putI64(buf + 16, words.b);
    putI64(buf + 24, words.c);
    putU32(buf + 32, static_cast<uint32_t>(c.offsets.size()));
    putU32(buf + 36, static_cast<uint32_t>(c.generationId));   // #125
    putU32(buf + 40, static_cast<uint32_t>(c.memberId.size()));
    std::size_t len = kTxnCtlHeaderBytes;
    if (c.kind == TxnControl::Kind::SendOffsets && !overCap) {
        std::memcpy(buf + len, c.groupId.data(), c.groupId.size());
        const std::size_t mat = align8(len + c.groupId.size());
        std::memset(buf + len + c.groupId.size(), 0, mat - (len + c.groupId.size()));
        if (!c.memberId.empty()) std::memcpy(buf + mat, c.memberId.data(), c.memberId.size());
        const std::size_t at = align8(mat + c.memberId.size());
        std::memset(buf + mat + c.memberId.size(), 0, at - (mat + c.memberId.size()));
        if (!c.offsets.empty()) std::memcpy(buf + at, c.offsets.data(), c.offsets.size() * sizeof(OffsetEntry));
        len = at + c.offsets.size() * sizeof(OffsetEntry);
    }
    if (len > static_cast<std::size_t>(ring_.maxMsgLength())) return OpResult::InvalidArgument;   // a tiny ring
    if (writeToRing(kMsgTxnControl, buf, len)) return OpResult::Accepted;
    return closed_() ? OpResult::Closed : OpResult::BackPressured;
}

OpResult ProducerImpl::issueTxnControl(const TxnControl& c) noexcept {
    if (!transactional_()) return OpResult::InvalidArgument;
    if (clientClosed()) return OpResult::Closed;
    const bool ends = c.kind == TxnControl::Kind::Commit || c.kind == TxnControl::Kind::Abort;
    if (mode_ == IngressMode::Direct) {
        if (std::this_thread::get_id() != dutyThreadId_) return OpResult::WrongThread;
        if (ends) txnGate_ = TxnGate::Closed;   // FD2: before the op is applied
        TxnControl d = c;
        if (hooks_.pinTarget != nullptr && c.kind != TxnControl::Kind::Begin) hooks_.pinTarget(hooks_.ctx, d.target);
        {
            const DutyScope duty(*this);
            applyTxnControl(d, clock_());
        }
        if (c.kind == TxnControl::Kind::Begin) txnGate_ = TxnGate::Open;
        return OpResult::Accepted;
    }
    const bool overCap = c.kind == TxnControl::Kind::SendOffsets &&
                         (c.offsets.size() > kMaxTxnOffsets || c.groupId.empty() ||
                          c.groupId.size() > kMaxTxnGroupIdBytes || c.memberId.size() > kMaxTxnMemberIdBytes);
    const TxnGate prev = txnGate_;
    if (ends) txnGate_ = TxnGate::Closed;   // FD2: BEFORE the write, so a produce that follows on this thread is refused
    const OpResult r = writeTxnControl_(c, overCap);
    if (r != OpResult::Accepted) {
        txnGate_ = prev;                    // nothing was written: the transaction is still as it was
        return r;
    }
    if (c.kind == TxnControl::Kind::Begin) txnGate_ = TxnGate::Open;   // after the write succeeded
    return r;
}

bool ProducerImpl::decodeTxnControl_(const uint8_t* p, std::size_t len, TxnControl& out) noexcept {
    if (len < kTxnCtlHeaderBytes || p[0] > static_cast<uint8_t>(TxnControl::Kind::Flush)) return false;
    out.kind  = static_cast<TxnControl::Kind>(p[0]);
    out.token = static_cast<Token>(getI64(p + 8));
    out.target = detail::decodeTargetWords(getI64(p + 16), getI64(p + 24));
    out.offsets = {};
    out.groupId = {};
    out.generationId = -1;
    out.memberId = {};
    // A CQ target of a ring op is accounted as a producer op (the issuer counted it in producerIssued_): marked,
    // not pinned.  (Direct mode pins in issueTxnControl; the pin hook is not used here.)
    out.target.producerOp = out.kind != TxnControl::Kind::Begin && out.target.handler.fn == nullptr &&
                            out.target.cqRef != kNoCqRef;
    if (len > kTxnCtlMaxBytes) {
        return true;   // over-cap: offsets and group id stay empty - applyTxnControl refuses InvalidArgument
    }
    if (out.kind != TxnControl::Kind::SendOffsets) return true;
    const uint32_t glen   = getU32(p + 4);
    const uint32_t ocount = getU32(p + 32);
    const uint32_t mlen   = getU32(p + 40);
    if (ocount > kMaxTxnOffsets || glen == 0 || glen > kMaxTxnGroupIdBytes || mlen > kMaxTxnMemberIdBytes) {
        return true;   // over-cap: offsets and group id stay empty - applyTxnControl refuses InvalidArgument
    }
    const std::size_t mat = align8(kTxnCtlHeaderBytes + glen);
    const std::size_t at  = align8(mat + mlen);
    if (len < at + static_cast<std::size_t>(ocount) * sizeof(OffsetEntry) || (reinterpret_cast<uintptr_t>(p) & 7u) != 0) {
        return true;   // truncated (cannot happen: the issuing side wrote it whole): refused like over-cap
    }
    out.groupId = std::string_view(reinterpret_cast<const char*>(p + kTxnCtlHeaderBytes), glen);
    out.generationId = static_cast<int32_t>(getU32(p + 36));
    out.memberId = std::string_view(reinterpret_cast<const char*>(p + mat), mlen);
    out.offsets = std::span<const OffsetEntry>(reinterpret_cast<const OffsetEntry*>(p + at), ocount);
    return true;
}

// ---------------------------------------------------------------------------------------------------------------
// Ops (duty thread)
// ---------------------------------------------------------------------------------------------------------------

void ProducerImpl::applyTxnControl(const TxnControl& c, int64_t nowNs) {
    if (!transactional_()) {
        refuseTxn_(c, OpStatus::InvalidArgument, 0, "producer has no transactionalId");
        return;
    }
    // FatalError first: a fenced producer fails its intake too, and the app must still see WHY (Fenced), not Closed.
    if (const auto* f = std::get_if<FatalError>(&txn_)) {
        refuseTxn_(c, f->status, f->errorCode, "transactional producer in FatalError (terminal)");
        return;
    }
    if (!lifeOpen()) {
        refuseTxn_(c, OpStatus::Closed, 0, "producer closed");
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
        applySendOffsets_(c, op);
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
            txn_ = Aborting{op, AwaitData{}, started, AbortKind::Plain, {}, TxnOutcome{}};
            dutyFlushPending_ = true;
            publishStatus_();
            return;
        }
        if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
            const TxnStarted started = ae->started;
            const AbortKind  kind    = ae->kind;
            const TxnOutcome failed{ae->status, ae->errorCode};   // its records complete with the poison
            abandonOpenClaimForClose();
            txn_ = Aborting{op, AwaitData{}, started, kind, {}, failed};
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
    if (txnFlushN_ != 0 || offsetsOp_.index() != 0) return true;
    if (std::holds_alternative<Initializing>(txn_)) return true;
    if (const auto* cm = std::get_if<Committing>(&txn_)) return !std::holds_alternative<InDoubt>(cm->phase);
    if (const auto* ab = std::get_if<Aborting>(&txn_)) return !std::holds_alternative<InDoubt>(ab->phase);
    return false;
}

bool ProducerImpl::txnDataSettled_() const noexcept {
    if (!deferred_.empty() || !txnAddsSettled_() || offsetsOp_.index() != 0) return false;
    for (const auto& up : partitions_) {
        const PartitionState& ps = *up;
        if ((ps.builder && ps.builder->recordCount() > 0) || ps.hasTier2() || !ps.inflight.empty()) return false;
    }
    return true;
}

int ProducerImpl::txnStepCold_(int64_t nowNs) noexcept {
    if (discarding()) return 0;   // closeImmediately detached every op
    if (fatalPending_) [[unlikely]] {
        const PendingFatal f = *fatalPending_;
        fatalPending_.reset();
        if (!std::holds_alternative<FatalError>(txn_)) enterFatal_(f.status, f.errorCode, f.why);
        return 1;
    }
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
    if (!txnAddQueue_.empty()) work += addStep_(nowNs);
    if (offsetsOp_.index() != 0) work += offsetsStep_(nowNs);
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
        if (ab != nullptr) {
            // Task 6 (Kafka abortUndrainedBatches): an abort never produces what is not sent yet - every record of
            // the transaction still unsent completes with the abort's outcome.  Completions may run user code: the
            // state is re-read on the next pass.
            const TxnOutcome o = ab->discard;
            if (txnDiscardUnsent_(o)) return 1;
        }
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
        phase         = EndSent{s, std::holds_alternative<ResendEnd>(phase) ? EndTry::Resend : EndTry::First};
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
    // Task 7: a live sendOffsets op is taken out first (its slots are released); it completes with the rest below.
    TxnOp offsetsOp{};
    bool  offsetsLive = false;
    if (const auto* oa = std::get_if<OffsetsAdding>(&offsetsOp_)) { offsetsOp = oa->op; offsetsLive = true; }
    else if (const auto* oc = std::get_if<OffsetsCommitting>(&offsetsOp_)) { offsetsOp = oc->op; offsetsLive = true; }
    offsetsOp_ = OffsetsIdle{};
    if (coord_) coord_->releaseAll();   // never offered, never re-sent (a session change re-sends nothing)
    txnResetMembership_();              // the AddPartitions slots are gone with the rest
    txn_      = FatalError{st, errorCode};
    identity_ = IdFailed{st, errorCode};   // the identity machine fails every pending and future record
    publishStatus_();
    failAllPending(st, errorCode);
    for (std::size_t i = 0; i < n; ++i) completeTxn_(ops[i], kinds[i], st, errorCode, text);
    for (std::size_t i = 0; i < nFlushes; ++i) completeTxn_(flushes[i], OpKind::Flush, st, errorCode, text);
    if (offsetsLive) completeTxn_(offsetsOp, OpKind::SendOffsets, st, errorCode, text);
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
    txnResetMembership_();   // Task 6: every partition of the transaction is NotInTxn again
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
    if (s.kind == ReqKind::AddPartitions) {
        onAddExpired_(s);
        return;
    }
    if (s.kind == ReqKind::AddOffsets || s.kind == ReqKind::TxnOffsetCommit) {
        offsetsExpired_(&s);
        return;
    }
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

void ProducerImpl::noteEndReoffer_() noexcept {
    // Task 6 fix round 1 (I1): the link re-offered EndTxn on its own (a session change, a coordinator move, a backoff
    // retry) - an earlier offer may have landed.  Recorded in the end phase: EndSent{.., Resend}.
    auto* cm = std::get_if<Committing>(&txn_);
    if (cm == nullptr) return;
    if (auto* e = std::get_if<EndSent>(&cm->phase); e != nullptr && e->slot != nullptr && e->slot->offers > 1) {
        e->attempt = EndTry::Resend;
    }
}

void ProducerImpl::onEndTxnAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s, int64_t nowNs) noexcept {
    noteEndReoffer_();   // an offer after the first one: its answer is about a request that may already have landed
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
        if (cm != nullptr && sent->attempt == EndTry::Resend) {
            // Task 6: an earlier EndTxn(commit) offer may have landed (the app's retry, or the link's own re-offer) -
            // only a definite answer ends the commit.  A non-retriable answer to a re-send cannot say it did not:
            // FatalError with that code, never an abort.
            enterFatal_(OpStatus::BrokerError, ucode,
                        "EndTxn: the re-sent commit was refused and the first one may have landed (fatal)");
            return;
        }
        // 39 InvalidRequest (FD6) on the first EndTxn: never re-sent; the transaction can only be aborted (a decode
        // failure is not a fence).  The end op completes with it.
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

void ProducerImpl::onCoordinatorRejected(uint16_t errorCode) noexcept {
    // #291: only a producer with a coordinator request live waits on that node; an idle one meets the same Rejected
    // session on its next request.  A login / grant is an admin's act: never retried (classify: 44..47 fatal).
    // Transactional: FatalError.  Plain: its InitProducerId can never be answered, so the identity fails (every
    // held record completes).  Deferred to the duty pass: this runs from hub-command entry, which a completion
    // handler can reach while a batch is being failed.
    if (!coord_ || coord_->inUse() == 0 || fatalPending_ || discarding()) return;
    if (transactional_() && std::holds_alternative<FatalError>(txn_)) return;
    if (!transactional_() && !std::holds_alternative<IdRequested>(identity_)) return;
    fatalPending_ = PendingFatal{OpStatus::BrokerError, errorCode,
                                 "the transaction coordinator node refused this client's session"};
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
    // Task 7: closeImmediately detaches a live sendOffsets op too (completed below, after the slots are released).
    if (coord_) coord_->releaseAll();
    txnResetMembership_();
    if (std::holds_alternative<IdRequested>(identity_)) identity_ = IdUnassigned{};
    publishStatus_();
    for (std::size_t i = 0; i < n; ++i) completeTxn_(ops[i], kinds[i], st, errorCode, "producer closed");
    for (std::size_t i = 0; i < nFlushes; ++i) completeTxn_(flushes[i], OpKind::Flush, st, errorCode, "producer closed");
    offsetsEnd_(st, errorCode, "producer closed", OffsetsEnd::Cancelled);
}

// ---------------------------------------------------------------------------------------------------------------
// Task 6: the transaction's data - AddPartitionsToTxn per partition, the duty re-check, poisoning
// ---------------------------------------------------------------------------------------------------------------

bool ProducerImpl::txnFlowing_() const noexcept {
    if (std::holds_alternative<InTransaction>(txn_)) return true;
    const auto* cm = std::get_if<Committing>(&txn_);
    return cm != nullptr && std::holds_alternative<AwaitData>(cm->phase);
}

TxnOutcome ProducerImpl::txnRefusal_() const noexcept {
    if (const auto* ae = std::get_if<AbortableError>(&txn_)) return TxnOutcome{ae->status, ae->errorCode};
    if (const auto* f = std::get_if<FatalError>(&txn_)) return TxnOutcome{f->status, f->errorCode};
    return TxnOutcome{OpStatus::BrokerError, kInvalidTxnState};   // no transaction to produce into
}

void ProducerImpl::txnLost_(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept {
    // #367: the partition leader fenced us (Kafka: INVALID_PRODUCER_EPOCH / PRODUCER_FENCED on produce is fatal for a
    // transactional producer).  Handed to the duty pass: enterFatal_ is not re-entrant while a batch is failed.
    if (st == OpStatus::BrokerError && (errorCode == kInvalidProducerEpoch || errorCode == kProducerFenced) &&
        !std::holds_alternative<FatalError>(txn_) && !fatalPending_) {
        fatalPending_ = PendingFatal{OpStatus::Fenced, errorCode, "producer fenced by a partition leader (fatal)"};
    }
    // A record the transaction accepted failed: the transaction can no longer commit (poison_ keeps a commit
    // that may have landed, and only upgrades the abort kind once it is aborting).
    poison_(st, errorCode, kind);
}

void ProducerImpl::txnEnroll_(PartitionState& ps) noexcept {
    if (std::holds_alternative<NotInTxn>(ps.txn)) txnSeen_.push_back(ps.slotIndex);   // capacity reserved
}

bool ProducerImpl::txnHold_(PartitionState& ps) noexcept {
    const bool data = (ps.builder && ps.builder->recordCount() > 0) || ps.hasTier2();
    if (!data) return false;
    // The first records of a partition in this transaction: queued for the next AddPartitionsToTxn (txnStep),
    // and held until it is OK.  Outside the flowing states nothing is added: the data waits for the abort's
    // discard (or its delivery timeout).
    if (std::holds_alternative<NotInTxn>(ps.txn) && txnFlowing_()) {
        txnEnroll_(ps);
        ps.txn = Adding{nullptr};
        txnAddQueue_.push_back(ps.slotIndex);   // capacity reserved; each slot at most once
    }
    return true;
}

int ProducerImpl::addStep_(int64_t nowNs) noexcept {
    if (!txnFlowing_() || !coord_) return 0;   // queued partitions wait (an abort discards their data)
    const auto* id = std::get_if<IdAssigned>(&identity_);
    if (id == nullptr) return 0;
    const int64_t deadline = nowNs + std::max<int64_t>(cfg_.produceTimeoutMs, 1) * 1'000'000LL;
    const std::span<const uint8_t> txnId(reinterpret_cast<const uint8_t*>(cfg_.transactionalId.data()),
                                         cfg_.transactionalId.size());
    int         work = 0;
    std::size_t q    = 0;
    const std::size_t n = txnAddQueue_.size();
    // FD5: one request carries every queued partition; what does not fit one 4096-byte frame goes in the next slot.
    while (q < n) {
        CoordinatorLink::Slot* s = coord_->acquire(ReqKind::AddPartitions, deadline);
        if (s == nullptr) break;   // no free slot: the rest go next pass
        rpc::AddPartitionsToTxn m;
        m.correlationId   = s->corrId;
        m.producerId      = id->producerId;
        m.producerEpoch   = id->epoch;
        m.transactionalId = txnId;
        uint8_t*          f       = s->frame.data();
        std::size_t       pos     = m.encode(f, {});   // header + body + transactional id + count 0
        const std::size_t countAt = pos - 2;
        uint16_t          count   = 0;
        while (q < n && pos + 8 <= CoordinatorLink::kCoordFrameCap) {
            PartitionState& ps = *partitions_[txnAddQueue_[q++]];
            auto* ad = std::get_if<Adding>(&ps.txn);
            if (ad == nullptr || ad->slot != nullptr) continue;   // no longer queued
            rpc::detail::putLE32(f, static_cast<int>(pos), ps.topicId);
            rpc::detail::putLE32(f, static_cast<int>(pos) + 4, ps.partition);
            pos += 8;
            ++count;
            ad->slot = s;
        }
        if (count == 0) {
            coord_->release(*s);
            break;
        }
        rpc::detail::putLE16(f + countAt, 0, count);
        s->frameLen = static_cast<uint16_t>(pos);
        coord_->submit(*s);
        ++work;
    }
    txnAddQueue_.erase(txnAddQueue_.begin(), txnAddQueue_.begin() + static_cast<std::ptrdiff_t>(q));   // no allocation
    return work;
}

void ProducerImpl::onAddPartitionsAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s) noexcept {
    // An ErrorResponse carrying code 0 is a protocol error: InvalidRequest (39), never a code-less success.
    const int16_t  worst  = (a.anyKind && a.errorCode == 0) ? static_cast<int16_t>(kInvalidRequest) : a.errorCode;
    const uint16_t uworst = static_cast<uint16_t>(worst);
    switch (CoordinatorLink::classify(ReqKind::AddPartitions, worst)) {
    case Verdict::Fenced:
        enterFatal_(OpStatus::Fenced, uworst, "AddPartitionsToTxn: producer fenced (fatal)");
        return;
    case Verdict::Fatal:
        enterFatal_(OpStatus::BrokerError, uworst, "AddPartitionsToTxn failed (fatal)");
        return;
    default:
        break;
    }
    // Per partition: its own result when the response names it, else the request's (worst) code.
    std::optional<rpc::AddPartitionsToTxnResponse> m;
    if (!a.anyKind) m = rpc::AddPartitionsToTxnResponse::decode(a.frame.data(), a.frame.size());
    bool     anyOk    = false;
    uint16_t failCode = 0;
    for (const uint32_t slot : txnSeen_) {
        PartitionState& ps = *partitions_[slot];
        const auto* ad = std::get_if<Adding>(&ps.txn);
        if (ad == nullptr || ad->slot != &s) continue;
        int16_t code = worst;
        if (m) {
            for (std::size_t i = 0; i < m->results.size(); ++i) {
                const auto r = m->results[i];
                if (r.topicId == ps.topicId && r.partition == ps.partition) {
                    code = r.errorCode;
                    break;
                }
            }
        }
        if (code == 0) {
            ps.txn = Added{};
            markDirty_(ps);   // its held batches go on the next flush
            anyOk = true;
        } else {
            ps.txn = AddFailed{static_cast<uint16_t>(code)};   // never produced; discarded at abort
            if (failCode == 0) failCode = static_cast<uint16_t>(code);
        }
    }
    if (anyOk) markStarted_(TxnStarted::Yes);   // B-1: the coordinator has the transaction now
    if (failCode != 0) poison_(OpStatus::BrokerError, failCode, AbortKind::Plain);   // never produced: no gap
}

void ProducerImpl::onAddExpired_(CoordinatorLink::Slot& s) noexcept {
    // No answer by produceTimeoutMs: queued again (re-sent while the data flows).  A frame that was on the wire may
    // have added the partitions: the transaction is possibly started (m15: an abort then sends EndTxn(abort)).
    const bool offered = s.offers > 0;   // offered at least once (a session change put it back to Unsent)
    bool any = false;
    for (const uint32_t slot : txnSeen_) {
        PartitionState& ps = *partitions_[slot];
        auto* ad = std::get_if<Adding>(&ps.txn);
        if (ad == nullptr || ad->slot != &s) continue;
        ad->slot = nullptr;
        txnAddQueue_.push_back(slot);
        any = true;
    }
    if (any && offered) markStarted_(TxnStarted::Maybe);
}

bool ProducerImpl::txnAddsSettled_() const noexcept {
    for (const uint32_t slot : txnSeen_) {
        const auto* ad = std::get_if<Adding>(&partitions_[slot]->txn);
        if (ad != nullptr && ad->slot != nullptr) return false;
    }
    return true;
}

void ProducerImpl::txnResetMembership_() noexcept {
    for (const uint32_t slot : txnSeen_) {
        PartitionState& ps = *partitions_[slot];
        if (const auto* ad = std::get_if<Adding>(&ps.txn); ad != nullptr && ad->slot != nullptr && coord_) {
            coord_->release(*ad->slot);   // idempotent; unreachable after a settled end (the data wait covers it)
        }
        ps.txn = NotInTxn{};
    }
    txnSeen_.clear();
    txnAddQueue_.clear();
}

bool ProducerImpl::txnDiscardUnsent_(const TxnOutcome& o) noexcept {
    // An AddPartitionsToTxn still on the link is cancelled: the abort never waits for it, and the link never re-sends
    // it after EndTxn(abort) (a re-sent one could open a new transaction on the coordinator).  One that was on the
    // wire may have added its partitions: the transaction is possibly started (m15).
    bool offered = false;
    bool changed = false;
    for (const uint32_t slot : txnSeen_) {
        auto* ad = std::get_if<Adding>(&partitions_[slot]->txn);
        if (ad == nullptr || ad->slot == nullptr) continue;
        changed = true;
        if (ad->slot->offers > 0) offered = true;   // a session change leaves it Unsent, but it was offered
        if (coord_) coord_->release(*ad->slot);   // idempotent: the partitions of one request share its slot
        ad->slot = nullptr;
    }
    if (offered) markStarted_(TxnStarted::Maybe);
    // Task 7: a sendOffsets op still running is cancelled with the abort's outcome (the abort never waits for it).
    if (offsetsEnd_(o.status, o.errorCode, "sendOffsetsToTransaction: the transaction was aborted", OffsetsEnd::Cancelled)) {
        changed = true;
    }
    const DutyScope duty(*this);   // sends from the completions below are deferred (Direct)
    // The ring holds nothing of this transaction: records ahead of the abort were drained before it applied (FD1),
    // later ones are refused by the drain (the machine is not InTransaction).
    bool dropped = false;   // a record was dropped unproduced
    if (!deferred_.empty()) {
        changed = true;
        dropped = true;
        failDeferred_(o.status, o.errorCode);
    }
    for (std::size_t i = 0; i < partitions_.size(); ++i) {
        PartitionState& ps = *partitions_[i];
        if (ps.builder && ps.builder->recordCount() > 0) {
            changed = true;
            dropped = true;
            failBuilder_(ps, o.status, o.errorCode);
        }
        while (ps.hasTier2()) {
            changed = true;
            dropped = true;
            const uint32_t slot = t2Pop_(ps);
            const uint64_t h  = tier2Pool_[slot].ackHandle;
            const int32_t  rc = tier2Pool_[slot].recordCount;
            t2Return_(slot);
            emitUnsentBatchAck_(ps.topicId, ps.partition, rc, o.status, 1, o.errorCode);
            completeHandle_(h, OpKind::Tier2Batch, -1, rc, o.status, o.errorCode);
        }
    }
    if (dropped && txnFlushN_ != 0) {
        // E3 / B-MEDIUM-1: a flush whose records the abort dropped (never produced) must not complete Ok - it completes
        // with the abort's outcome (the scan restarts nothing: the array is taken first, completions may run user code).
        const std::array<TxnOp, kMaxTxnFlushes> flushes = txnFlushes_;
        const std::size_t                        nFlushes = txnFlushN_;
        txnFlushN_ = 0;
        for (std::size_t i = 0; i < nFlushes; ++i) {
            completeTxn_(flushes[i], OpKind::Flush, o.status, o.errorCode,
                         "flush: the transaction was aborted; its unsent records were dropped");
        }
    }
    return changed;
}

static_assert(ProducerImpl::kTxnResetOos == static_cast<uint16_t>(rpc::ErrorCode::OutOfOrderSequence));
static_assert(ProducerImpl::kTxnResetUnknownPid == static_cast<uint16_t>(rpc::ErrorCode::UnknownProducerId));

void ProducerImpl::txnResetPartition_(PartitionState& ps, uint16_t cause) noexcept {
    // KIP-360: a transactional producer's epoch belongs to its coordinator - a local bump would put this
    // partition's data under an epoch no EndTxn ever ends.  The partition sends nothing new (not Added, its id
    // pending), the transaction can only abort, and the abort's epoch bump restarts every sequence.
    ps.producerId = -1;
    const bool first = std::holds_alternative<NotInTxn>(ps.txn) || std::holds_alternative<Added>(ps.txn);
    if (first) {
        txnEnroll_(ps);
        ps.txn = AddFailed{cause};
    }
    poison_(OpStatus::BrokerError, cause, AbortKind::BumpEpoch);
    // Fix round 1 (I2): every batch still in flight here carries the old sequences, which can never be restarted
    // in place - re-sent, each would only meet the same gap until its delivery timeout, and the abort would wait for
    // it.  They fail now with the transaction's status (each record exactly once: the slot is released, a late
    // answer finds nothing); the abort hides whatever reached the log and its epoch bump fences the rest.
    const TxnOutcome o = txnRefusal_();
    ps.inflight.forEachLive([&](InFlightBatch& b) { failBatch_(ps, b, o.status, o.errorCode); });
    ps.resendParked = false;
    ps.laneWait     = false;
    ps.routeResend  = false;
    if (first) {   // one line per partition per transaction (a reset can repeat while the transaction aborts)
        std::fprintf(stderr,
                     "ProducerImpl: topic=%d part=%d transactional sequences must restart: the transaction can only "
                     "abort (its epoch bump restarts them)\n",
                     ps.topicId, ps.partition);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// close(): end the open transaction first (the removed transactional handle's close semantics)
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
        txn_ = Aborting{own, AwaitData{}, started, AbortKind::Plain, {}, TxnOutcome{}};
    } else if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
        const TxnStarted started = ae->started;
        const AbortKind  kind    = ae->kind;
        const TxnOutcome failed{ae->status, ae->errorCode};
        txn_ = Aborting{own, AwaitData{}, started, kind, {}, failed};
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


// ---------------------------------------------------------------------------------------------------------------
// Task 7: sendOffsetsToTransaction - AddOffsetsToTxn, then TxnOffsetCommit (split per FD5)
// ---------------------------------------------------------------------------------------------------------------

void ProducerImpl::applySendOffsets_(const TxnControl& c, const TxnOp& op) {
    // The state gates (InTransaction only; AbortableError refuses with the poison) ran in applyTxnControl.
    if (offsetsOp_.index() != 0) {
        refuseTxn_(c, OpStatus::BrokerError, kInvalidTxnState, "sendOffsetsToTransaction: another call is still running");
        return;
    }
    if (c.offsets.size() > kMaxTxnOffsets) {
        refuseTxn_(c, OpStatus::InvalidArgument, 0, "sendOffsetsToTransaction: at most 1024 offsets per call");
        return;
    }
    if (c.groupId.empty() || c.groupId.size() > kMaxTxnGroupIdBytes || c.memberId.size() > kMaxTxnMemberIdBytes) {
        refuseTxn_(c, OpStatus::InvalidArgument, 0,
                   "sendOffsetsToTransaction: at most 1024 offsets, a group id of 1 to 255 bytes and a member id of at "
                   "most 512 bytes");
        return;
    }
    OffsetsScratch& sc = *offsetsScratch_;   // allocated at construction of a transactional producer
    std::copy(c.offsets.begin(), c.offsets.end(), sc.entries.begin());
    std::memcpy(sc.group.data(), c.groupId.data(), c.groupId.size());
    if (!c.memberId.empty()) std::memcpy(sc.member.data(), c.memberId.data(), c.memberId.size());
    sc.n        = static_cast<uint16_t>(c.offsets.size());
    sc.groupLen = static_cast<uint8_t>(c.groupId.size());
    sc.memberLen    = static_cast<uint16_t>(c.memberId.size());
    sc.generationId = c.generationId < 0 ? -1 : c.generationId;   // #125: < 0 = no member block (legacy)
    offsetsOp_  = OffsetsAdding{op, nullptr};   // txnStep sends the AddOffsetsToTxn
}

namespace {
// TxnOffsetCommit::encode's entry layout (TxnMessages.h): topicId(4) partition(4) offset(8) metaLen(2), no metadata.
// The entries are written by hand (no entry-array scratch needed); the count is the last field of the empty encode.
constexpr std::size_t kTxnOffsetEntryBytes = 4 + 4 + 8 + 2;
// A frame holds the header, the ids (a transactional id is at most 255 bytes, Client.cpp), at least one entry and the
// #125 member block (generationId 4 + u16 length + the member id).
static_assert(24 + 10 + 2 + 255 + 2 + kMaxTxnGroupIdBytes + 2 + kTxnOffsetEntryBytes +
                      rpc::TxnOffsetCommit::kMemberBlockFixed + kMaxTxnMemberIdBytes <= CoordinatorLink::kCoordFrameCap,
              "a TxnOffsetCommit frame must hold at least one entry and the member block");
}  // namespace

int ProducerImpl::offsetsStep_(int64_t nowNs) noexcept {
    const std::span<const uint8_t> txnId(reinterpret_cast<const uint8_t*>(cfg_.transactionalId.data()),
                                         cfg_.transactionalId.size());
    OffsetsScratch& sc = *offsetsScratch_;
    if (const auto* ae = std::get_if<AbortableError>(&txn_)) {
        // Another leg poisoned the transaction: it can only abort, so no more requests - the call ends with the poison.
        offsetsEnd_(ae->status, ae->errorCode, "sendOffsetsToTransaction: the transaction failed - abort it",
                    OffsetsEnd::Cancelled);
        return 1;
    }
    const std::span<const uint8_t> group(reinterpret_cast<const uint8_t*>(sc.group.data()), sc.groupLen);
    if (auto* oa = std::get_if<OffsetsAdding>(&offsetsOp_)) {
        if (nowNs >= oa->op.deadlineNs) {
            offsetsExpired_(nullptr);
            return 1;
        }
        const auto* id = std::get_if<IdAssigned>(&identity_);
        if (oa->slot != nullptr || !coord_ || id == nullptr) return 0;
        CoordinatorLink::Slot* s = coord_->acquire(ReqKind::AddOffsets, oa->op.deadlineNs);
        if (s == nullptr) return 0;   // no free slot: next pass
        rpc::AddOffsetsToTxn m;
        m.correlationId   = s->corrId;
        m.producerId      = id->producerId;
        m.producerEpoch   = id->epoch;
        m.transactionalId = txnId;
        m.groupId         = group;
        s->frameLen = static_cast<uint16_t>(m.encode(s->frame.data()));
        coord_->submit(*s);
        oa->slot = s;
        return 1;
    }
    auto* oc = std::get_if<OffsetsCommitting>(&offsetsOp_);
    if (oc == nullptr) return 0;
    if (nowNs >= oc->op.deadlineNs) {
        offsetsExpired_(nullptr);
        return 1;
    }
    if (oc->sent >= sc.n) {
        if (oc->outstanding != 0) return 0;
        offsetsEnd_(OpStatus::Ok, 0, {}, OffsetsEnd::Done);   // no entries: AddOffsetsToTxn was all there was
        return 1;
    }
    const auto* id = std::get_if<IdAssigned>(&identity_);
    if (!coord_ || id == nullptr) return 0;
    int work = 0;
    // FD5: the entries that do not fit one kCoordFrameCap frame go in the next request (slot); a pass that finds no
    // free slot leaves the rest for the next one.
    while (oc->sent < sc.n) {
        CoordinatorLink::Slot* s = coord_->acquire(ReqKind::TxnOffsetCommit, oc->op.deadlineNs);
        if (s == nullptr) break;
        rpc::TxnOffsetCommit m;
        m.correlationId   = s->corrId;
        m.producerId      = id->producerId;
        m.producerEpoch   = id->epoch;
        m.transactionalId = txnId;
        m.groupId         = group;
        uint8_t*          f       = s->frame.data();
        std::size_t       pos     = m.encode(f, {});   // header + ids + count 0 (generationId -1: no block yet)
        const std::size_t countAt = pos - 2;
        uint16_t          count   = 0;
        // #125: every request of the split carries the member block after its entries (room reserved for it).
        m.generationId = sc.generationId;
        m.memberId     = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sc.member.data()), sc.memberLen);
        const std::size_t block = m.memberBlockSize();
        while (oc->sent < sc.n && pos + kTxnOffsetEntryBytes + block <= CoordinatorLink::kCoordFrameCap) {
            const OffsetEntry& e = sc.entries[oc->sent++];
            rpc::detail::putLE32(f, static_cast<int>(pos), e.topicId);
            rpc::detail::putLE32(f, static_cast<int>(pos) + 4, e.partition);
            rpc::detail::putLE64(f, static_cast<int>(pos) + 8, e.offset);
            rpc::detail::putLE16(f, static_cast<int>(pos) + 16, 0);
            pos += kTxnOffsetEntryBytes;
            ++count;
        }
        assert(count > 0);   // the frame always has room for an entry (static_assert above)
        rpc::detail::putLE16(f + countAt, 0, count);
        pos += m.encodeMemberBlock(f, pos);
        s->frameLen = static_cast<uint16_t>(pos);
        coord_->submit(*s);
        oc->slots[oc->outstanding++] = s;
        ++work;
    }
    return work;
}

void ProducerImpl::onOffsetsAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s) noexcept {
    // An ErrorResponse carrying code 0 is a protocol error: InvalidRequest (39), never a code-less success.
    const int16_t  code  = (a.anyKind && a.errorCode == 0) ? static_cast<int16_t>(kInvalidRequest) : a.errorCode;
    const uint16_t ucode = static_cast<uint16_t>(code);
    auto* oa = std::get_if<OffsetsAdding>(&offsetsOp_);
    auto* oc = std::get_if<OffsetsCommitting>(&offsetsOp_);
    if (s.kind == ReqKind::AddOffsets ? (oa == nullptr || oa->slot != &s) : oc == nullptr) return;   // stale
    if (oc != nullptr) {   // a TxnOffsetCommit request: it leaves the outstanding set (the caller releases the slot)
        std::size_t i = 0;
        while (i < oc->outstanding && oc->slots[i] != &s) ++i;
        if (i == oc->outstanding) return;   // not ours (stale)
        for (; i + 1 < oc->outstanding; ++i) oc->slots[i] = oc->slots[i + 1];
        oc->slots[--oc->outstanding] = nullptr;
    }
    switch (CoordinatorLink::classify(s.kind, code)) {
    case Verdict::Ok:
        if (oa != nullptr) {
            markStarted_(TxnStarted::Yes);   // B-1: AddOffsetsToTxn OK - the transaction exists on the coordinator
            offsetsOp_ = OffsetsCommitting{oa->op, 0, 0, {}};   // txnStep sends the TxnOffsetCommit requests
        } else if (oc->sent >= offsetsScratch_->n && oc->outstanding == 0) {
            offsetsEnd_(OpStatus::Ok, 0, {}, OffsetsEnd::Done);
        }
        return;
    case Verdict::Retry:
    case Verdict::Rediscover:
        return;   // the link re-sends; never handed over
    case Verdict::Fenced:
        enterFatal_(OpStatus::Fenced, ucode, "producer fenced (fatal)");
        return;
    case Verdict::Fatal:
        enterFatal_(OpStatus::BrokerError, ucode, "transaction leg failed (fatal)");
        return;
    case Verdict::Abortable:
    case Verdict::Reject:
        offsetsEnd_(OpStatus::BrokerError, ucode,
                    s.kind == ReqKind::AddOffsets ? "AddOffsetsToTxn failed" : "TxnOffsetCommit failed",
                    OffsetsEnd::Failed);
        return;
    }
}

void ProducerImpl::offsetsExpired_(CoordinatorLink::Slot*) noexcept {
    // The call is in doubt (a leg may have landed): the transaction can only abort, like any failed leg.
    offsetsEnd_(OpStatus::Timeout, 0, "sendOffsetsToTransaction: timed out (the offsets are in doubt: abort)",
                OffsetsEnd::TimedOut);
}

bool ProducerImpl::offsetsEnd_(OpStatus st, uint16_t errorCode, std::string_view text, OffsetsEnd how) noexcept {
    TxnOp op{};
    bool  maybe = false;
    if (const auto* oa = std::get_if<OffsetsAdding>(&offsetsOp_)) {
        op = oa->op;
        if (oa->slot != nullptr) {
            maybe = oa->slot->offers > 0;   // offered (even if a session change put it back to Unsent)
            if (coord_) coord_->release(*oa->slot);
        }
    } else if (const auto* oc = std::get_if<OffsetsCommitting>(&offsetsOp_)) {
        op = oc->op;
        for (uint8_t i = 0; i < oc->outstanding; ++i) {
            maybe = maybe || oc->slots[i]->offers > 0;
            if (coord_) coord_->release(*oc->slots[i]);
        }
    } else {
        return false;
    }
    offsetsOp_ = OffsetsIdle{};
    // A leg that was on the wire may have started the transaction: an abort then sends EndTxn(abort) (m15).
    if (maybe && (how == OffsetsEnd::TimedOut || how == OffsetsEnd::Cancelled)) markStarted_(TxnStarted::Maybe);
    if (how == OffsetsEnd::Failed || how == OffsetsEnd::TimedOut) poison_(st, errorCode, AbortKind::Plain);
    completeTxn_(op, OpKind::SendOffsets, st, errorCode, text);
    return true;
}

}  // namespace rune::client::producer
