// src/producer/CoordinatorLink.cpp - Phase 2b F (FD5 / FD6).  See CoordinatorLink.h.
#include <rune/client/producer/CoordinatorLink.h>

#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <algorithm>
#include <cassert>

namespace rune::client::producer {

namespace rpc = rune::protocol::client;

namespace {
constexpr int16_t kOk                     = 0;
constexpr int16_t kNotCoordinator         = static_cast<int16_t>(rpc::ErrorCode::NotCoordinator);
constexpr int16_t kNotLeader             = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
constexpr int16_t kInvalidProducerEpoch   = static_cast<int16_t>(rpc::ErrorCode::InvalidProducerEpoch);
constexpr int16_t kProducerFenced         = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);
constexpr int16_t kInvalidTxnState        = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr int16_t kConcurrentTransactions = static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions);
constexpr int16_t kInvalidRequest         = static_cast<int16_t>(rpc::ErrorCode::InvalidRequest);
constexpr int16_t kIllegalGeneration      = static_cast<int16_t>(rpc::ErrorCode::IllegalGeneration);
constexpr int16_t kUnknownMember          = static_cast<int16_t>(rpc::ErrorCode::UnknownMember);
constexpr int16_t kAuthenticationFailed   = static_cast<int16_t>(rpc::ErrorCode::AuthenticationFailed);  // 44..47
constexpr int16_t kPasswordChangeRequired = static_cast<int16_t>(rpc::ErrorCode::PasswordChangeRequired);
constexpr int16_t kAuthUnavailable        = static_cast<int16_t>(rpc::ErrorCode::AuthUnavailable);         // 48
constexpr int16_t kNamespaceSuspended     = static_cast<int16_t>(rpc::ErrorCode::NamespaceSuspended);      // 51
constexpr int16_t kNamespaceLimitExceeded = static_cast<int16_t>(rpc::ErrorCode::NamespaceLimitExceeded);  // 52
constexpr int16_t kThrottled              = static_cast<int16_t>(rpc::ErrorCode::Throttled);               // 53
}  // namespace

Verdict CoordinatorLink::classify(ReqKind kind, int16_t ec) noexcept {
    if (ec == kOk) return Verdict::Ok;
    if (ec == kConcurrentTransactions) return Verdict::Retry;         // retriable everywhere
    // #607 class: AuthUnavailable (48) = the broker cannot check security yet (booting / behind): transient on every
    // leg, retried with backoff until the request's deadline - never fatal, never an abort.
    if (ec == kAuthUnavailable) return Verdict::Retry;
    if (ec == kNotCoordinator) return Verdict::Rediscover;            // re-discover, then re-send
    if (ec == kProducerFenced || ec == kInvalidProducerEpoch) return Verdict::Fenced;
    // The broker could not decode the request: re-sending the same bytes fails the same way.
    if (ec == kInvalidRequest) return Verdict::Reject;
    // #503: the coordinator has not applied this topic's placement yet (PR 501): transient, retried with backoff
    // until the request's deadline (Kafka: NOT_LEADER / UNKNOWN_TOPIC_OR_PARTITION are retriable).
    if (ec == kNotLeader && (kind == ReqKind::AddPartitions || kind == ReqKind::TxnOffsetCommit)) return Verdict::Retry;
    // Authentication (44..47): a login or a grant is an admin's act - never retried.  InitProducerId and
    // EndTxn cannot go on without it (fatal); the other legs fail only the transaction (abortable).
    // Namespaces Phase 2: Throttled (53) is a rate limit - retried with backoff until the request's deadline.
    if (ec == kThrottled) return Verdict::Retry;
    // NamespaceSuspended (51) / NamespaceLimitExceeded (52) are an admin's act like 44..47 (a suspend, a limit): a re-send
    // gets the same answer.  Same split: InitProducerId / EndTxn fatal, the data legs abortable.
    if ((ec >= kAuthenticationFailed && ec <= kPasswordChangeRequired) || ec == kNamespaceSuspended ||
        ec == kNamespaceLimitExceeded) {
        return (kind == ReqKind::InitProducerId || kind == ReqKind::EndTxn) ? Verdict::Fatal : Verdict::Abortable;
    }
    // #125 (KIP-447): the TxnOffsetCommit's member block was fenced (the group rebalanced / the member is gone):
    // never retried - the transaction can only abort; the consumer rejoins and re-processes from the committed offsets.
    if (kind == ReqKind::TxnOffsetCommit && (ec == kIllegalGeneration || ec == kUnknownMember)) return Verdict::Abortable;
    switch (kind) {
    case ReqKind::InitProducerId:
        return Verdict::Fatal;                                         // anything not retriable is fatal
    case ReqKind::EndTxn:
        return ec == kInvalidTxnState ? Verdict::Fatal : Verdict::Retry;   // retried until 0 / 19 / 17 / 18
    case ReqKind::AddPartitions:
    case ReqKind::AddOffsets:
    case ReqKind::TxnOffsetCommit:
        return Verdict::Abortable;                                     // 19 / unknown: the transaction can only abort
    }
    return Verdict::Abortable;
}

int64_t CoordinatorLink::backoffNs(uint32_t attempt) noexcept {
    if (attempt == 0) return 0;
    const uint32_t shift = std::min<uint32_t>(attempt - 1, 20u);
    return std::min<int64_t>(kBackoffBaseNs << shift, kBackoffCapNs);
}

CoordinatorLink::Slot* CoordinatorLink::acquire(ReqKind kind, int64_t deadlineNs) noexcept {
    assert(deadlineNs > 0 && "every coordinator request needs a deadline");
    if (deadlineNs <= 0) return nullptr;
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        Slot& s = slots_[i];
        if (!std::holds_alternative<Free>(s.state)) continue;
        s.state      = Building{};   // claimed; not offered until submit()
        s.kind       = kind;
        s.attempt    = 0;
        s.offers     = 0;
        s.deadlineNs = deadlineNs;
        s.frameLen   = 0;
        s.corrId     = coordcorr::make(ordinal_, static_cast<uint8_t>(i), (++seq_ == 0 ? ++seq_ : seq_));
        ++live_;
        return &s;
    }
    return nullptr;
}

void CoordinatorLink::submit(Slot& s) noexcept {
    assert(std::holds_alternative<Building>(s.state) && s.frameLen >= kHdrCorrOff + 8 && "submit: the caller encodes the request before submit()");
    rpc::detail::putLE64(s.frame.data(), static_cast<int>(kHdrCorrOff), s.corrId);
    s.state = Unsent{};
}

void CoordinatorLink::release(Slot& s) noexcept {
    if (std::holds_alternative<Free>(s.state)) return;
    --live_;
    s.state = Free{};
    s.corrId = 0;
    s.frameLen = 0;
}

bool CoordinatorLink::offer_(Slot& s) noexcept {
    const int64_t sid = sink_.sessionId != nullptr ? sink_.sessionId(sink_.ctx) : 0;
    if (sid == 0 || sink_.offer == nullptr) return false;   // not connected: held, re-tried next pass
    rpc::detail::putLE64(s.frame.data(), static_cast<int>(kHdrSessionOff), sid);
    switch (sink_.offer(sink_.ctx, s.frame.data(), s.frameLen)) {
    case CoordinatorSink::Offer::Retry:
        return false;
    case CoordinatorSink::Offer::Accepted:
    case CoordinatorSink::Offer::Fatal:   // never deliverable: consumed, the slot's deadline completes it
        s.state = InFlight{sid, s.deadlineNs};
        ++s.offers;
        return true;
    }
    return false;
}

int CoordinatorLink::offerIfDue_(Slot& s, int64_t nowNs) noexcept {
    if (std::holds_alternative<Unsent>(s.state)) return offer_(s) ? 1 : 0;
    if (const auto* b = std::get_if<Backoff>(&s.state)) return (nowNs >= b->untilNs && offer_(s)) ? 1 : 0;
    if (const auto* a = std::get_if<AwaitCoordinator>(&s.state)) {
        if (a->gen != gen_) return (nowNs >= a->notBeforeNs && offer_(s)) ? 1 : 0;
        askRediscover_();   // #351: a refused ask is retried each pass until it is submitted
        return 0;
    }
    return 0;   // InFlight: waits for its answer (or onSessionChanged / its deadline)
}

void CoordinatorLink::askRediscover_() noexcept {
    if (askedGen_ == gen_) return;   // F14: once per generation, however many requests wait
    if (sink_.rediscover == nullptr || sink_.rediscover(sink_.ctx)) askedGen_ = gen_;
}

void CoordinatorLink::onSessionChanged() noexcept {
    for (Slot& s : slots_) {
        if (std::holds_alternative<InFlight>(s.state)) s.state = Unsent{};   // same corrId: idempotent at the coordinator
    }
}

CoordinatorLink::Slot* CoordinatorLink::onAnswer(const CoordAnswer& a, int64_t nowNs) noexcept {
    if (!coordcorr::isCoord(a.corrId) || coordcorr::ordinal(a.corrId) != (ordinal_ & batchcorr::kMaxOrdinal)) return nullptr;
    const std::size_t i = coordcorr::slot(a.corrId);
    if (i >= slots_.size()) return nullptr;
    Slot& s = slots_[i];
    if (std::holds_alternative<Free>(s.state) || s.corrId != a.corrId) return nullptr;   // late / stale
    if (!a.anyKind && a.kind != s.kind) return nullptr;
    switch (classify(s.kind, a.errorCode)) {
    case Verdict::Retry:
        // Only the answer to the offer on the wire retries; a late duplicate (the re-send is already
        // pending) changes nothing.
        if (!std::holds_alternative<InFlight>(s.state)) return nullptr;
        ++s.attempt;
        s.state = Backoff{nowNs + backoffNs(s.attempt)};
        return nullptr;
    case Verdict::Rediscover:
        if (!std::holds_alternative<InFlight>(s.state)) return nullptr;
        ++s.attempt;
        s.state = AwaitCoordinator{gen_, nowNs + backoffNs(s.attempt)};
        askRediscover_();
        return nullptr;
    default:
        return &s;
    }
}

}  // namespace rune::client::producer
