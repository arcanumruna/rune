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
constexpr int16_t kInvalidProducerEpoch   = static_cast<int16_t>(rpc::ErrorCode::InvalidProducerEpoch);
constexpr int16_t kProducerFenced         = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);
constexpr int16_t kInvalidTxnState        = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);
constexpr int16_t kConcurrentTransactions = static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions);
constexpr int16_t kInvalidRequest         = static_cast<int16_t>(rpc::ErrorCode::InvalidRequest);
constexpr int16_t kAuthenticationFailed   = static_cast<int16_t>(rpc::ErrorCode::AuthenticationFailed);  // 44..47
constexpr int16_t kPasswordChangeRequired = static_cast<int16_t>(rpc::ErrorCode::PasswordChangeRequired);
}  // namespace

Verdict CoordinatorLink::classify(ReqKind kind, int16_t ec) noexcept {
    if (ec == kOk) return Verdict::Ok;
    if (ec == kConcurrentTransactions) return Verdict::Retry;         // retriable everywhere
    if (ec == kNotCoordinator) return Verdict::Rediscover;            // re-discover, then re-send
    if (ec == kProducerFenced || ec == kInvalidProducerEpoch) return Verdict::Fenced;
    // The broker could not decode the request: re-sending the same bytes fails the same way.
    if (ec == kInvalidRequest) return Verdict::Reject;
    // Authentication (44..47): a login or a grant is an admin's act - never retried.  InitProducerId and
    // EndTxn cannot go on without it (fatal); the other legs fail only the transaction (abortable).
    if (ec >= kAuthenticationFailed && ec <= kPasswordChangeRequired) {
        return (kind == ReqKind::InitProducerId || kind == ReqKind::EndTxn) ? Verdict::Fatal : Verdict::Abortable;
    }
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
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        Slot& s = slots_[i];
        if (!std::holds_alternative<Free>(s.state)) continue;
        s.state      = Building{};   // claimed; not offered until submit()
        s.kind       = kind;
        s.attempt    = 0;
        s.deadlineNs = deadlineNs;
        s.frameLen   = 0;
        s.corrId     = coordcorr::make(ordinal_, static_cast<uint8_t>(i), ++seq_);
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
    s.state = Free{};
    s.corrId = 0;
    s.frameLen = 0;
}

std::size_t CoordinatorLink::inUse() const noexcept {
    std::size_t n = 0;
    for (const Slot& s : slots_) n += std::holds_alternative<Free>(s.state) ? 0 : 1;
    return n;
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
        return true;
    }
    return false;
}

int CoordinatorLink::offerIfDue_(Slot& s, int64_t nowNs) noexcept {
    if (std::holds_alternative<Unsent>(s.state)) return offer_(s) ? 1 : 0;
    if (const auto* b = std::get_if<Backoff>(&s.state)) return (nowNs >= b->untilNs && offer_(s)) ? 1 : 0;
    if (const auto* a = std::get_if<AwaitCoordinator>(&s.state)) return (a->gen != gen_ && offer_(s)) ? 1 : 0;
    return 0;   // InFlight: waits for its answer (or onSessionChanged / its deadline)
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
        s.state = AwaitCoordinator{gen_};
        return nullptr;
    default:
        return &s;
    }
}

}  // namespace rune::client::producer
