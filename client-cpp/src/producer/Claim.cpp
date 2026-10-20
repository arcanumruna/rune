// src/producer/Claim.cpp — Phase 2b Part B: Claim (zero-copy produce) and the
// claim credit settlement shared with BatchClaim.
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <utility>

namespace rune::client {

namespace detail {

namespace {
void giveBack(const ClaimOpen& o) noexcept {
    if (o.cq != nullptr) o.cq->unreserve(producer::kAckRecordBytes);
    if (o.bq != nullptr) {
        for (uint32_t i = 0; i < o.units; ++i) o.bq->unreserve(producer::kAckRecordBytes);
    }
}
}  // namespace

bool ClaimCredit::mayCommit(const ClaimOpen& o) noexcept {
    return (o.cq == nullptr || o.cq->onOwnerThread()) && (o.bq == nullptr || o.bq->onOwnerThread());
}

bool ClaimCredit::commit(const ClaimOpen& o) noexcept {
    if (!o.impl->commitClaim(o.token)) {
        giveBack(o);  // abandoned: no completion will ever arrive
        return false;
    }
    if (o.cq != nullptr) ++o.cq->producerIssued_;
    if (o.bq != nullptr) o.bq->producerIssued_ += o.units;
    return true;
}

void ClaimCredit::abort(const ClaimOpen& o) noexcept {
    o.impl->abortClaim(o.token);  // the slot must be released from any thread
    if (mayCommit(o)) giveBack(o);  // credit is owner-thread state: never touched off it
}

}  // namespace detail

Claim::Claim(Claim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}

Claim& Claim::operator=(Claim&& o) noexcept {
    if (this != &o) {
        abort();
        state_ = std::exchange(o.state_, Settled{});
    }
    return *this;
}

Claim::~Claim() { abort(); }

SendResult Claim::result() const noexcept {
    if (const auto* r = std::get_if<Refused>(&state_)) return r->why;
    return SendResult::Ok;
}

std::span<uint8_t> Claim::buffer() const noexcept {
    if (const auto* o = std::get_if<Open>(&state_)) return {o->value, o->len};
    return {};
}

bool Claim::commit() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return false;
    const detail::ClaimOpen core = o->core;
    if (!detail::ClaimCredit::mayCommit(core)) return false;  // stays open: not this thread's to settle
    state_ = Settled{};
    return detail::ClaimCredit::commit(core);
}

void Claim::abort() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    detail::ClaimCredit::abort(core);
}

}  // namespace rune::client
