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
    return o.impl->maySettleClaim() && (o.cq == nullptr || o.cq->onOwnerThread()) &&
           (o.bq == nullptr || o.bq->onOwnerThread());
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
    if (mayCommit(o)) {
        o.impl->abortClaim(o.token);
        giveBack(o);
        return;
    }
    o.impl->abortClaimOffOwner(o.token);  // #322: flagged, never silent; owner state untouched
}

}  // namespace detail

// Claim's accessors, move and the open-test of commit() / abort() / ~Claim
// are inline in Producer.h (Task 10); only settling an Open claim is here.
// The open state is read in place and the Claim marked Settled after the
// settle call (no copy of it); nothing a settle calls can reach this Claim.
bool Claim::commitOpen_() noexcept {
    const detail::ClaimOpen& core = std::get_if<Open>(&state_)->core;  // callers checked Open
    if (!detail::ClaimCredit::mayCommit(core)) return false;  // stays open: not this thread's to settle
    const bool ok = detail::ClaimCredit::commit(core);
    state_ = Settled{};
    return ok;
}

void Claim::abortOpen_() noexcept {
    detail::ClaimCredit::abort(std::get_if<Open>(&state_)->core);  // callers checked Open
    state_ = Settled{};
}

BatchClaim::BatchClaim(BatchClaim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}

BatchClaim& BatchClaim::operator=(BatchClaim&& o) noexcept {
    if (this != &o) {
        abort();
        state_ = std::exchange(o.state_, Settled{});
    }
    return *this;
}

BatchClaim::~BatchClaim() { abort(); }

SendResult BatchClaim::result() const noexcept {
    if (const auto* r = std::get_if<Refused>(&state_)) return r->why;
    return SendResult::Ok;
}

std::span<uint8_t> BatchClaim::record(uint32_t i) const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr || i >= o->n) return {};
    uint8_t* p = o->offsets != nullptr ? o->base + o->offsets[i]
                                       : o->base + static_cast<std::size_t>(i) * o->recLen;
    return {p, o->recLen};
}

uint32_t BatchClaim::count() const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    return o != nullptr ? o->n : 0u;
}

uint32_t BatchClaim::recordLength() const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    return o != nullptr ? o->recLen : 0u;
}

bool BatchClaim::commit() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return false;
    const detail::ClaimOpen core = o->core;
    if (!detail::ClaimCredit::mayCommit(core)) return false;  // stays open: not this thread's to settle
    state_ = Settled{};
    return detail::ClaimCredit::commit(core);
}

void BatchClaim::abort() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    detail::ClaimCredit::abort(core);
}

}  // namespace rune::client
