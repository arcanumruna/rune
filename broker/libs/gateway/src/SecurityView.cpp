// SecurityView.cpp — the control thread's mirror of __security (authentication plan D29).
#include <rune/gateway/SecurityView.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <string>
#include <utility>

namespace rune::gateway {

namespace sec = rune::security;

std::expected<sec::Applied, sec::ApplyError> SecurityView::onApplied(int64_t offset, std::span<const uint8_t> bytes) {
    if (const Halted* h = std::get_if<Halted>(&st_)) return std::unexpected(h->error);
    auto fail = [&](sec::ApplyError::Code code, std::string detail) {
        sec::ApplyError err;
        err.offset = offset;
        err.code   = code;
        err.detail = std::move(detail);
        return std::unexpected(halt_(std::move(err)));
    };
    // A gap or a repeat is a halt: the mirror never skips and never re-applies.
    if (offset != state_.appliedOffset() + 1)
        return fail(sec::ApplyError::Code::OutOfOrder,
                    "the control thread's security mirror expected offset " +
                        std::to_string(state_.appliedOffset() + 1) + ", got " + std::to_string(offset));
    auto ev = sec::decode(bytes);
    if (!ev) return fail(ev.error() == sec::DecodeError::UnknownFormatVer ? sec::ApplyError::Code::UnsupportedFormat
                                                                          : sec::ApplyError::Code::BadOps,
                         "the mirrored record does not decode: " + std::string{sec::decodeErrorName(ev.error())});
    // #269: the fake-iteration mode is maintained incrementally for the events that change a user's verifiers.
    const bool userKind = ev->kind == sec::EventKind::CreateUser || ev->kind == sec::EventKind::AlterUser ||
                          ev->kind == sec::EventKind::DropUser;
    if (userKind) {
        if (const sec::UserObject* u = state_.userByName(ev->nsId, ev->objectName)) countVerifiers_(*u, -1);
    }
    auto a = sec::apply(state_, *ev, offset);
    if (!a) return std::unexpected(halt_(std::move(a.error())));   // halted: the counts no longer matter
    window_ = state_.hasSecurityAdmin() ? Window::Closed : Window::Open;   // once per applied event
    switch (ev->kind) {
        case sec::EventKind::CreateUser:
        case sec::EventKind::AlterUser:
        case sec::EventKind::DropUser:
            if (const sec::UserObject* u = state_.userByName(ev->nsId, ev->objectName)) countVerifiers_(*u, +1);
            pickFakeIterations_();
            break;
        case sec::EventKind::CreateRole:
        case sec::EventKind::DropRole:
        case sec::EventKind::Grant:
        case sec::EventKind::Revoke:
        case sec::EventKind::GrantRole:
        case sec::EventKind::RevokeRole:
            break;   // no verifier changes
        default:     // Bootstrap, NamespacePurged, anything new: the full rebuild
            rebuildDerived_();
            break;
    }
    return a;
}

void SecurityView::onHalted(const sec::ApplyError& error) {
    if (halted()) return;   // the first cause stays
    (void)halt_(error);
}

void SecurityView::rebuildDerived_() {
    ++derivedFullRebuilds_;
    window_ = state_.hasSecurityAdmin() ? Window::Closed : Window::Open;
    iterationCounts_.clear();
    for (const auto& [id, u] : state_.users()) countVerifiers_(u, +1);
    pickFakeIterations_();
}

void SecurityView::countVerifiers_(const sec::UserObject& user, int delta) {
    for (const auto& v : user.verifiers) {
        const auto it = std::lower_bound(iterationCounts_.begin(), iterationCounts_.end(), v.iterations,
                                         [](const auto& e, uint32_t k) { return e.first < k; });
        if (it != iterationCounts_.end() && it->first == v.iterations) {
            if (delta > 0) ++it->second;
            else if (--it->second == 0) iterationCounts_.erase(it);
        } else if (delta > 0) {
            iterationCounts_.insert(it, {v.iterations, 1});
        }
    }
}

void SecurityView::pickFakeIterations_() {
    uint32_t best = 4096;
    std::size_t bestN = 0;
    for (const auto& [it, n] : iterationCounts_)   // ascending: on a tie the smallest count wins
        if (n > bestN) { best = it; bestN = n; }
    fakeIterations_ = best;
}

sec::ApplyError SecurityView::halt_(sec::ApplyError error) {
    spdlog::critical("SecurityView: the control thread's __security mirror HALTED at offset {} — {} {} (state at "
                     "offset {}); sessions fail closed", error.offset, sec::applyErrorName(error.code), error.detail,
                     state_.appliedOffset());
    st_ = Halted{error};
    return error;
}

} // namespace rune::gateway
