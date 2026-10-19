// SecurityView.cpp — the control thread's mirror of __security (authentication plan D29).
#include <rune/gateway/SecurityView.h>

#include <spdlog/spdlog.h>

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
    auto a = sec::apply(state_, *ev, offset);
    if (!a) return std::unexpected(halt_(std::move(a.error())));
    return a;
}

void SecurityView::onHalted(const sec::ApplyError& error) {
    if (halted()) return;   // the first cause stays
    (void)halt_(error);
}

sec::ApplyError SecurityView::halt_(sec::ApplyError error) {
    spdlog::critical("SecurityView: the control thread's __security mirror HALTED at offset {} — {} {} (state at "
                     "offset {}); sessions fail closed", error.offset, sec::applyErrorName(error.code), error.detail,
                     state_.appliedOffset());
    st_ = Halted{error};
    return error;
}

} // namespace rune::gateway
