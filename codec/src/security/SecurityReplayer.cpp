// src/security/SecurityReplayer.cpp — see rune/security/SecurityReplayer.h.
#include <rune/security/SecurityReplayer.h>

#include <cstdio>
#include <string>
#include <utility>

namespace rune::security {

namespace {
std::string hex(uint64_t v) {
    char buf[2 + 16 + 1];
    std::snprintf(buf, sizeof buf, "0x%016llx", static_cast<unsigned long long>(v));
    return buf;
}
std::string_view kindText(ReplayError::Kind k) noexcept {
    switch (k) {
    case ReplayError::Kind::Decode: return "Decode";
    case ReplayError::Kind::Apply:  return "Apply";
    case ReplayError::Kind::Gap:    return "Gap";
    }
    return "?";
}
} // namespace

std::string describe(const ReplayError& e) {
    std::string s;
    if (e.offset >= 0) s = "offset " + std::to_string(e.offset) + ": ";
    s += kindText(e.kind);
    if (e.decode) { s += ' '; s += decodeErrorName(*e.decode); }
    if (e.apply && e.kind == ReplayError::Kind::Gap) {
        s += ' '; s += e.apply->detail;   // "expected offset N"
    } else if (e.apply) {
        s += ' ';
        s += applyErrorName(e.apply->code);
        if (e.apply->refusal) { s += ' '; s += refusalName(*e.apply->refusal); }
        if (e.apply->code == ApplyError::Code::FingerprintMismatch)
            s += " expected=" + hex(e.apply->expected) + " actual=" + hex(e.apply->actual);
        if (!e.apply->detail.empty()) { s += ' '; s += e.apply->detail; }
    }
    return s;
}

std::expected<Applied, ReplayError> SecurityReplayer::feed(int64_t offset, std::span<const uint8_t> record) {
    auto e = decode(record);
    if (!e) return std::unexpected(ReplayError{ReplayError::Kind::Decode, offset, e.error(), std::nullopt});
    return feedDecoded(offset, *e);
}

std::expected<Applied, ReplayError> SecurityReplayer::feedDecoded(int64_t offset, const SecurityEvent& e) {
    if (offset != nextOffset()) {
        ApplyError gap;
        gap.offset = offset; gap.code = ApplyError::Code::OutOfOrder;
        gap.detail = "expected offset " + std::to_string(nextOffset());
        return std::unexpected(ReplayError{ReplayError::Kind::Gap, offset, std::nullopt, std::move(gap)});
    }
    auto a = apply(state_, e, offset);
    if (!a) return std::unexpected(ReplayError{ReplayError::Kind::Apply, offset, std::nullopt, std::move(a.error())});
    return *a;
}

} // namespace rune::security
