// src/catalog/CatalogReplayer.cpp — see CatalogReplayer.h.
#include <rune/catalog/CatalogReplayer.h>

#include <cstdio>
#include <string>
#include <utility>

namespace rune::catalog {

namespace {
std::string hex(uint64_t v) {
    char buf[2 + 16 + 1];
    std::snprintf(buf, sizeof buf, "0x%016llx", static_cast<unsigned long long>(v));
    return buf;
}
} // namespace

std::string_view replayErrorName(ReplayError::Code c) noexcept {
    switch (c) {
    case ReplayError::Code::Decode: return "Decode";
    case ReplayError::Code::Gap:    return "Gap";
    case ReplayError::Code::Apply:  return "Apply";
    }
    return "?";
}

std::string describe(const ReplayError& e) {
    std::string s;
    if (e.offset >= 0) s = "offset " + std::to_string(e.offset) + ": ";
    s += replayErrorName(e.code);
    if (e.decode) { s += ' '; s += decodeErrorName(*e.decode); }
    if (e.apply) {
        s += ' ';
        s += applyErrorName(e.apply->code);
        if (e.apply->code == ApplyError::Code::FingerprintMismatch)
            s += " expected=" + hex(e.apply->expected) + " actual=" + hex(e.apply->actual);
    }
    if (!e.detail.empty()) { s += ' '; s += e.detail; }
    return s;
}

std::expected<Applied, ReplayError> CatalogReplayer::feed(int64_t offset, std::span<const uint8_t> record) {
    auto e = decode(record);
    if (!e)
        return std::unexpected(ReplayError{.offset = offset, .code = ReplayError::Code::Decode, .decode = e.error(),
                                           .apply = std::nullopt,
                                           .detail = "record of " + std::to_string(record.size()) + " bytes"});
    return feedDecoded(offset, *e);
}

std::expected<Applied, ReplayError> CatalogReplayer::feedDecoded(int64_t offset, const CatalogEvent& e) {
    if (offset != nextOffset())
        return std::unexpected(ReplayError{.offset = offset, .code = ReplayError::Code::Gap, .decode = std::nullopt,
                                           .apply = std::nullopt,
                                           .detail = "expected offset " + std::to_string(nextOffset())});
    auto a = apply(state_, e, offset);
    if (!a) {
        std::string detail = a.error().detail;
        return std::unexpected(ReplayError{.offset = offset, .code = ReplayError::Code::Apply, .decode = std::nullopt,
                                           .apply = std::move(a.error()), .detail = std::move(detail)});
    }
    return *a;
}

} // namespace rune::catalog
