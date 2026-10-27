// src/security/ApplyV2.cpp — see ApplyV2.h.
#include "ApplyV2.h"

#include <rune/catalog/CatalogFingerprintV2.h>

#include <utility>

namespace rune::security {

std::expected<Staged, ApplyError> ApplyV2::run(const SecurityState& s, const SecurityEvent& e, int64_t offset) {
    auto st = ApplyV1::run(s, e, offset);   // the shared object rules, namespace-scoped by e.nsId
    if (!st) return st;
    st->applied.fingerprint = rune::catalog::objectFingerprintV2(st->applied.fingerprint, e.nsId);
    return st;
}

Applied ApplyV2::commit(SecurityState& s, const SecurityEvent& e, int64_t offset, Staged&& st) {
    return ApplyV1::commit(s, e, offset, std::move(st));
}

} // namespace rune::security
