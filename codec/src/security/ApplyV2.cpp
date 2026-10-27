// src/security/ApplyV2.cpp — see ApplyV2.h.
#include "ApplyV2.h"

#include <rune/catalog/CatalogFingerprintV2.h>

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>

#include <algorithm>
#include <set>
#include <string>
#include <utility>

namespace rune::security {

namespace {

Applied purgeApplied(const SecurityEvent& e) {
    return Applied{.kind = e.kind, .objectId = 0, .newVersion = 0, .fingerprint = 0, .effect = SessionEffect::None,
                   .affectedUsers = {}, .nsId = e.nsId, .refreshedUsers = {}};
}

/// NamespacePurged.  Resources are NAMED, not id'd, so a purge also removes the cross-namespace grants a NEW namespace of
/// the same name received before this purge applied - fail-closed (fewer permissions), never a leak.  Idempotent: with
/// nothing to erase it stages an empty change.  Users and roles never reference another namespace's roles (a membership
/// stays inside one namespace), so erasing a namespace's roles touches no surviving user's role set.
std::expected<Staged, ApplyError> purge(const SecurityState& s, const SecurityEvent& e, int64_t offset) {
    const auto refuse = [&](Refusal r, std::string d) -> std::expected<Staged, ApplyError> {
        return std::unexpected(ApplyError{.offset = offset, .code = ApplyError::Code::Precondition, .refusal = r,
                                          .expected = 0, .actual = 0, .detail = std::move(d)});
    };
    if (e.nsId == 0) return refuse(Refusal::NamespaceMismatch, "the default namespace cannot be purged");
    if (!validName(e.objectName)) return refuse(Refusal::InvalidName, "invalid namespace name '" + e.objectName + "'");
    if (e.objectId != 0 || e.prevVersion != 0 || e.newVersion != 0)
        return refuse(Refusal::VersionMismatch, "a NamespacePurged event has object id 0 and versions 0");
    const auto strip = [&](std::set<GrantSpec>& gs) {
        return std::erase_if(gs, [&](const GrantSpec& g) { return grantNamesNamespace(g, e.objectName); }) != 0;
    };
    StagedPurge p;
    std::vector<uint64_t> erased, refreshed;
    for (const auto& [id, u] : s.users()) {   // ordered by id: deterministic
        if (u.nsId == e.nsId) { p.eraseUsers.push_back(id); erased.push_back(id); continue; }
        UserObject c = u;
        if (strip(c.grants)) { ++c.version; p.putUsers.push_back(std::move(c)); refreshed.push_back(id); }
    }
    for (const auto& [id, r] : s.roles()) {
        if (r.nsId == e.nsId) { p.eraseRoles.push_back(id); continue; }
        RoleObject c = r;
        if (strip(c.grants)) {
            ++c.version; p.putRoles.push_back(std::move(c));
            for (uint64_t m : s.membersOf(id)) refreshed.push_back(m);
        }
    }
    std::sort(refreshed.begin(), refreshed.end());
    refreshed.erase(std::unique(refreshed.begin(), refreshed.end()), refreshed.end());
    Applied a = purgeApplied(e);
    if (!erased.empty()) { a.effect = SessionEffect::Revoke; a.affectedUsers = erased; a.refreshedUsers = std::move(refreshed); }
    else if (!refreshed.empty()) { a.effect = SessionEffect::Refresh; a.affectedUsers = std::move(refreshed); }
    // Deterministic, state-independent fingerprint of (namespace name): the purge is idempotent.
    uint64_t h = rune::codec::detail::kFnvSeed;
    h = rune::codec::detail::fnv1a_byte(h, 'P');
    h = rune::catalog::detail::fnvBytes(h, e.objectName.data(), e.objectName.size());
    a.fingerprint = h;
    return Staged{std::move(p), std::move(a)};
}

} // namespace

std::expected<Staged, ApplyError> ApplyV2::run(const SecurityState& s, const SecurityEvent& e, int64_t offset) {
    if (e.kind == EventKind::NamespacePurged) {
        auto st = purge(s, e, offset);
        if (st) st->applied.fingerprint = rune::catalog::objectFingerprintV2(st->applied.fingerprint, e.nsId);
        return st;
    }
    auto st = ApplyV1::run(s, e, offset);   // the shared object rules, namespace-scoped by e.nsId
    if (!st) return st;
    st->applied.fingerprint = rune::catalog::objectFingerprintV2(st->applied.fingerprint, e.nsId);
    return st;
}

Applied ApplyV2::commit(SecurityState& s, const SecurityEvent& e, int64_t offset, Staged&& st) {
    return ApplyV1::commit(s, e, offset, std::move(st));
}

} // namespace rune::security
