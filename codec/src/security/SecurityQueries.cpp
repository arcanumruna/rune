// src/security/SecurityQueries.cpp — see rune/security/SecurityQueries.h.
#include <rune/security/SecurityQueries.h>
#include <rune/security/Permissions.h>

#include <algorithm>

namespace rune::security {

namespace {
/// Which kind of grantee/object the event is about.
GranteeKind subjectOf(const SecurityEvent& e) noexcept {
    switch (e.kind) {
    case EventKind::CreateRole: case EventKind::DropRole: return GranteeKind::Role;
    case EventKind::Grant:  return std::get<GrantOps>(e.ops).grantee;
    case EventKind::Revoke: return std::get<RevokeOps>(e.ops).grantee;
    default: return GranteeKind::User;
    }
}
std::vector<std::string> sortedUserNames(const SecurityState& s, const std::vector<uint64_t>& ids) {
    std::vector<std::string> out;
    for (uint64_t id : ids) if (const UserObject* u = s.userById(id)) out.push_back(u->name);
    std::sort(out.begin(), out.end());
    return out;
}
} // namespace

std::vector<RecordedEvent> history(std::span<const RecordedEvent> log, const HistoryFilter& f) {
    std::vector<RecordedEvent> out;
    for (const RecordedEvent& r : log) {
        if (f.kind && subjectOf(r.event) != *f.kind) continue;
        if (!f.name.empty() && r.event.objectName != f.name) continue;
        if (f.nsId && r.event.nsId != *f.nsId) continue;
        if (f.sinceMs && r.event.timestampMs < *f.sinceMs) continue;
        out.push_back(r);
    }
    return out;
}

std::expected<SecurityState, ReplayError> stateAsOf(std::span<const RecordedEvent> log, const AsOf& at) {
    SecurityReplayer r;
    for (const RecordedEvent& rec : log) {
        const bool past = at.by == AsOf::By::Offset ? rec.offset > at.value : rec.event.timestampMs > at.value;
        if (past) break;
        if (auto a = r.feedDecoded(rec.offset, rec.event); !a) return std::unexpected(std::move(a.error()));
    }
    return r.state();
}

std::optional<GrantsView> grantsFor(const SecurityState& s, GranteeKind kind, std::string_view name, uint32_t nsId) {
    GrantsView v;
    if (kind == GranteeKind::Role) {
        const RoleObject* r = s.roleByName(nsId, name);
        if (!r) return std::nullopt;
        v.direct.assign(r->grants.begin(), r->grants.end());
        v.effective = v.direct;
        return v;
    }
    const UserObject* u = s.userByName(nsId, name);
    if (!u) return std::nullopt;
    v.direct.assign(u->grants.begin(), u->grants.end());
    for (uint64_t rid : u->roles)
        if (const RoleObject* r = s.roleById(rid))
            v.viaRoles.emplace_back(r->name, std::vector<GrantSpec>(r->grants.begin(), r->grants.end()));
    std::sort(v.viaRoles.begin(), v.viaRoles.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    v.effective = effectiveGrants(s, u->objectId);
    return v;
}

std::vector<UserRow> users(const SecurityState& s) {
    std::vector<UserRow> out;
    for (const auto& [id, u] : s.users()) {
        UserRow row{u.name, u.enabled, u.mustChangePassword, static_cast<uint8_t>(u.verifiers.size()), {}, u.version, u.nsId};
        for (uint64_t rid : u.roles) if (const RoleObject* r = s.roleById(rid)) row.roles.push_back(r->name);
        std::sort(row.roles.begin(), row.roles.end());
        out.push_back(std::move(row));
    }
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) { return a.nsId != b.nsId ? a.nsId < b.nsId : a.name < b.name; });
    return out;
}

std::vector<RoleRow> roles(const SecurityState& s) {
    std::vector<RoleRow> out;
    for (const auto& [id, r] : s.roles())
        out.push_back(RoleRow{r.name, sortedUserNames(s, s.membersOf(id)), r.grants.size(), r.version, r.nsId});
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) { return a.nsId != b.nsId ? a.nsId < b.nsId : a.name < b.name; });
    return out;
}

} // namespace rune::security
