#pragma once
// Shared fixture for the security tests: Script builds a VALID event against the current state
// (objectId, versions and the fingerprint via previewFingerprint) exactly as the __security leader will.
#include <rune/security/Permissions.h>
#include <rune/security/SecurityFingerprint.h>
#include <rune/security/SecurityState.h>

namespace rs = rune::security;
namespace {
rs::ScramVerifier v(uint8_t seed) {
    rs::ScramVerifier x; x.salt.assign(16, seed); x.iterations = 4096;
    x.storedKey.fill(seed); x.serverKey.fill(static_cast<uint8_t>(seed + 1)); return x;
}
struct Script {
    rs::SecurityState s; int64_t offset = 0;
    rs::SecurityEvent make(rs::Ops ops, std::string name) {
        rs::SecurityEvent e; e.kind = rs::kindOf(ops); e.objectName = std::move(name); e.ops = std::move(ops);
        const rs::UserObject* u = s.user(e.objectName); const rs::RoleObject* r = s.role(e.objectName);
        const bool roleTarget = e.kind == rs::EventKind::CreateRole || e.kind == rs::EventKind::DropRole ||
            ((e.kind == rs::EventKind::Grant || e.kind == rs::EventKind::Revoke) &&
             std::visit([](const auto& o) { if constexpr (requires { o.grantee; }) return o.grantee == rs::GranteeKind::Role; else return false; }, e.ops));
        const bool create = e.kind == rs::EventKind::CreateUser || e.kind == rs::EventKind::CreateRole || e.kind == rs::EventKind::Bootstrap;
        e.objectId = create ? s.nextObjectId() : (roleTarget ? (r ? r->objectId : 0) : (u ? u->objectId : 0));
        e.prevVersion = create ? 0 : (roleTarget ? (r ? r->version : 0) : (u ? u->version : 0));
        e.newVersion = e.prevVersion + 1;
        if (auto f = rs::previewFingerprint(s, e)) e.fingerprint = *f;
        return e;
    }
    std::expected<rs::Applied, rs::ApplyError> run(rs::Ops ops, std::string name) { return rs::apply(s, make(std::move(ops), std::move(name)), offset++); }
};
rs::GrantSpec g(rs::Operation op, rs::ResourceType t, rs::PatternKind k, std::string n) { return {op, {t, k, std::move(n)}}; }
} // namespace
