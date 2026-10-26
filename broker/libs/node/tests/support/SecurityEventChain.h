#pragma once
// SecurityEventChain.h — builds VALID __security events the way the leader does (fill the event, stamp it with
// previewFingerprint, apply it to a private growing state), for tests that need more than the golden script
// (10k-event boots, write-path expectations).  Single-threaded test helper.

#include <rune/security/SecurityEvent.h>
#include <rune/security/SecurityState.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace rune::node::test {

[[nodiscard]] inline rune::security::ScramVerifier testVerifier(uint8_t seed) {
    rune::security::ScramVerifier v;
    v.salt.assign(16, static_cast<uint8_t>(0x10 + seed));
    v.iterations = 4096;
    v.storedKey.fill(static_cast<uint8_t>(0x30 + seed));
    v.serverKey.fill(static_cast<uint8_t>(0x50 + seed));
    return v;
}

class SecurityEventChain {
public:
    /// The next event, targeting `name` (a user, or a role for the role kinds), stamped and applied to the chain's
    /// state.  `principal` = who authenticated.
    rune::security::SecurityEvent emit(rune::security::Ops ops, const std::string& name, std::string statement,
                                       const std::string& principal = "admin") {
        using namespace rune::security;
        const auto i = static_cast<int64_t>(count_);
        SecurityEvent e;
        e.kind = kindOf(ops);
        e.ops = std::move(ops);
        e.objectName = name;
        e.statement = std::move(statement);
        const bool roleTarget = e.kind == EventKind::CreateRole || e.kind == EventKind::DropRole ||
            (e.kind == EventKind::Grant && std::get<GrantOps>(e.ops).grantee == GranteeKind::Role) ||
            (e.kind == EventKind::Revoke && std::get<RevokeOps>(e.ops).grantee == GranteeKind::Role);
        const bool create = e.kind == EventKind::CreateUser || e.kind == EventKind::CreateRole || e.kind == EventKind::Bootstrap;
        if (create) {
            e.objectId = s_.nextObjectId();
            e.prevVersion = 0;
        } else if (roleTarget) {
            const RoleObject* r = s_.roleByName(rune::catalog::kDefaultNsId, name);
            if (!r) throw std::logic_error("SecurityEventChain: no role " + name);
            e.objectId = r->objectId;
            e.prevVersion = r->version;
        } else {
            const UserObject* u = s_.userByName(rune::catalog::kDefaultNsId, name);
            if (!u) throw std::logic_error("SecurityEventChain: no user " + name);
            e.objectId = u->objectId;
            e.prevVersion = u->version;
        }
        e.newVersion = e.prevVersion + 1;
        e.principal.authenticated = principal;
        e.principal.requestId = rune::catalog::RequestId{0xC4A1, count_ + 1};
        e.principal.requestFingerprint = 0xF00D'0000 + count_;
        e.timestampMs = 1'760'000'000'000 + 1000 * i;
        auto fp = previewFingerprint(s_, e);
        if (!fp) throw std::logic_error("SecurityEventChain: " + fp.error().detail);
        e.fingerprint = *fp;
        if (auto a = apply(s_, e, i); !a) throw std::logic_error("SecurityEventChain: " + a.error().detail);
        ++count_;
        return e;
    }
    rune::security::SecurityEvent createUser(const std::string& name) {
        return emit(rune::security::CreateUserOps{testVerifier(static_cast<uint8_t>(count_))}, name,
                    "CREATE USER " + name + " WITH PASSWORD '<redacted>';");
    }
    [[nodiscard]] const rune::security::SecurityState& state() const noexcept { return s_; }
    [[nodiscard]] uint64_t count() const noexcept { return count_; }

private:
    rune::security::SecurityState s_;
    uint64_t count_ = 0;
};

} // namespace rune::node::test
