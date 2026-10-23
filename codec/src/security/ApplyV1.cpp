// src/security/ApplyV1.cpp — the FROZEN formatVer-1 rules.  NEVER edited after the golden log
// pins them; a change ships as formatVer 2 (ApplyV2 beside this file).
#include "ApplyV1.h"
#include <rune/security/SecurityFingerprint.h>

#include <algorithm>
#include <string>
#include <type_traits>
#include <utility>

namespace rune::security {
namespace {

using Err = std::unexpected<ApplyError>;

Err refuse(int64_t offset, Refusal r, std::string detail) {
    return Err(ApplyError{.offset = offset, .code = ApplyError::Code::Precondition, .refusal = r, .expected = 0,
                          .actual = 0, .detail = std::move(detail)});
}

struct Ctx {
    const SecurityState& s;
    const SecurityEvent& e;
    int64_t offset;
    std::string quoted() const { return "'" + e.objectName + "'"; }
    /// Creates: prevVersion 0, newVersion 1, objectId == nextObjectId.  Updates: the object's id and
    /// current version, newVersion = prevVersion + 1.
    std::optional<Err> createVersion() const {
        if (e.prevVersion != 0 || e.newVersion != 1 || e.objectId != s.nextObjectId())
            return refuse(offset, Refusal::VersionMismatch, "create of " + quoted() + " must be version 1 with object id " +
                                                              std::to_string(s.nextObjectId()));
        return std::nullopt;
    }
    std::optional<Err> updateVersion(uint64_t id, uint32_t version) const {
        if (e.objectId != id)
            return refuse(offset, Refusal::VersionMismatch, "object id " + std::to_string(e.objectId) + " is not " + quoted());
        if (e.prevVersion != version || e.newVersion != version + 1)
            return refuse(offset, Refusal::VersionMismatch, quoted() + " is at version " + std::to_string(version) +
                                                              ", not " + std::to_string(e.prevVersion));
        return std::nullopt;
    }
    Applied applied(SessionEffect effect, std::vector<uint64_t> users) const {
        return Applied{.kind = e.kind, .objectId = e.objectId, .newVersion = e.newVersion, .fingerprint = 0,
                       .effect = users.empty() ? SessionEffect::None : effect, .affectedUsers = std::move(users)};
    }
};

std::expected<Staged, ApplyError> finish(const Ctx& c, StagedChange change, Applied a) {
    a.fingerprint = std::visit([&](const auto& ch) -> uint64_t {
        using T = std::decay_t<decltype(ch)>;
        if constexpr (std::is_same_v<T, StagedPutUser>) return fingerprintOf(ch.obj, c.s);
        else if constexpr (std::is_same_v<T, StagedPutRole>) return fingerprintOf(ch.obj);
        else return fingerprintOf(ch.tomb);
    }, change);
    return Staged{std::move(change), std::move(a)};
}

std::expected<Staged, ApplyError> createUser(const Ctx& c, const ScramVerifier& v, bool bootstrap) {
    if (bootstrap && c.s.hasSecurityAdmin())
        return refuse(c.offset, Refusal::AlreadyBootstrapped, "an enabled security administrator already exists");
    if (!validName(c.e.objectName)) return refuse(c.offset, Refusal::InvalidName, "invalid name " + c.quoted());
    if (c.s.user(c.e.objectName)) return refuse(c.offset, Refusal::UserExists, "user " + c.quoted() + " already exists");
    if (auto bad = c.createVersion()) return *bad;
    UserObject u;
    u.objectId = c.e.objectId; u.name = c.e.objectName; u.version = 1; u.enabled = true;
    u.verifiers.push_back(v);
    if (bootstrap) {
        u.mustChangePassword = true;
        u.grants.insert(GrantSpec{Operation::Admin, {ResourceType::Cluster, PatternKind::Exact, {}}});
    }
    return finish(c, StagedPutUser{std::move(u), true}, c.applied(SessionEffect::None, {}));
}

std::expected<Staged, ApplyError> alterUser(const Ctx& c, const AlterUserOps& ops) {
    const UserObject* cur = c.s.user(c.e.objectName);
    if (!cur) return refuse(c.offset, Refusal::UnknownUser, "unknown user " + c.quoted());
    if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
    UserObject u = *cur;
    u.version = c.e.newVersion;
    SessionEffect effect = SessionEffect::None;
    bool affectsUser = false;
    if (const auto* a = std::get_if<AddPassword>(&ops.change)) {
        if (u.verifiers.size() >= 2) return refuse(c.offset, Refusal::TooManyPasswords, "user " + c.quoted() + " already has two passwords");
        u.verifiers.push_back(a->verifier);
    } else if (const auto* d = std::get_if<DropPassword>(&ops.change)) {
        if (d->index < 1 || d->index > u.verifiers.size())
            return refuse(c.offset, Refusal::NoSuchPassword, "user " + c.quoted() + " has no password " + std::to_string(d->index));
        if (u.verifiers.size() < 2) return refuse(c.offset, Refusal::LastPassword, "cannot drop the last password of " + c.quoted());
        u.verifiers.erase(u.verifiers.begin() + (d->index - 1));
    } else if (const auto* sp = std::get_if<SetPassword>(&ops.change)) {
        u.verifiers.assign(1, sp->verifier);
        u.mustChangePassword = false;
        effect = sp->terminateSessions ? SessionEffect::Revoke : SessionEffect::Refresh;
        affectsUser = true;
    } else if (std::holds_alternative<DisableUser>(ops.change)) {
        if (!u.enabled) return refuse(c.offset, Refusal::AlreadyDisabled, "user " + c.quoted() + " is already disabled");
        u.enabled = false;
        effect = SessionEffect::Revoke; affectsUser = true;
    } else {   // EnableUser
        if (u.enabled) return refuse(c.offset, Refusal::AlreadyEnabled, "user " + c.quoted() + " is already enabled");
        u.enabled = true;
    }
    return finish(c, StagedPutUser{std::move(u), false},
                  c.applied(effect, affectsUser ? std::vector<uint64_t>{cur->objectId} : std::vector<uint64_t>{}));
}

std::expected<Staged, ApplyError> dropUser(const Ctx& c) {
    const UserObject* cur = c.s.user(c.e.objectName);
    if (!cur) return refuse(c.offset, Refusal::UnknownUser, "unknown user " + c.quoted());
    if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
    return finish(c, StagedRemoveUser{Tombstone{cur->objectId, false, cur->name, c.e.newVersion}},
                  c.applied(SessionEffect::Revoke, {cur->objectId}));
}

std::expected<Staged, ApplyError> createRole(const Ctx& c) {
    if (!validName(c.e.objectName)) return refuse(c.offset, Refusal::InvalidName, "invalid name " + c.quoted());
    if (c.s.role(c.e.objectName)) return refuse(c.offset, Refusal::RoleExists, "role " + c.quoted() + " already exists");
    if (auto bad = c.createVersion()) return *bad;
    RoleObject r;
    r.objectId = c.e.objectId; r.name = c.e.objectName; r.version = 1;
    return finish(c, StagedPutRole{std::move(r), true}, c.applied(SessionEffect::None, {}));
}

std::expected<Staged, ApplyError> dropRole(const Ctx& c) {
    const RoleObject* cur = c.s.role(c.e.objectName);
    if (!cur) return refuse(c.offset, Refusal::UnknownRole, "unknown role " + c.quoted());
    if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
    std::vector<uint64_t> members = c.s.membersOf(cur->objectId);
    // The staged removal and the answer each get their own list: argument evaluation order is unspecified, so
    // moving `members` into one argument may empty it before the other copies it (the memberships then survive).
    std::vector<uint64_t> affected = members;
    return finish(c, StagedRemoveRole{Tombstone{cur->objectId, true, cur->name, c.e.newVersion}, std::move(members)},
                  c.applied(SessionEffect::Refresh, std::move(affected)));
}

std::expected<Staged, ApplyError> grantOrRevoke(const Ctx& c, GranteeKind grantee, const GrantSpec& g, bool grant) {
    if (grantee == GranteeKind::User) {
        const UserObject* cur = c.s.user(c.e.objectName);
        if (!cur) return refuse(c.offset, Refusal::UnknownUser, "unknown user " + c.quoted());
        if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
        if (!validGrant(g)) return refuse(c.offset, Refusal::InvalidGrant, "invalid grant on " + c.quoted());
        UserObject u = *cur;
        u.version = c.e.newVersion;
        if (grant) {
            if (!u.grants.insert(g).second) return refuse(c.offset, Refusal::AlreadyGranted, c.quoted() + " already holds that grant");
        } else if (u.grants.erase(g) == 0) {
            return refuse(c.offset, Refusal::NotGranted, c.quoted() + " does not hold that grant");
        }
        return finish(c, StagedPutUser{std::move(u), false}, c.applied(SessionEffect::Refresh, {cur->objectId}));
    }
    const RoleObject* cur = c.s.role(c.e.objectName);
    if (!cur) return refuse(c.offset, Refusal::UnknownRole, "unknown role " + c.quoted());
    if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
    if (!validGrant(g)) return refuse(c.offset, Refusal::InvalidGrant, "invalid grant on " + c.quoted());
    RoleObject r = *cur;
    r.version = c.e.newVersion;
    if (grant) {
        if (!r.grants.insert(g).second) return refuse(c.offset, Refusal::AlreadyGranted, "role " + c.quoted() + " already holds that grant");
    } else if (r.grants.erase(g) == 0) {
        return refuse(c.offset, Refusal::NotGranted, "role " + c.quoted() + " does not hold that grant");
    }
    return finish(c, StagedPutRole{std::move(r), false}, c.applied(SessionEffect::Refresh, c.s.membersOf(cur->objectId)));
}

std::expected<Staged, ApplyError> grantOrRevokeRole(const Ctx& c, uint64_t roleId, const std::string& roleName, bool grant) {
    const UserObject* cur = c.s.user(c.e.objectName);
    if (!cur) return refuse(c.offset, Refusal::UnknownUser, "unknown user " + c.quoted());
    const RoleObject* role = c.s.roleById(roleId);
    if (!role || role->name != roleName) return refuse(c.offset, Refusal::UnknownRole, "unknown role '" + roleName + "'");
    if (auto bad = c.updateVersion(cur->objectId, cur->version)) return *bad;
    UserObject u = *cur;
    u.version = c.e.newVersion;
    if (grant) {
        if (!u.roles.insert(roleId).second) return refuse(c.offset, Refusal::AlreadyMember, c.quoted() + " is already a member of '" + roleName + "'");
    } else if (u.roles.erase(roleId) == 0) {
        return refuse(c.offset, Refusal::NotMember, c.quoted() + " is not a member of '" + roleName + "'");
    }
    return finish(c, StagedPutUser{std::move(u), false}, c.applied(SessionEffect::Refresh, {cur->objectId}));
}

} // namespace

std::expected<Staged, ApplyError> ApplyV1::run(const SecurityState& s, const SecurityEvent& e, int64_t offset) {
    const Ctx c{s, e, offset};
    return std::visit([&](const auto& o) -> std::expected<Staged, ApplyError> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateUserOps>) return createUser(c, o.verifier, false);
        else if constexpr (std::is_same_v<T, BootstrapOps>) return createUser(c, o.verifier, true);
        else if constexpr (std::is_same_v<T, AlterUserOps>) return alterUser(c, o);
        else if constexpr (std::is_same_v<T, DropUserOps>) return dropUser(c);
        else if constexpr (std::is_same_v<T, CreateRoleOps>) return createRole(c);
        else if constexpr (std::is_same_v<T, DropRoleOps>) return dropRole(c);
        else if constexpr (std::is_same_v<T, GrantOps>) return grantOrRevoke(c, o.grantee, o.grant, true);
        else if constexpr (std::is_same_v<T, RevokeOps>) return grantOrRevoke(c, o.grantee, o.grant, false);
        else if constexpr (std::is_same_v<T, GrantRoleOps>) return grantOrRevokeRole(c, o.roleId, o.roleName, true);
        else return grantOrRevokeRole(c, o.roleId, o.roleName, false);   // RevokeRoleOps
    }, e.ops);
}

Applied ApplyV1::commit(SecurityState& s, const SecurityEvent& e, int64_t offset, Staged&& st) {
    std::visit([&](auto& ch) {
        using T = std::decay_t<decltype(ch)>;
        if constexpr (std::is_same_v<T, StagedPutUser>) {
            if (ch.created) { s.userNames_[ch.obj.name] = ch.obj.objectId; s.nextObjectId_ = std::max(s.nextObjectId_, ch.obj.objectId + 1); }
            s.users_.insert_or_assign(ch.obj.objectId, std::move(ch.obj));
        } else if constexpr (std::is_same_v<T, StagedPutRole>) {
            if (ch.created) { s.roleNames_[ch.obj.name] = ch.obj.objectId; s.nextObjectId_ = std::max(s.nextObjectId_, ch.obj.objectId + 1); }
            s.roles_.insert_or_assign(ch.obj.objectId, std::move(ch.obj));
        } else if constexpr (std::is_same_v<T, StagedRemoveUser>) {
            s.users_.erase(ch.tomb.objectId);
            s.userNames_.erase(ch.tomb.name);
            s.tombstones_.push_back(std::move(ch.tomb));
        } else {   // StagedRemoveRole
            for (uint64_t m : ch.members) s.users_.at(m).roles.erase(ch.tomb.objectId);
            s.roles_.erase(ch.tomb.objectId);
            s.roleNames_.erase(ch.tomb.name);
            s.tombstones_.push_back(std::move(ch.tomb));
        }
    }, st.change);
    if (const auto* b = std::get_if<BootstrapOps>(&e.ops); b && !s.fakeSaltSeed_) s.fakeSaltSeed_ = b->fakeSaltSeed;   // D8: the first one wins
    s.appliedOffset_ = offset;
    if (!e.principal.requestId.none())
        s.requests_.try_emplace(e.principal.requestId,
                                RequestRecord{offset, e.kind, e.objectId, e.newVersion, e.principal.requestFingerprint});
    return std::move(st.applied);
}

// ── the public entry points (formatVer dispatch) ─────────────────────────────
namespace {
std::optional<ApplyError> checkWellFormed(const SecurityEvent& e, int64_t offset) {
    if (e.formatVer != kFormatV1)
        return ApplyError{.offset = offset, .code = ApplyError::Code::UnsupportedFormat, .refusal = std::nullopt,
                          .expected = 0, .actual = 0,
                          .detail = "formatVer " + std::to_string(e.formatVer) + " has no apply rules"};
    if (auto bad = validateEvent(e))
        return ApplyError{.offset = offset, .code = ApplyError::Code::BadOps, .refusal = std::nullopt, .expected = 0,
                          .actual = 0, .detail = std::string("malformed event: ") + std::string(encodeErrorName(*bad))};
    return std::nullopt;
}
} // namespace

std::expected<Applied, ApplyError> apply(SecurityState& s, const SecurityEvent& e, int64_t offset) {
    if (auto bad = checkWellFormed(e, offset)) return std::unexpected(std::move(*bad));
    if (offset <= s.appliedOffset())
        return std::unexpected(ApplyError{.offset = offset, .code = ApplyError::Code::OutOfOrder, .refusal = std::nullopt,
                                          .expected = 0, .actual = 0,
                                          .detail = "offset " + std::to_string(offset) + " is not past the applied offset " +
                                                    std::to_string(s.appliedOffset())});
    auto staged = ApplyV1::run(s, e, offset);
    if (!staged) return std::unexpected(std::move(staged.error()));
    const uint64_t actual = staged->applied.fingerprint;
    if (actual != e.fingerprint)
        return std::unexpected(ApplyError{.offset = offset, .code = ApplyError::Code::FingerprintMismatch, .refusal = std::nullopt,
                                          .expected = e.fingerprint, .actual = actual,
                                          .detail = std::string(kindName(e.kind)) + " '" + e.objectName + "'"});
    return ApplyV1::commit(s, e, offset, std::move(*staged));
}

std::expected<Applied, ApplyError> previewApplied(const SecurityState& s, const SecurityEvent& e) {
    if (auto bad = checkWellFormed(e, -1)) return std::unexpected(std::move(*bad));
    auto staged = ApplyV1::run(s, e, -1);
    if (!staged) return std::unexpected(std::move(staged.error()));
    return std::move(staged->applied);
}

std::expected<uint64_t, ApplyError> previewFingerprint(const SecurityState& s, const SecurityEvent& e) {
    auto a = previewApplied(s, e);
    if (!a) return std::unexpected(std::move(a.error()));
    return a->fingerprint;
}

} // namespace rune::security
