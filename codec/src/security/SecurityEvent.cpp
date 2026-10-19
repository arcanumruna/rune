// src/security/SecurityEvent.cpp — the frozen formatVer-1 __security event encoding
// (layout: include/rune/security/SecurityEvent.h).  One validation, run by BOTH directions.
#include <rune/security/SecurityEvent.h>
#include "../catalog/Wire.h"

#include <new>
#include <type_traits>
#include <utility>

namespace rune::security {
namespace {

namespace wire = rune::catalog::wire;
using wire::Reader;
using wire::Writer;

enum class Violation : uint8_t { UnknownFormatVer, UnknownKind, KindOpsMismatch, BadEnum, BadVerifier };

constexpr bool validOperation(Operation o) noexcept { return static_cast<uint8_t>(o) <= static_cast<uint8_t>(Operation::Admin); }
constexpr bool validResourceType(ResourceType t) noexcept { return static_cast<uint8_t>(t) <= static_cast<uint8_t>(ResourceType::TransactionalId); }
constexpr bool validPatternKind(PatternKind k) noexcept { return static_cast<uint8_t>(k) <= static_cast<uint8_t>(PatternKind::Prefix); }
constexpr bool validGrantee(GranteeKind g) noexcept { return static_cast<uint8_t>(g) <= static_cast<uint8_t>(GranteeKind::Role); }
constexpr bool validSaltLen(std::size_t n) noexcept { return n >= kSaltMin && n <= kSaltMax; }
constexpr bool validIterations(uint32_t n) noexcept { return n >= kIterMin && n <= kIterMax; }
bool validVerifier(const ScramVerifier& v) noexcept { return validSaltLen(v.salt.size()) && validIterations(v.iterations); }
bool validGrantSpec(const GrantSpec& g) noexcept {
    return validOperation(g.op) && validResourceType(g.resource.type) && validPatternKind(g.resource.kind);
}

std::optional<Violation> checkOps(const Ops& ops) noexcept {
    return std::visit([](const auto& o) -> std::optional<Violation> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateUserOps> || std::is_same_v<T, BootstrapOps>) {
            if (!validVerifier(o.verifier)) return Violation::BadVerifier;
        } else if constexpr (std::is_same_v<T, AlterUserOps>) {
            if (const auto* a = std::get_if<AddPassword>(&o.change); a && !validVerifier(a->verifier)) return Violation::BadVerifier;
            if (const auto* s = std::get_if<SetPassword>(&o.change); s && !validVerifier(s->verifier)) return Violation::BadVerifier;
        } else if constexpr (std::is_same_v<T, GrantOps> || std::is_same_v<T, RevokeOps>) {
            if (!validGrantee(o.grantee) || !validGrantSpec(o.grant)) return Violation::BadEnum;
        }
        return std::nullopt;
    }, ops);
}
std::optional<Violation> checkFormatVer(uint16_t v) noexcept {
    return v == kFormatV1 ? std::nullopt : std::optional<Violation>(Violation::UnknownFormatVer);
}
std::optional<Violation> checkKind(uint16_t k) noexcept {
    return (k >= static_cast<uint16_t>(EventKind::CreateUser) && k <= static_cast<uint16_t>(EventKind::Bootstrap))
        ? std::nullopt : std::optional<Violation>(Violation::UnknownKind);
}
std::optional<Violation> validate(const SecurityEvent& e) noexcept {
    if (auto v = checkFormatVer(e.formatVer)) return v;
    if (auto v = checkKind(static_cast<uint16_t>(e.kind))) return v;
    if (e.kind != kindOf(e.ops)) return Violation::KindOpsMismatch;
    return checkOps(e.ops);
}
DecodeError asDecodeError(Violation v) noexcept {
    switch (v) {
    case Violation::UnknownFormatVer: return DecodeError::UnknownFormatVer;
    case Violation::UnknownKind:      return DecodeError::UnknownKind;
    case Violation::KindOpsMismatch:  return DecodeError::KindOpsMismatch;
    case Violation::BadEnum:          return DecodeError::BadEnum;
    case Violation::BadVerifier:      return DecodeError::BadVerifier;
    }
    return DecodeError::BadEnum;
}
EncodeError asEncodeError(Violation v) noexcept {
    switch (v) {
    case Violation::UnknownFormatVer: return EncodeError::UnknownFormatVer;
    case Violation::UnknownKind:      return EncodeError::UnknownKind;
    case Violation::KindOpsMismatch:  return EncodeError::KindOpsMismatch;
    case Violation::BadEnum:          return EncodeError::BadEnum;
    case Violation::BadVerifier:      return EncodeError::BadVerifier;
    }
    return EncodeError::BadEnum;
}

// ── body writers ────────────────────────────────────────────────────────────
void putVerifier(Writer& w, const ScramVerifier& v) {
    w.u8(static_cast<uint8_t>(v.salt.size())); w.raw(v.salt); w.u32(v.iterations);
    w.raw(v.storedKey); w.raw(v.serverKey);
}
void putGrant(Writer& w, const GrantSpec& g) {
    w.e8(g.op); w.e8(g.resource.type); w.e8(g.resource.kind); w.str16(g.resource.name);
}
void putOps(Writer& w, const Ops& ops) {
    w.u8(static_cast<uint8_t>(kindOf(ops)));   // the tag = the kind, exactly as __catalog (frozen)
    std::visit([&](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateUserOps>) {
            putVerifier(w, o.verifier);
        } else if constexpr (std::is_same_v<T, BootstrapOps>) {
            putVerifier(w, o.verifier); w.raw(o.fakeSaltSeed);
        } else if constexpr (std::is_same_v<T, AlterUserOps>) {
            w.u8(static_cast<uint8_t>(o.change.index()));   // the change tag = UserChangeKind (0-based, frozen)
            std::visit([&](const auto& c) {
                using C = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<C, AddPassword>) putVerifier(w, c.verifier);
                else if constexpr (std::is_same_v<C, DropPassword>) w.u8(c.index);
                else if constexpr (std::is_same_v<C, SetPassword>) { putVerifier(w, c.verifier); w.u8(c.terminateSessions ? 1 : 0); }
            }, o.change);
        } else if constexpr (std::is_same_v<T, GrantOps> || std::is_same_v<T, RevokeOps>) {
            w.e8(o.grantee); putGrant(w, o.grant);
        } else if constexpr (std::is_same_v<T, GrantRoleOps> || std::is_same_v<T, RevokeRoleOps>) {
            w.u64(o.roleId); w.str16(o.roleName);
        }   // DropUser / CreateRole / DropRole: empty body
    }, ops);
}

// ── body readers (parse only; ranges are judged afterwards by validate()) ───
std::optional<DecodeError> getBool(Reader& r, bool& b) {
    uint8_t v; if (!r.u8(v)) return DecodeError::Truncated;
    if (v > 1) return DecodeError::BadEnum;
    b = v == 1; return std::nullopt;
}
std::optional<DecodeError> getVerifier(Reader& r, ScramVerifier& v) {
    uint8_t n; if (!r.u8(n)) return DecodeError::Truncated;
    if (!validSaltLen(n)) return DecodeError::BadVerifier;   // judged before any byte is read
    v.salt.resize(n);
    if (!r.raw(v.salt.data(), n) || !r.u32(v.iterations)) return DecodeError::Truncated;
    if (!validIterations(v.iterations)) return DecodeError::BadVerifier;
    if (!r.raw(v.storedKey.data(), kKeyBytes) || !r.raw(v.serverKey.data(), kKeyBytes)) return DecodeError::Truncated;
    return std::nullopt;
}
bool getGrant(Reader& r, GrantSpec& g) {
    return r.e8(g.op) && r.e8(g.resource.type) && r.e8(g.resource.kind) && r.str16(g.resource.name);
}
std::expected<Ops, DecodeError> getOps(Reader& r, EventKind kind) {
    uint8_t tag; if (!r.u8(tag)) return std::unexpected(DecodeError::Truncated);
    if (tag != static_cast<uint8_t>(kind)) return std::unexpected(DecodeError::KindOpsMismatch);
    switch (kind) {
    case EventKind::CreateUser: { CreateUserOps o; if (auto e = getVerifier(r, o.verifier)) return std::unexpected(*e); return o; }
    case EventKind::Bootstrap: {
        BootstrapOps o;
        if (auto e = getVerifier(r, o.verifier)) return std::unexpected(*e);
        if (!r.raw(o.fakeSaltSeed.data(), o.fakeSaltSeed.size())) return std::unexpected(DecodeError::Truncated);
        return o;
    }
    case EventKind::AlterUser: {
        uint8_t ct; if (!r.u8(ct)) return std::unexpected(DecodeError::Truncated);
        AlterUserOps o;
        switch (ct) {
        case 0: { AddPassword c; if (auto e = getVerifier(r, c.verifier)) return std::unexpected(*e); o.change = std::move(c); break; }
        case 1: { DropPassword c; if (!r.u8(c.index)) return std::unexpected(DecodeError::Truncated); o.change = c; break; }
        case 2: {
            SetPassword c;
            if (auto e = getVerifier(r, c.verifier)) return std::unexpected(*e);
            if (auto e = getBool(r, c.terminateSessions)) return std::unexpected(*e);
            o.change = std::move(c); break;
        }
        case 3: o.change = DisableUser{}; break;
        case 4: o.change = EnableUser{}; break;
        default: return std::unexpected(DecodeError::BadEnum);
        }
        return o;
    }
    case EventKind::DropUser:   return Ops{DropUserOps{}};
    case EventKind::CreateRole: return Ops{CreateRoleOps{}};
    case EventKind::DropRole:   return Ops{DropRoleOps{}};
    case EventKind::Grant: {
        GrantOps o; if (!r.e8(o.grantee) || !getGrant(r, o.grant)) return std::unexpected(DecodeError::Truncated); return o;
    }
    case EventKind::Revoke: {
        RevokeOps o; if (!r.e8(o.grantee) || !getGrant(r, o.grant)) return std::unexpected(DecodeError::Truncated); return o;
    }
    case EventKind::GrantRole: {
        GrantRoleOps o; if (!r.u64(o.roleId) || !r.str16(o.roleName)) return std::unexpected(DecodeError::Truncated); return o;
    }
    case EventKind::RevokeRole: {
        RevokeRoleOps o; if (!r.u64(o.roleId) || !r.str16(o.roleName)) return std::unexpected(DecodeError::Truncated); return o;
    }
    }
    return std::unexpected(DecodeError::UnknownKind);   // unreachable: the kind was validated first
}

} // namespace

std::optional<EncodeError> validateEvent(const SecurityEvent& e) noexcept {
    if (auto v = validate(e)) return asEncodeError(*v);
    return std::nullopt;
}

EventKind kindOf(const Ops& ops) noexcept { return static_cast<EventKind>(ops.index() + 1); }

std::expected<void, EncodeError> encodeInto(const SecurityEvent& e, std::vector<uint8_t>& out) {
    out.clear();
    if (auto v = validate(e)) return std::unexpected(asEncodeError(*v));   // the decoder's own rules
    Writer w(out);
    w.u16(e.formatVer); w.u16(static_cast<uint16_t>(e.kind)); w.u64(e.objectId);
    w.u32(e.prevVersion); w.u32(e.newVersion); w.i64(e.timestampMs); w.u64(e.fingerprint);
    w.str16(e.objectName); w.str32(e.statement);
    wire::writePrincipalAndReplay(w, e.principal, e.replayedFrom);
    const std::size_t lenAt = w.size();
    w.u32(0);
    putOps(w, e.ops);
    w.patchLen32(lenAt);
    if (w.error()) {   // never leave a truncated, undecodable prefix behind
        out.clear();
        return std::unexpected(EncodeError::FieldTooLong);
    }
    return {};
}

std::expected<std::vector<uint8_t>, EncodeError> encode(const SecurityEvent& e) {
    std::vector<uint8_t> b;
    if (auto r = encodeInto(e, b); !r) return std::unexpected(r.error());
    return b;
}

std::expected<SecurityEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept {
    try {
        Reader r(bytes);
        SecurityEvent e;
        uint16_t kind;
        // formatVer and kind decide how the rest is parsed: judged before anything else is read.
        if (!r.u16(e.formatVer)) return std::unexpected(DecodeError::Truncated);
        if (auto v = checkFormatVer(e.formatVer)) return std::unexpected(asDecodeError(*v));
        if (!r.u16(kind)) return std::unexpected(DecodeError::Truncated);
        if (auto v = checkKind(kind)) return std::unexpected(asDecodeError(*v));
        e.kind = static_cast<EventKind>(kind);
        if (!r.u64(e.objectId) || !r.u32(e.prevVersion) || !r.u32(e.newVersion) || !r.i64(e.timestampMs) ||
            !r.u64(e.fingerprint) || !r.str16(e.objectName) || !r.str32(e.statement))
            return std::unexpected(DecodeError::Truncated);
        if (auto err = wire::readPrincipalAndReplay(r, e.principal, e.replayedFrom))
            return std::unexpected(*err == rune::catalog::DecodeError::BadEnum ? DecodeError::BadEnum : DecodeError::Truncated);
        uint32_t opsLen; Reader ops(std::span<const uint8_t>{});
        if (!r.u32(opsLen) || !r.sub(opsLen, ops)) return std::unexpected(DecodeError::Truncated);
        auto o = getOps(ops, e.kind);
        if (!o) return std::unexpected(o.error());
        if (ops.rem() != 0 || r.rem() != 0) return std::unexpected(DecodeError::TrailingBytes);
        e.ops = std::move(*o);
        if (auto v = validate(e)) return std::unexpected(asDecodeError(*v));   // the encoder's own rules
        return e;
    } catch (const std::bad_alloc&) {
        return std::unexpected(DecodeError::OutOfMemory);
    }
}

std::string_view kindName(EventKind k) noexcept {
    switch (k) {
    case EventKind::CreateUser: return "CreateUser"; case EventKind::AlterUser: return "AlterUser";
    case EventKind::DropUser: return "DropUser";     case EventKind::CreateRole: return "CreateRole";
    case EventKind::DropRole: return "DropRole";     case EventKind::Grant: return "Grant";
    case EventKind::Revoke: return "Revoke";         case EventKind::GrantRole: return "GrantRole";
    case EventKind::RevokeRole: return "RevokeRole"; case EventKind::Bootstrap: return "Bootstrap";
    }
    return "?";
}
std::string_view decodeErrorName(DecodeError e) noexcept {
    switch (e) {
    case DecodeError::Truncated: return "Truncated"; case DecodeError::UnknownFormatVer: return "UnknownFormatVer";
    case DecodeError::UnknownKind: return "UnknownKind"; case DecodeError::KindOpsMismatch: return "KindOpsMismatch";
    case DecodeError::BadEnum: return "BadEnum"; case DecodeError::BadVerifier: return "BadVerifier";
    case DecodeError::TrailingBytes: return "TrailingBytes"; case DecodeError::OutOfMemory: return "OutOfMemory";
    }
    return "?";
}
std::string_view encodeErrorName(EncodeError e) noexcept {
    switch (e) {
    case EncodeError::KindOpsMismatch: return "KindOpsMismatch"; case EncodeError::FieldTooLong: return "FieldTooLong";
    case EncodeError::BadVerifier: return "BadVerifier"; case EncodeError::UnknownFormatVer: return "UnknownFormatVer";
    case EncodeError::UnknownKind: return "UnknownKind"; case EncodeError::BadEnum: return "BadEnum";
    }
    return "?";
}
std::string_view operationName(Operation o) noexcept {
    switch (o) {
    case Operation::Read: return "READ"; case Operation::Write: return "WRITE"; case Operation::Create: return "CREATE";
    case Operation::Alter: return "ALTER"; case Operation::Drop: return "DROP"; case Operation::Describe: return "DESCRIBE";
    case Operation::Admin: return "ADMIN";
    }
    return "?";
}
std::string_view resourceTypeName(ResourceType t) noexcept {
    switch (t) {
    case ResourceType::Cluster: return "CLUSTER"; case ResourceType::Topic: return "TOPIC";
    case ResourceType::View: return "VIEW"; case ResourceType::Group: return "GROUP";
    case ResourceType::TransactionalId: return "TRANSACTIONAL_ID";
    }
    return "?";
}

namespace detail_testing {
std::size_t opsBodySize(const Ops& ops) {
    std::vector<uint8_t> b;
    Writer w(b);
    putOps(w, ops);
    return b.size() - 1;   // minus the tag byte
}
} // namespace detail_testing

} // namespace rune::security
