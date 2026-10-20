#pragma once
// SecurityMessages.h — security DDL (79) and the one-time bootstrap (80), client version 9
// (spec §5, D3/D4/D36).  Both are answered by DdlResponse (78) (`objectId` = the user / role
// id, `newVersion`, detail).  The statement carried in the DdlExtension is the REDACTED one
// (every password literal replaced by '<redacted>'): no password ever travels.
//
// Layouts (LE; encoders return 0 when a field does not fit, decoders are bounds-checked
// and return nullopt for out-of-range enum bytes / sizes; string_views point into `buf`):
//
//   SecurityDdl (79)        header | ext (catalog DdlExtension layout) | kind u8 | name var16 | per-kind:
//     CreateUser            verifier
//     AlterUser             change u8 | DropPassword: passwordIndex u8
//                                     | SetPassword: terminateSessions u8, verifier
//                                     | AddPassword: verifier | Disable / Enable: -
//     DropUser, CreateRole, DropRole   -
//     Grant, Revoke         granteeKind u8, op u8, resourceType u8, patternKind u8, pattern var16
//     GrantRole, RevokeRole roleName var16
//   then an OPTIONAL trailer (absent = flags 0; a pre-trailer decoder ignores it):
//                           flags u8 (kSecurityDdlFlag*; unknown bits refused) | credentialProof 64 when flags has
//                           kSecurityDdlFlagCredentialProof
//   verifier                salt var16 | iterations u32 | storedKey 64 raw bytes | serverKey 64 raw bytes
//   SecurityBootstrap (80)  header | adminName var16 | verifier | fakeSaltSeed 32 | nonce 16 | mac 64   (D4, D8)
//   Grant / Revoke enum bytes are range-checked on decode (SecurityTypes.h).

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>
#include <vector>
#include "AuthMessages.h"
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"
#include "SchemaMessages.h"
#include "../SecurityTypes.h"

namespace rune::protocol::client {

enum class SecurityOpKind : uint8_t {
    CreateUser = 1, AlterUser = 2, DropUser = 3, CreateRole = 4, DropRole = 5,
    Grant = 6, Revoke = 7, GrantRole = 8, RevokeRole = 9
};
enum class UserChangeKind : uint8_t { AddPassword = 0, DropPassword = 1, SetPassword = 2, Disable = 3, Enable = 4 };

inline constexpr std::size_t kVerifierKeyBytes     = 64;   // SHA-512 StoredKey / ServerKey
inline constexpr std::size_t kBootstrapSeedBytes   = 32;   // the cluster-wide fake-salt seed (D8), replicated in the Bootstrap event
inline constexpr std::size_t kBootstrapNonceBytes  = 16;
inline constexpr std::size_t kBootstrapMacBytes    = 64;   // HMAC-SHA-512
inline constexpr std::size_t kSecurityPatternMax   = 249;  // a topic / view / group name bound
inline constexpr std::size_t kCredentialProofBytes = 64;   // SCRAM-SHA-512 ClientProof size

/// SecurityDdl trailer flags.
///   Force (issue #121): the statement's FORCE - remove the last enabled security admin anyway (the bootstrap window
///     reopens, D3 recovery).
///   CredentialProof (issue #114): a fresh proof of a CURRENT password of the session's own user rides along
///     (credentialChangeMessage); required for a user's own ADD / DROP / SET PASSWORD without ADMIN.
inline constexpr uint8_t kSecurityDdlFlagForce           = 0x01;
inline constexpr uint8_t kSecurityDdlFlagCredentialProof = 0x02;
inline constexpr uint8_t kSecurityDdlKnownFlags          = kSecurityDdlFlagForce | kSecurityDdlFlagCredentialProof;

struct VerifierWire {
    std::string_view salt;
    uint32_t         iterations = 0;
    std::string_view storedKey, serverKey;   // exactly 64 bytes each
    bool operator==(const VerifierWire&) const = default;
};

struct SecurityDdl {
    int64_t sessionId = 0, correlationId = 0;
    DdlExtension ext;                     // statement (REDACTED), clientName, requestId, replayedFrom
    SecurityOpKind kind = SecurityOpKind::CreateUser;
    std::string_view name;                // the user / role the op targets (Grant/Revoke: the grantee)
    UserChangeKind change = UserChangeKind::AddPassword;  // AlterUser only
    uint8_t passwordIndex = 0;            // DropPassword
    uint8_t terminateSessions = 0;        // SetPassword
    VerifierWire verifier;                // CreateUser, AddPassword, SetPassword
    uint8_t granteeKind = 0, op = 0, resourceType = 0, patternKind = 0;   // Grant / Revoke
    std::string_view pattern;             // Grant / Revoke
    std::string_view roleName;            // GrantRole / RevokeRole
    uint8_t flags = 0;                    // the trailer (kSecurityDdlFlag*); 0 = no trailer on the wire
    std::string_view credentialProof;     // kCredentialProofBytes raw bytes when flags has CredentialProof

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::SecurityDdl);
    static constexpr uint16_t BODY_FIXED  = 0;

    /// True when the kind carries a verifier.
    [[nodiscard]] bool hasVerifier() const noexcept {
        return kind == SecurityOpKind::CreateUser ||
               (kind == SecurityOpKind::AlterUser &&
                (change == UserChangeKind::AddPassword || change == UserChangeKind::SetPassword));
    }

    [[nodiscard]] std::size_t encodedSize() const noexcept;
    std::size_t encode(uint8_t* buf) const noexcept;
    static std::optional<SecurityDdl> decode(const uint8_t* buf, std::size_t avail) noexcept;
};

struct SecurityBootstrap {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view adminName;
    VerifierWire verifier;
    std::array<uint8_t, kBootstrapSeedBytes>  fakeSaltSeed{};   // D8: random, generated by the CLI
    std::array<uint8_t, kBootstrapNonceBytes> nonce{};
    std::array<uint8_t, kBootstrapMacBytes>   mac{};   // D4

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::SecurityBootstrap);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept;
    std::size_t encode(uint8_t* buf) const noexcept;
    static std::optional<SecurityBootstrap> decode(const uint8_t* buf, std::size_t avail) noexcept;
};

namespace detail {

[[nodiscard]] inline bool verifierFits(const VerifierWire& v) noexcept {
    return fitsVarLen16(v.salt.size()) && v.storedKey.size() == kVerifierKeyBytes &&
           v.serverKey.size() == kVerifierKeyBytes;
}
[[nodiscard]] constexpr std::size_t verifierEncodedSize(const VerifierWire& v) noexcept {
    return 2u + v.salt.size() + 4u + 2u * kVerifierKeyBytes;
}
inline std::size_t putVerifier(uint8_t* buf, const VerifierWire& v) noexcept {
    std::size_t pos = putVarStr(buf, v.salt);
    putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(v.iterations)); pos += 4;
    std::memcpy(buf + pos, v.storedKey.data(), kVerifierKeyBytes); pos += kVerifierKeyBytes;
    std::memcpy(buf + pos, v.serverKey.data(), kVerifierKeyBytes); pos += kVerifierKeyBytes;
    return pos;
}
/// Reads and bounds-checks (salt 16..64, iterations 4096..1e6) a verifier.
inline bool getVerifier(const uint8_t* buf, std::size_t avail, std::size_t& pos, VerifierWire& out) noexcept {
    VerifierWire v;
    if (!getVarStr(buf, avail, pos, v.salt) || !authSaltOk(v.salt.size())) return false;
    if (pos + 4u + 2u * kVerifierKeyBytes > avail) return false;
    v.iterations = static_cast<uint32_t>(getLE32(buf, static_cast<int>(pos))); pos += 4;
    if (!authIterationsOk(v.iterations)) return false;
    v.storedKey = std::string_view(reinterpret_cast<const char*>(buf + pos), kVerifierKeyBytes); pos += kVerifierKeyBytes;
    v.serverKey = std::string_view(reinterpret_cast<const char*>(buf + pos), kVerifierKeyBytes); pos += kVerifierKeyBytes;
    out = v;
    return true;
}
[[nodiscard]] constexpr bool securityNameOk(std::size_t n) noexcept { return n >= 1 && n <= kAuthNameMax; }
} // namespace detail

inline std::size_t SecurityDdl::encodedSize() const noexcept {
    std::size_t n = MessageHeader::ENCODED_SIZE + BODY_FIXED + extEncodedSize(ext) + 1u + 2u + name.size();
    switch (kind) {
        case SecurityOpKind::CreateUser: n += detail::verifierEncodedSize(verifier); break;
        case SecurityOpKind::AlterUser:
            n += 1u;
            if (change == UserChangeKind::DropPassword) n += 1u;
            else if (change == UserChangeKind::SetPassword) n += 1u + detail::verifierEncodedSize(verifier);
            else if (change == UserChangeKind::AddPassword) n += detail::verifierEncodedSize(verifier);
            break;
        case SecurityOpKind::Grant: case SecurityOpKind::Revoke: n += 4u + 2u + pattern.size(); break;
        case SecurityOpKind::GrantRole: case SecurityOpKind::RevokeRole: n += 2u + roleName.size(); break;
        default: break;
    }
    if (flags != 0) n += 1u + ((flags & kSecurityDdlFlagCredentialProof) != 0 ? kCredentialProofBytes : 0u);
    return n;
}

inline std::size_t SecurityDdl::encode(uint8_t* buf) const noexcept {
    const auto k = static_cast<uint8_t>(kind);
    if (k < static_cast<uint8_t>(SecurityOpKind::CreateUser) || k > static_cast<uint8_t>(SecurityOpKind::RevokeRole)) return 0;
    if (!extFits(ext) || !fitsVarLen16(name.size())) return 0;
    if (kind == SecurityOpKind::AlterUser && static_cast<uint8_t>(change) > static_cast<uint8_t>(UserChangeKind::Enable)) return 0;
    if (hasVerifier() && !detail::verifierFits(verifier)) return 0;
    if ((kind == SecurityOpKind::Grant || kind == SecurityOpKind::Revoke) && !fitsVarLen16(pattern.size())) return 0;
    if ((kind == SecurityOpKind::GrantRole || kind == SecurityOpKind::RevokeRole) && !fitsVarLen16(roleName.size())) return 0;
    if ((flags & ~kSecurityDdlKnownFlags) != 0) return 0;
    if ((flags & kSecurityDdlFlagCredentialProof) != 0 && credentialProof.size() != kCredentialProofBytes) return 0;
    std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
    pos += putExt(buf + pos, ext);
    buf[pos++] = k;
    pos += putVarStr(buf + pos, name);
    switch (kind) {
        case SecurityOpKind::CreateUser: pos += detail::putVerifier(buf + pos, verifier); break;
        case SecurityOpKind::AlterUser:
            buf[pos++] = static_cast<uint8_t>(change);
            if (change == UserChangeKind::DropPassword) buf[pos++] = passwordIndex;
            else if (change == UserChangeKind::SetPassword) {
                buf[pos++] = terminateSessions;
                pos += detail::putVerifier(buf + pos, verifier);
            } else if (change == UserChangeKind::AddPassword) pos += detail::putVerifier(buf + pos, verifier);
            break;
        case SecurityOpKind::Grant: case SecurityOpKind::Revoke:
            buf[pos++] = granteeKind; buf[pos++] = op; buf[pos++] = resourceType; buf[pos++] = patternKind;
            pos += putVarStr(buf + pos, pattern);
            break;
        case SecurityOpKind::GrantRole: case SecurityOpKind::RevokeRole: pos += putVarStr(buf + pos, roleName); break;
        default: break;
    }
    if (flags != 0) {
        buf[pos++] = flags;
        if ((flags & kSecurityDdlFlagCredentialProof) != 0) {
            std::memcpy(buf + pos, credentialProof.data(), kCredentialProofBytes);
            pos += kCredentialProofBytes;
        }
    }
    return pos;
}

inline std::optional<SecurityDdl> SecurityDdl::decode(const uint8_t* buf, std::size_t avail) noexcept {
    std::size_t pos = 0;
    auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
    if (!h) return std::nullopt;
    SecurityDdl m;
    m.sessionId = h->sessionId; m.correlationId = h->correlationId;
    if (!getExt(buf, avail, pos, m.ext)) return std::nullopt;
    if (pos + 1u > avail) return std::nullopt;
    const uint8_t k = buf[pos++];
    if (k < static_cast<uint8_t>(SecurityOpKind::CreateUser) || k > static_cast<uint8_t>(SecurityOpKind::RevokeRole)) return std::nullopt;
    m.kind = static_cast<SecurityOpKind>(k);
    if (!getVarStr(buf, avail, pos, m.name) || !detail::securityNameOk(m.name.size())) return std::nullopt;
    switch (m.kind) {
        case SecurityOpKind::CreateUser:
            if (!detail::getVerifier(buf, avail, pos, m.verifier)) return std::nullopt;
            break;
        case SecurityOpKind::AlterUser: {
            if (pos + 1u > avail) return std::nullopt;
            const uint8_t c = buf[pos++];
            if (c > static_cast<uint8_t>(UserChangeKind::Enable)) return std::nullopt;
            m.change = static_cast<UserChangeKind>(c);
            if (m.change == UserChangeKind::DropPassword) {
                if (pos + 1u > avail) return std::nullopt;
                m.passwordIndex = buf[pos++];
            } else if (m.change == UserChangeKind::SetPassword) {
                if (pos + 1u > avail) return std::nullopt;
                m.terminateSessions = buf[pos++];
                if (m.terminateSessions > 1) return std::nullopt;
                if (!detail::getVerifier(buf, avail, pos, m.verifier)) return std::nullopt;
            } else if (m.change == UserChangeKind::AddPassword) {
                if (!detail::getVerifier(buf, avail, pos, m.verifier)) return std::nullopt;
            }
            break;
        }
        case SecurityOpKind::Grant: case SecurityOpKind::Revoke:
            if (pos + 4u > avail) return std::nullopt;
            m.granteeKind = buf[pos]; m.op = buf[pos + 1]; m.resourceType = buf[pos + 2]; m.patternKind = buf[pos + 3];
            pos += 4;
            if (!validSecurityGranteeKind(m.granteeKind) || !validSecurityOperation(m.op) ||
                !validSecurityResourceType(m.resourceType) || !validSecurityPatternKind(m.patternKind))
                return std::nullopt;
            if (!getVarStr(buf, avail, pos, m.pattern) || m.pattern.size() > kSecurityPatternMax) return std::nullopt;
            break;
        case SecurityOpKind::GrantRole: case SecurityOpKind::RevokeRole:
            if (!getVarStr(buf, avail, pos, m.roleName) || !detail::securityNameOk(m.roleName.size())) return std::nullopt;
            break;
        default: break;
    }
    if (pos < avail) {   // the optional trailer
        m.flags = buf[pos++];
        if ((m.flags & ~kSecurityDdlKnownFlags) != 0 || m.flags == 0) return std::nullopt;
        if ((m.flags & kSecurityDdlFlagCredentialProof) != 0) {
            if (pos + kCredentialProofBytes > avail) return std::nullopt;
            m.credentialProof = std::string_view(reinterpret_cast<const char*>(buf + pos), kCredentialProofBytes);
            pos += kCredentialProofBytes;
        }
        if (pos != avail) return std::nullopt;
    }
    return m;
}

inline std::size_t SecurityBootstrap::encodedSize() const noexcept {
    return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + adminName.size() + detail::verifierEncodedSize(verifier) +
           kBootstrapSeedBytes + kBootstrapNonceBytes + kBootstrapMacBytes;
}

inline std::size_t SecurityBootstrap::encode(uint8_t* buf) const noexcept {
    if (!fitsVarLen16(adminName.size()) || !detail::verifierFits(verifier)) return 0;
    std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
    pos += putVarStr(buf + pos, adminName);
    pos += detail::putVerifier(buf + pos, verifier);
    std::memcpy(buf + pos, fakeSaltSeed.data(), fakeSaltSeed.size()); pos += fakeSaltSeed.size();
    std::memcpy(buf + pos, nonce.data(), nonce.size()); pos += nonce.size();
    std::memcpy(buf + pos, mac.data(), mac.size());     pos += mac.size();
    return pos;
}

inline std::optional<SecurityBootstrap> SecurityBootstrap::decode(const uint8_t* buf, std::size_t avail) noexcept {
    std::size_t pos = 0;
    auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
    if (!h) return std::nullopt;
    SecurityBootstrap m;
    m.sessionId = h->sessionId; m.correlationId = h->correlationId;
    if (!getVarStr(buf, avail, pos, m.adminName) || !detail::securityNameOk(m.adminName.size())) return std::nullopt;
    if (!detail::getVerifier(buf, avail, pos, m.verifier)) return std::nullopt;
    if (pos + kBootstrapSeedBytes + kBootstrapNonceBytes + kBootstrapMacBytes > avail) return std::nullopt;
    std::memcpy(m.fakeSaltSeed.data(), buf + pos, m.fakeSaltSeed.size()); pos += m.fakeSaltSeed.size();
    std::memcpy(m.nonce.data(), buf + pos, m.nonce.size()); pos += m.nonce.size();
    std::memcpy(m.mac.data(), buf + pos, m.mac.size());     pos += m.mac.size();
    return m;
}

/// The bytes the bootstrap MAC covers (D4): "rune-bootstrap-v1" | adminName var16 | salt var16 |
/// iterations u32 | storedKey | serverKey | fakeSaltSeed 32 | nonce.  ONE definition (CLI + gateway).  `out` is
/// cleared and refilled (its capacity persists).
inline void bootstrapMacInput(const SecurityBootstrap& b, std::vector<uint8_t>& out) {
    constexpr std::string_view kTag = "rune-bootstrap-v1";
    out.clear();
    if (!fitsVarLen16(b.adminName.size()) || !detail::verifierFits(b.verifier)) return;   // not encodable: empty input
    out.resize(kTag.size() + 2u + b.adminName.size() + detail::verifierEncodedSize(b.verifier) + b.fakeSaltSeed.size() + b.nonce.size());
    uint8_t* p = out.data();
    std::memcpy(p, kTag.data(), kTag.size()); p += kTag.size();
    p += putVarStr(p, b.adminName);
    p += detail::putVerifier(p, b.verifier);   // salt var16 | iterations u32 | storedKey | serverKey
    std::memcpy(p, b.fakeSaltSeed.data(), b.fakeSaltSeed.size()); p += b.fakeSaltSeed.size();
    std::memcpy(p, b.nonce.data(), b.nonce.size());
}

} // namespace rune::protocol::client
