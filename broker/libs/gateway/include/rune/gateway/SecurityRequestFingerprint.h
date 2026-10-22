#pragma once
// SecurityRequestFingerprint.h - the F14 request fingerprint of a security DDL (FNV-1a-64 over every field that makes
// the statement what it is - never the request id, client name, session or principal), in ONE place: the leader
// (SecurityAdmin) records it with every applied request and dedupes retries on (request id, fingerprint); the gateway
// (PR #410 review F2) recognizes a true retry - the same request id AND the same fingerprint as a request its mirror
// holds - before it exempts it from the login throttle.  Cold path (security DDL rate).
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <array>
#include <cstdint>
#include <span>
#include <string_view>

namespace rune::gateway {

inline constexpr uint16_t kSecurityDdlFingerprintTid = 79;   // TemplateId::SecurityDdl

/// The verifier part as the leader sees it: a kind without a verifier hashes an empty salt, iterations 0 and two
/// all-zero 64-byte keys (the ring message's default ScramVerifier).
struct VerifierBytes {
    std::span<const uint8_t> salt;
    uint32_t iterations = 0;
    std::span<const uint8_t> storedKey, serverKey;
};

[[nodiscard]] inline uint64_t securityDdlFingerprint(std::string_view statement, uint8_t kind, std::string_view name,
                                                     uint8_t change, uint8_t passwordIndex, uint8_t terminateSessions,
                                                     const VerifierBytes& v, uint8_t granteeKind, uint8_t op,
                                                     uint8_t resourceType, uint8_t patternKind, std::string_view pattern,
                                                     std::string_view roleName) noexcept {
    namespace det = rune::catalog::detail;
    uint64_t h = 0xcbf29ce484222325ULL;
    auto u = [&](uint64_t x) { h = det::fnvU64(h, x); };
    auto s = [&](std::string_view x) { h = det::fnvBytes(h, x.data(), x.size()); };
    auto b = [&](std::span<const uint8_t> x) { h = det::fnvBytes(h, x.data(), x.size()); };
    u(kSecurityDdlFingerprintTid); s(statement); u(kind); s(name); u(change); u(passwordIndex); u(terminateSessions);
    b(v.salt); u(v.iterations); b(v.storedKey); b(v.serverKey);
    u(granteeKind); u(op); u(resourceType); u(patternKind); s(pattern); s(roleName);
    return h;
}

/// The fingerprint of a decoded wire request (what the leader computes from the ring message built from it).
[[nodiscard]] inline uint64_t securityDdlFingerprint(const rune::protocol::client::SecurityDdl& m) noexcept {
    static constexpr std::array<uint8_t, 64> kZeroKey{};
    VerifierBytes v{{}, 0, kZeroKey, kZeroKey};
    if (m.hasVerifier()) {
        v.salt = {reinterpret_cast<const uint8_t*>(m.verifier.salt.data()), m.verifier.salt.size()};
        v.iterations = m.verifier.iterations;
        v.storedKey = {reinterpret_cast<const uint8_t*>(m.verifier.storedKey.data()), m.verifier.storedKey.size()};
        v.serverKey = {reinterpret_cast<const uint8_t*>(m.verifier.serverKey.data()), m.verifier.serverKey.size()};
    }
    return securityDdlFingerprint(m.ext.statement, static_cast<uint8_t>(m.kind), m.name, static_cast<uint8_t>(m.change),
                                  m.passwordIndex, m.terminateSessions, v, m.granteeKind, m.op, m.resourceType,
                                  m.patternKind, m.pattern, m.roleName);
}

/// The one refusal text of an own-credential change without a valid proof (issue #114) - also what the leader answers
/// a request-id reuse of a proof-checked change, so the answer never tells a right proof from a wrong one (F2).
inline constexpr std::string_view kOwnCredentialProofRefusal =
    "not authorized: changing your own password needs a fresh proof of a current password (log in again and retry)";

} // namespace rune::gateway
