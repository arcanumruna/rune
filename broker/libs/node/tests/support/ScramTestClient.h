#pragma once
// ScramTestClient.h - the client half of one SCRAM-SHA-512 exchange for tests that speak raw frames (authentication
// Task 11; used by the gateway unit tests and AuthWireClient).  Driven exactly as the real client's SessionMachine does:
// one RFC 5802 proof per listed salt, each over its OWN AuthMessage (D5).  A password of "" produces a garbage proof.

#include "support/SecurityEventChain.h"

#include <rune/crypto/Scram.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/security/SecurityState.h>

#include <array>
#include <cstring>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::node::test {

/// A real stored verifier (PBKDF2 at 4096 iterations) of `password` with a salt filled with 0xA0 + saltSeed.
[[nodiscard]] inline rune::security::ScramVerifier scramVerifierOf(const std::string& password, uint8_t saltSeed = 0, uint32_t iterations = 4096) {
    namespace rs = rune::crypto::scram;
    std::vector<uint8_t> salt(16, static_cast<uint8_t>(0xA0 + saltSeed));
    auto sp = rs::saltPassword(rune::crypto::Digest::Sha512, password, salt, iterations);
    auto v = rs::makeVerifier(rune::crypto::Digest::Sha512, *sp, salt, iterations);
    rune::security::ScramVerifier out;
    out.salt = salt; out.iterations = iterations;
    std::memcpy(out.storedKey.data(), v->storedKey.bytes.data(), 64);
    std::memcpy(out.serverKey.data(), v->serverKey.bytes.data(), 64);
    return out;
}

inline constexpr const char* kTestPasswordA = "correct horse battery";
inline constexpr const char* kTestPasswordB = "second password here";
inline constexpr const char* kTestAdminPassword = "admin password 123";

/// (the chain: emit() further events and feed their bytes to a SecurityView)
/// admin (Bootstrap, must change its password, seed `seed`), ws1 (two passwords A then B), ws2 (one password A),
/// off (disabled, password A).
[[nodiscard]] inline SecurityEventChain makeAuthTestChain(uint8_t seed = 0x42) {
    namespace sec = rune::security;
    SecurityEventChain c;
    sec::BootstrapOps b; b.verifier = scramVerifierOf(kTestAdminPassword, 9); b.fakeSaltSeed.fill(seed);
    (void)c.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    (void)c.emit(sec::CreateUserOps{scramVerifierOf(kTestPasswordA, 1)}, "ws1", "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    (void)c.emit(sec::AlterUserOps{sec::AddPassword{scramVerifierOf(kTestPasswordB, 2)}}, "ws1", "ALTER USER ws1 ADD PASSWORD '<redacted>';");
    (void)c.emit(sec::CreateUserOps{scramVerifierOf(kTestPasswordA, 3)}, "ws2", "CREATE USER ws2 WITH PASSWORD '<redacted>';");
    (void)c.emit(sec::CreateUserOps{scramVerifierOf(kTestPasswordA, 4)}, "off", "CREATE USER off WITH PASSWORD '<redacted>';");
    (void)c.emit(sec::AlterUserOps{sec::DisableUser{}}, "off", "ALTER USER off DISABLE;");
    return c;
}
[[nodiscard]] inline rune::security::SecurityState makeAuthTestState(uint8_t seed = 0x42) { return makeAuthTestChain(seed).state(); }

/// The salted-password cache of the real client (ScramCache): PBKDF2 is the expensive step of a login, paid once per
/// (salt, iterations) per client; later logins skip it.  Keyed on salt bytes + iterations.
struct ScramSaltCache {
    std::map<std::string, rune::crypto::scram::SaltedPassword> entries;
};

struct ScramAnswer {
    rune::protocol::client::AuthClientFinal final;
    std::array<std::array<uint8_t, 64>, 2>  expectedSignature{};   ///< per slot, what the broker must sign with
    std::array<std::optional<rune::crypto::scram::SaltedPassword>, 2> salted{};   ///< per slot (issue #114 proofs)
};

/// `passwords[i]` is tried against slot i (""= garbage proof); `count` proofs are sent (normally = first.count).
[[nodiscard]] inline std::optional<ScramAnswer> scramAnswer(const std::string& user, const std::string& clientNonce,
                                                            const rune::protocol::client::AuthServerFirst& first,
                                                            const std::array<std::string, 2>& passwords, int64_t sessionId,
                                                            int64_t correlationId, uint8_t count = 0,
                                                            ScramSaltCache* cache = nullptr) {
    namespace rs = rune::crypto::scram;
    ScramAnswer out;
    out.final.sessionId = sessionId;
    out.final.correlationId = correlationId;
    out.final.count = count == 0 ? first.count : count;
    const std::string cfb = rs::clientFirstBare(user, clientNonce);
    const std::string combined(first.combinedNonce);
    for (uint8_t i = 0; i < out.final.count; ++i) {
        out.final.proofs[i].fill(0x5A);
        if (i >= first.count || passwords[i].empty()) continue;
        const auto salt = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(first.salts[i].salt.data()),
                                                   first.salts[i].salt.size());
        std::optional<rs::SaltedPassword> sp;
        const std::string key = passwords[i] + '|' + std::string(first.salts[i].salt) + '|' + std::to_string(first.salts[i].iterations);
        if (cache != nullptr)
            if (auto it = cache->entries.find(key); it != cache->entries.end()) sp = it->second;
        if (!sp) {
            sp = rs::saltPassword(rune::crypto::Digest::Sha512, passwords[i], salt, first.salts[i].iterations);
            if (sp && cache != nullptr) cache->entries.emplace(key, *sp);
        }
        if (!sp) return std::nullopt;
        const std::string am = rs::authMessage(cfb, rs::serverFirst(combined, salt, first.salts[i].iterations),
                                               rs::clientFinalWithoutProof(combined));
        const auto proof = rs::clientProof(rune::crypto::Digest::Sha512, *sp, am);
        const auto sig = rs::expectedServerSignature(rune::crypto::Digest::Sha512, *sp, am);
        if (!proof || !sig) return std::nullopt;
        std::memcpy(out.final.proofs[i].data(), proof->bytes.data(), 64);
        std::memcpy(out.expectedSignature[i].data(), sig->bytes.data(), 64);
        out.salted[i] = *sp;
    }
    return out;
}

} // namespace rune::node::test
