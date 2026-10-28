// src/security/SecurityDdl.cpp — see rune/client/security/SecurityDdl.h.
#include <rune/client/security/SecurityDdl.h>

#include <rune/client/detail/SecureZero.h>
#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>

#include <cstring>
#include <stdexcept>

namespace rune::client::security {

namespace rpc = rune::protocol::client;

void VerifierStorage::wipe() noexcept {
    detail::secureZero(storedKey.data(), storedKey.size());
    detail::secureZero(serverKey.data(), serverKey.size());
    if (!salt.empty()) detail::secureZero(salt.data(), salt.size());
}

rpc::VerifierWire VerifierStorage::view() const noexcept {
    rpc::VerifierWire v;
    v.salt       = salt;
    v.iterations = iterations;
    v.storedKey  = std::string_view(storedKey.data(), storedKey.size());
    v.serverKey  = std::string_view(serverKey.data(), serverKey.size());
    return v;
}

void buildVerifier(std::string_view password, uint32_t iterations, uint32_t minPasswordLength, VerifierStorage& out) {
    if (password.size() < minPasswordLength) {
        throw std::invalid_argument("password must be at least " + std::to_string(minPasswordLength) + " characters");
    }
    if (password.size() > rune::client::detail::kMaxPasswordBytes) {
        throw std::invalid_argument("password must be at most " + std::to_string(rune::client::detail::kMaxPasswordBytes) + " characters");
    }
    auto v = rune::crypto::scram::makeVerifier(password, iterations);
    if (!v || v->storedKey.size != 64 || v->serverKey.size != 64) {
        throw std::runtime_error("cannot derive a SCRAM verifier (no randomness or hashing available)");
    }
    out.wipe();
    out.salt.assign(reinterpret_cast<const char*>(v->salt.data()), v->salt.size());
    out.iterations = v->iterations;
    std::memcpy(out.storedKey.data(), v->storedKey.bytes.data(), 64);
    std::memcpy(out.serverKey.data(), v->serverKey.bytes.data(), 64);
    detail::secureZero(v->storedKey.bytes.data(), v->storedKey.bytes.size());
    detail::secureZero(v->serverKey.bytes.data(), v->serverKey.bytes.size());
}

void fillSecurityDdl(const rune::ql::SecurityPlan& plan, uint32_t iterations, uint32_t minPasswordLength,
                     VerifierStorage& vs, rpc::SecurityDdl& msg) {
    msg.kind              = plan.kind;
    msg.name              = plan.name;
    msg.change            = plan.change;
    msg.passwordIndex     = plan.passwordIndex;
    msg.terminateSessions = plan.terminateSessions;
    msg.granteeKind       = plan.granteeKind;
    msg.op                = plan.op;
    msg.resourceType      = plan.resourceType;
    msg.patternKind       = plan.patternKind;
    msg.pattern           = plan.pattern;
    msg.roleName          = plan.roleName;
    if (plan.force) msg.flags |= rpc::kSecurityDdlFlagForce;
    if (msg.hasVerifier()) {
        buildVerifier(plan.password, iterations, minPasswordLength, vs);
        msg.verifier = vs.view();
    }
}

void buildBootstrap(std::string_view adminName, std::string_view oneTimePassword,
                    std::span<const uint8_t> clusterKeyFileContents, uint32_t iterations,
                    uint32_t minPasswordLength, VerifierStorage& vs, rpc::SecurityBootstrap& msg) {
    auto key = rune::crypto::deriveClusterKey(clusterKeyFileContents);
    if (!key) throw std::invalid_argument("the cluster key file must hold at least 32 bytes");
    buildVerifier(oneTimePassword, iterations, minPasswordLength, vs);
    msg.adminName = adminName;
    msg.verifier  = vs.view();
    if (!rune::crypto::randomBytes(msg.fakeSaltSeed) || !rune::crypto::randomBytes(msg.nonce)) {
        throw std::runtime_error("no randomness available for the bootstrap seed / nonce");
    }
    std::vector<uint8_t> input;
    rpc::bootstrapMacInput(msg, input);
    if (input.empty()) throw std::invalid_argument("the bootstrap admin name or verifier is not encodable");
    rune::crypto::DigestBytes mac;
    if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, key->master, input, mac) || mac.size != msg.mac.size()) {
        throw std::runtime_error("cannot compute the bootstrap MAC");
    }
    std::memcpy(msg.mac.data(), mac.bytes.data(), msg.mac.size());
    detail::secureZero(input.data(), input.size());
    detail::secureZero(key->master.data(), key->master.size());   // the derived master key
    detail::secureZero(mac.bytes.data(), mac.bytes.size());
}

} // namespace rune::client::security
