#pragma once
// AuthWireClient.h - raw Rune frames over Aeron UDP plus the rune-crypto SCRAM client (authentication Task 11; used by
// Tasks 11-19).  A thin layer over ShardTestClient (its own embedded driver, bounded waits) that speaks the v9
// handshake the way the real client does - and lets a test do what the real client never would: a wrong proof, the
// wrong number of proofs, a request before the login, a pre-v9 Connect - and keep the raw AuthServerFirst /
// AuthServerFinal frames to compare them byte for byte.
//
//   connect(channel, version)        - Connect; the session id or -1 (lastConnectResponse() tells why)
//   login(channel, sid, user, pws)   - AuthClientFirst, AuthClientFinal; every frame and verdict captured in Login
//   connectAndLogin(channel, u, pw)  - both, the password tried against EVERY listed salt (the real client's shape)
//   securityDdl / bootstrap helpers  - the admin side of a test: users, roles, grants, passwords (verifiers are built
//                                      here with rune-crypto: the broker never sees a password)

#include "support/ScramTestClient.h"
#include "support/ShardTestClient.h"

#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace rune::node::test {

class AuthWireClient {
public:
    using Clock = std::chrono::steady_clock;
    using DdlAnswer = ShardTestClient::DdlAnswer;

    explicit AuthWireClient(const std::filesystem::path& aeronDir) : cl_(aeronDir) { cl_.keepSessionsAlive(true); }

    [[nodiscard]] ShardTestClient& raw() noexcept { return cl_; }
    [[nodiscard]] const std::optional<pc::ConnectResponse>& lastConnect() const noexcept { return cl_.lastConnectResponse(); }

    /// One Connect (retried until `within` while the broker's UDP link warms up).  -1 = refused / no answer.
    int64_t connect(const std::string& channel, int32_t clientVersion = pc::kClientVersionAuth,
                    std::chrono::milliseconds within = std::chrono::seconds{30}) {
        cl_.setClientVersion(clientVersion);
        const auto deadline = Clock::now() + within;
        for (;;) {
            const int64_t s = cl_.connect(channel, kClientStream, deadline);
            if (s > 0) return s;
            // A definitive refusal (a ConnectResponse with a code) is the answer; a missing one is a warming link.
            if (cl_.lastConnectResponse() && cl_.lastConnectResponse()->code != 0) return -1;
            if (Clock::now() >= deadline) return -1;
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
    }

    struct Login {
        int64_t sessionId = 0;
        std::string clientNonce;
        std::optional<pc::AuthServerFirst> first;
        std::vector<uint8_t> firstBytes, finalBytes;
        std::optional<pc::AuthServerFinal> fin;
        std::optional<ScramAnswer> answer;                  ///< what was sent (expected signatures per slot)
        [[nodiscard]] bool answered() const noexcept { return fin.has_value(); }
        [[nodiscard]] bool ok() const noexcept { return fin && fin->errorCode == 0; }
        [[nodiscard]] bool mustChangePassword() const noexcept {
            return fin && (fin->flags & pc::kAuthFlagMustChangePassword) != 0;
        }
        [[nodiscard]] uint16_t errorCode() const noexcept { return fin ? fin->errorCode : uint16_t{0xFFFF}; }
        /// The server signature verifies against ANY slot's expected one (what the real client checks).
        [[nodiscard]] bool serverSignatureValid() const noexcept {
            if (!ok() || !answer || fin->serverSignature.size() != 64) return false;
            for (uint8_t i = 0; i < answer->final.count; ++i)
                if (std::memcmp(answer->expectedSignature[i].data(), fin->serverSignature.data(), 64) == 0) return true;
            return false;
        }
    };

    /// The login legs on `sid`.  passwords[i] is tried against slot i ("" = a garbage proof).  `count` proofs are sent
    /// (0 = as many as the broker listed).  Stops after AuthServerFirst when `sendFinal` is false.
    Login login(const std::string& channel, int64_t sid, const std::string& user, const std::array<std::string, 2>& passwords,
                uint8_t count = 0, bool sendFinal = true, std::chrono::milliseconds within = std::chrono::seconds{20}) {
        Login out;
        out.sessionId = sid;
        out.clientNonce = rune::crypto::scram::makeNonce();
        pc::AuthClientFirst cf;
        cf.sessionId = sid; cf.correlationId = cl_.nextCorrelationId(); cf.username = user; cf.clientNonce = out.clientNonce;
        std::vector<uint8_t> b(cf.encodedSize());
        b.resize(cf.encode(b.data()));
        if (!cl_.send(channel, kClientStream, b.data(), b.size())) return out;
        const auto deadline = Clock::now() + within;
        const auto got = cl_.awaitAuthFrame(cf.correlationId, deadline);
        if (!got) return out;
        if (got->templateId == static_cast<uint16_t>(pc::TemplateId::AuthServerFinal)) {   // refused before any salt
            out.finalBytes = got->bytes;
            out.fin = pc::AuthServerFinal::decode(got->bytes.data(), got->bytes.size());
            return out;
        }
        out.firstBytes = got->bytes;
        out.first = pc::AuthServerFirst::decode(out.firstBytes.data(), out.firstBytes.size());
        if (!out.first || !sendFinal) return out;
        out.answer = scramAnswer(user, out.clientNonce, *out.first, passwords, sid, cf.correlationId, count,
                                 cacheSaltedPasswords_ ? &cache_ : nullptr);
        if (!out.answer) return out;
        std::vector<uint8_t> fb(out.answer->final.encodedSize());
        fb.resize(out.answer->final.encode(fb.data()));
        if (!cl_.send(channel, kClientStream, fb.data(), fb.size())) return out;
        if (const auto fin = cl_.awaitAuthFinal(cf.correlationId, deadline)) {
            out.finalBytes = fin->bytes;
            out.fin = pc::AuthServerFinal::decode(out.finalBytes.data(), out.finalBytes.size());
        }
        return out;
    }

    /// The real client's shape: one password tried against every listed salt.
    Login connectAndLogin(const std::string& channel, const std::string& user, const std::string& password,
                          int32_t clientVersion = pc::kClientVersionAuth) {
        const int64_t sid = connect(channel, clientVersion);
        if (sid <= 0) return {};
        return login(channel, sid, user, {password, password});
    }

    // ── the admin side: security DDL on an authenticated (admin) session ─────────────────────────────────────
    DdlAnswer securityDdl(const std::string& channel, pc::SecurityDdl m, std::string_view statement,
                          std::chrono::milliseconds within = std::chrono::seconds{30}) {
        m.correlationId = cl_.nextCorrelationId();
        m.ext.clientName = "auth-wire-client";
        m.ext.statement = statement;
        m.ext.requestId = pinnedRid_ ? *pinnedRid_ : rune::protocol::RequestId{0xA07E'0000'0000'0000ull | nonce_, ++requestCounter_};
        const auto deadline = Clock::now() + within;
        m.correlationId = cl_.nextCorrelationId();
        std::vector<uint8_t> b(m.encodedSize());
        b.resize(m.encode(b.data()));
        if (b.empty() || !cl_.send(channel, kClientStream, b.data(), b.size(), deadline)) return {0xFFFF, 0, 0, -1, "send failed"};
        const auto a = cl_.awaitAnyDdlAnswer(m.correlationId, deadline);
        if (!a) return {0xFFFF, 0, 0, -1, "no answer"};
        if (a->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DdlResponse))
            if (auto d = cl_.awaitDdl(m.correlationId, deadline)) return *d;
        // Any other response type (PasswordChangeRequired answers in the DdlResponse; an ErrorResponse carries a message).
        return {static_cast<uint16_t>(a->code), 0, 0, -1, a->message};
    }
    DdlAnswer createRole(const std::string& channel, int64_t sid, const std::string& name) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::CreateRole; m.name = name;
        return securityDdl(channel, m, "CREATE ROLE " + name + ";");
    }
    DdlAnswer createUser(const std::string& channel, int64_t sid, const std::string& name, const std::string& password,
                         std::chrono::milliseconds within = std::chrono::seconds{30}) {
        // A pinned request id is a RETRY of the same request: byte-identical, so the verifier (random salt) is reused.
        static std::map<std::string, Verifier> pinnedVerifiers;
        const Verifier v = pinnedRid_ ? pinnedVerifiers.try_emplace(password, verifierOf(password)).first->second : verifierOf(password);
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::CreateUser; m.name = name; m.verifier = v.wire();
        return securityDdl(channel, m, "CREATE USER " + name + " WITH PASSWORD '<redacted>';", within);
    }
    DdlAnswer alterPassword(const std::string& channel, int64_t sid, const std::string& user, pc::UserChangeKind change,
                            const std::string& password = {}, bool terminate = false, uint8_t index = 1) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::AlterUser; m.name = user; m.change = change;
        m.terminateSessions = terminate ? 1 : 0; m.passwordIndex = index;
        Verifier v;
        if (!password.empty()) { v = verifierOf(password); m.verifier = v.wire(); }
        return securityDdl(channel, m, "ALTER USER " + user + " ...;");
    }
    DdlAnswer grant(const std::string& channel, int64_t sid, const std::string& grantee, bool granteeIsRole,
                    rune::protocol::SecurityOperation op, rune::protocol::SecurityResourceType type,
                    rune::protocol::SecurityPatternKind kind, const std::string& pattern) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::Grant; m.name = grantee;
        m.granteeKind = granteeIsRole ? 1 : 0; m.op = static_cast<uint8_t>(op); m.resourceType = static_cast<uint8_t>(type);
        m.patternKind = static_cast<uint8_t>(kind); m.pattern = pattern;
        return securityDdl(channel, m, "GRANT ...;");
    }
    DdlAnswer revoke(const std::string& channel, int64_t sid, const std::string& grantee, bool granteeIsRole,
                     rune::protocol::SecurityOperation op, rune::protocol::SecurityResourceType type,
                     rune::protocol::SecurityPatternKind kind, const std::string& pattern) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::Revoke; m.name = grantee;
        m.granteeKind = granteeIsRole ? 1 : 0; m.op = static_cast<uint8_t>(op); m.resourceType = static_cast<uint8_t>(type);
        m.patternKind = static_cast<uint8_t>(kind); m.pattern = pattern;
        return securityDdl(channel, m, "REVOKE ...;");
    }
    DdlAnswer revokeRole(const std::string& channel, int64_t sid, const std::string& user, const std::string& role) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::RevokeRole; m.name = user; m.roleName = role;
        return securityDdl(channel, m, "REVOKE ROLE " + role + " FROM " + user + ";");
    }
    DdlAnswer dropRole(const std::string& channel, int64_t sid, const std::string& name) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::DropRole; m.name = name;
        return securityDdl(channel, m, "DROP ROLE " + name + ";");
    }
    DdlAnswer dropUser(const std::string& channel, int64_t sid, const std::string& name) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::DropUser; m.name = name;
        return securityDdl(channel, m, "DROP USER " + name + ";");
    }
    DdlAnswer grantRole(const std::string& channel, int64_t sid, const std::string& user, const std::string& role) {
        pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::GrantRole; m.name = user; m.roleName = role;
        return securityDdl(channel, m, "GRANT ROLE " + role + " TO " + user + ";");
    }

    /// SecurityBootstrap with a MAC under the cluster key file's contents (D4).  The answer is a DdlResponse.
    DdlAnswer bootstrap(const std::string& channel, int64_t sid, const std::string& keyFileContents,
                        const std::string& adminName, const std::string& oneTimePassword, uint8_t seed = 0x40) {
        const Verifier v = verifierOf(oneTimePassword);
        pc::SecurityBootstrap m;
        m.sessionId = sid; m.correlationId = cl_.nextCorrelationId(); m.adminName = adminName; m.verifier = v.wire();
        for (std::size_t i = 0; i < m.fakeSaltSeed.size(); ++i) m.fakeSaltSeed[i] = static_cast<uint8_t>(seed + i);
        rune::crypto::randomBytes(std::span<uint8_t>(m.nonce.data(), m.nonce.size()));
        const auto key = rune::crypto::deriveClusterKey(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(keyFileContents.data()), keyFileContents.size()));
        std::vector<uint8_t> input;
        pc::bootstrapMacInput(m, input);
        rune::crypto::DigestBytes mac;
        if (!key || !rune::crypto::hmac(rune::crypto::Digest::Sha512, key->master, input, mac)) return {0xFFFF, 0, 0, -1, "no mac"};
        std::memcpy(m.mac.data(), mac.bytes.data(), m.mac.size());
        std::vector<uint8_t> b(m.encodedSize());
        b.resize(m.encode(b.data()));
        const auto deadline = Clock::now() + std::chrono::seconds{30};
        if (!cl_.send(channel, kClientStream, b.data(), b.size(), deadline)) return {0xFFFF, 0, 0, -1, "send failed"};
        const auto d = cl_.awaitDdl(m.correlationId, deadline);
        return d ? *d : DdlAnswer{0xFFFF, 0, 0, -1, "no answer"};
    }

    /// Task 19: every following securityDdl carries this request id (a retry of the SAME request after a leader kill);
    /// nullopt = a fresh id per DDL again.
    void pinRequestId(std::optional<rune::protocol::RequestId> r) noexcept { pinnedRid_ = r; }

    /// Cache Hi(password, salt, i) like the real client's ScramCache: only the first login pays PBKDF2.
    void cacheSaltedPasswords(bool on) noexcept { cacheSaltedPasswords_ = on; }

    static constexpr int32_t kClientStream = 1;

private:
    struct Verifier {
        std::vector<uint8_t> salt;
        std::array<uint8_t, 64> stored{}, server{};
        [[nodiscard]] pc::VerifierWire wire() const {
            return pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(salt.data()), salt.size()), 4096,
                                    std::string_view(reinterpret_cast<const char*>(stored.data()), 64),
                                    std::string_view(reinterpret_cast<const char*>(server.data()), 64)};
        }
    };
    static Verifier verifierOf(const std::string& password) {
        Verifier out;
        const auto v = rune::crypto::scram::makeVerifier(password, 4096);
        out.salt = v->salt;
        std::memcpy(out.stored.data(), v->storedKey.bytes.data(), 64);
        std::memcpy(out.server.data(), v->serverKey.bytes.data(), 64);
        return out;
    }

    ShardTestClient cl_;
    ScramSaltCache cache_;
    bool cacheSaltedPasswords_ = false;
    uint64_t nonce_ = (static_cast<uint64_t>(std::random_device{}()) << 16 ^ std::random_device{}()) & 0x0000'FFFF'FFFF'FFFFull;
    uint64_t requestCounter_ = 0;
    std::optional<rune::protocol::RequestId> pinnedRid_;
};

} // namespace rune::node::test
