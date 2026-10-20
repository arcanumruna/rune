#pragma once
// Fixtures for the authentication golden vectors (conformance/vectors/auth_*.bin,
// security_*.bin, connect_response_v9*.bin).  ONE definition shared by the vector generator
// (conformance/gen_auth_vectors.cpp) and the harness (ConformanceTest.cpp), so the committed
// bytes and the expected field values can never drift apart.
#include <array>
#include <string>
#include <string_view>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

namespace rune::protocol::client::authvec {

inline constexpr int64_t kSession = static_cast<int64_t>(0x1122334455667788ULL);
inline constexpr int64_t kCorr    = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

inline const std::string& salt16() { static const std::string s = "0123456789abcdef"; return s; }
inline const std::string& salt32() { static const std::string s = "0123456789abcdef0123456789ABCDEF"; return s; }
inline const std::string& key64(char c) { static std::string a(64, 'a'), b(64, 'b'); return c == 'a' ? a : b; }
inline const std::string& sig64() { static const std::string s(64, 'g'); return s; }

inline AuthClientFirst clientFirst() {
    AuthClientFirst m; m.sessionId = kSession; m.correlationId = kCorr;
    m.username = "ws1"; m.clientNonce = "rOprNGfwEbeRWgbNEkqO"; return m;
}
inline AuthServerFirst serverFirst() {
    AuthServerFirst m; m.sessionId = kSession; m.correlationId = kCorr;
    m.combinedNonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"; m.count = 2;
    m.salts[0] = {salt16(), 4096}; m.salts[1] = {salt32(), 8192}; return m;
}
inline AuthClientFinal clientFinal() {
    AuthClientFinal m; m.sessionId = kSession; m.correlationId = kCorr; m.count = 2;
    for (std::size_t i = 0; i < kScramProofBytes; ++i) {
        m.proofs[0][i] = static_cast<uint8_t>(i);
        m.proofs[1][i] = static_cast<uint8_t>(0xFF - i);
    }
    return m;
}
inline AuthServerFinal serverFinal() {
    AuthServerFinal m; m.sessionId = kSession; m.correlationId = kCorr;
    m.errorCode = 0; m.flags = kAuthFlagMustChangePassword; m.retryAfterMs = 0; m.serverSignature = sig64(); return m;
}
inline AuthServerFinal serverFinalFailed() {
    AuthServerFinal m; m.sessionId = kSession; m.correlationId = kCorr;
    m.errorCode = static_cast<uint16_t>(ErrorCode::AuthenticationFailed); m.retryAfterMs = 750;
    m.detail = "authentication failed"; return m;
}
inline VerifierWire verifier() { return VerifierWire{salt16(), 4096, key64('a'), key64('b')}; }
inline DdlExtension ext() {
    DdlExtension e;
    e.statement = "CREATE USER ws1 PASSWORD '<redacted>'";
    e.clientName = "cli"; e.requestId = {0xC11E, 77}; return e;
}
inline SecurityDdl createUser() {
    SecurityDdl m; m.sessionId = kSession; m.correlationId = kCorr; m.ext = ext();
    m.kind = SecurityOpKind::CreateUser; m.name = "ws1"; m.verifier = verifier(); return m;
}
inline SecurityDdl grant() {
    SecurityDdl m; m.sessionId = kSession; m.correlationId = kCorr; m.ext = ext();
    m.ext.statement = "GRANT WRITE ON TOPIC PREFIX 'world.' TO USER ws1";
    m.kind = SecurityOpKind::Grant; m.name = "ws1";
    m.granteeKind = 0; m.op = 1; m.resourceType = 1; m.patternKind = 1; m.pattern = "world.";   // WRITE TOPIC PREFIX (a grant v1 accepts)
    return m;
}
inline SecurityDdl setPassword() {
    SecurityDdl m = createUser();
    m.ext.statement = "ALTER USER ws1 SET PASSWORD '<redacted>' TERMINATE SESSIONS";
    m.kind = SecurityOpKind::AlterUser; m.change = UserChangeKind::SetPassword; m.terminateSessions = 1; return m;
}
inline SecurityBootstrap bootstrap() {
    SecurityBootstrap m; m.sessionId = kSession; m.correlationId = kCorr;
    m.adminName = "admin"; m.verifier = verifier();
    for (std::size_t i = 0; i < m.fakeSaltSeed.size(); ++i) m.fakeSaltSeed[i] = static_cast<uint8_t>(0x40 + i);
    for (std::size_t i = 0; i < m.nonce.size(); ++i) m.nonce[i] = static_cast<uint8_t>(0x10 + i);
    for (std::size_t i = 0; i < m.mac.size(); ++i) m.mac[i] = static_cast<uint8_t>(0x80 + i);
    return m;
}
inline ConnectResponse connectResponseV9() {
    ConnectResponse m; m.sessionId = kSession; m.correlationId = kCorr; m.code = 0; m.leaderMemberId = 2;
    m.assignedSessionId = static_cast<int64_t>(0xDEADBEEFULL);
    m.brokerVersion = kBrokerVersionAuth;   // the v9 golden vector pins broker version 5 (kBrokerVersion is now 6)
    m.v9 = true; m.authMode = AuthMode::Required; m.authState = AuthState::MustAuthenticate; return m;
}
inline ConnectResponse connectResponseV9Crypto() {
    ConnectResponse m = connectResponseV9(); m.chosenCipher = 2; m.handshakeBytes = "serverhello";
    m.authState = AuthState::ResumedAuthenticated; return m;
}

} // namespace rune::protocol::client::authvec
