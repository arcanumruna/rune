// Authentication spec §5: the client half of SCRAM-SHA-512 inside the session handshake (Task 7).
//
// A fake SessionIo captures every frame the machine sends; the "server" side is computed with rune-crypto
// in the test (an independent implementation of the broker's half: it verifies the client's proof against
// a stored verifier and signs with the ServerKey).  The fake is as strict as the real broker will be:
// the combined nonce must extend the client's, one proof per listed verifier, nothing else accepted.
#include <rune/client/session/SessionMachine.h>
#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/crypto/TlsSession.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>

namespace pc = rune::protocol::client;
namespace rs = rune::crypto::scram;
namespace sm = rune::client::session;
namespace cr = rune::crypto;

namespace {

struct CapturingIo : sm::SessionIo {
    std::vector<std::vector<uint8_t>> frames;
    bool sendFrame(const uint8_t* b, std::size_t n) override { frames.emplace_back(b, b + n); return true; }
    bool retarget(const std::string&) noexcept override { return true; }
    [[nodiscard]] uint16_t lastTemplate() const {
        return pc::MessageHeader::decode(frames.back().data(), frames.back().size())->templateId;
    }
    [[nodiscard]] int64_t lastCorrelation() const {
        return pc::MessageHeader::decode(frames.back().data(), frames.back().size())->correlationId;
    }
    [[nodiscard]] int countOf(pc::TemplateId t) const {
        int n = 0;
        for (const auto& f : frames) {
            if (pc::MessageHeader::decode(f.data(), f.size())->templateId == static_cast<uint16_t>(t)) ++n;
        }
        return n;
    }
};

struct Server {   // a test SCRAM server over one or two verifiers
    std::vector<rs::Verifier> verifiers;
    std::string snonce = "SRVnonce";
    pc::AuthServerFirst first(const pc::AuthClientFirst& cf, std::string& combined) {
        combined = std::string(cf.clientNonce) + snonce;
        pc::AuthServerFirst sf;
        sf.combinedNonce = combined;
        sf.count = static_cast<uint8_t>(verifiers.size());
        for (std::size_t i = 0; i < verifiers.size(); ++i)
            sf.salts[i] = {std::string_view(reinterpret_cast<const char*>(verifiers[i].salt.data()),
                                            verifiers[i].salt.size()), verifiers[i].iterations};
        return sf;
    }
    [[nodiscard]] std::string authMessage(const pc::AuthClientFirst& cf, const std::string& combined, std::size_t i) const {
        return rs::authMessage(rs::clientFirstBare(cf.username, cf.clientNonce),
                               rs::serverFirst(combined, verifiers[i].salt, verifiers[i].iterations),
                               rs::clientFinalWithoutProof(combined));
    }
    [[nodiscard]] std::string signature(const pc::AuthClientFirst& cf, const std::string& combined, std::size_t i) const {
        auto sig = rs::serverSignature(cr::Digest::Sha512, verifiers[i].serverKey, authMessage(cf, combined, i)).value();
        return std::string(reinterpret_cast<const char*>(sig.bytes.data()), sig.size);
    }
};

pc::ConnectResponse authRequired(int64_t corr) {
    pc::ConnectResponse r;
    r.correlationId = corr; r.code = 0; r.assignedSessionId = 42; r.v9 = true;
    r.authMode = pc::AuthMode::Required; r.authState = pc::AuthState::MustAuthenticate;
    return r;
}

// Drives connect() and the ConnectResponse of a Required broker; returns the AuthClientFirst the machine sent.
struct Rig {
    CapturingIo io;
    sm::SessionConfig cfg;
    std::unique_ptr<sm::SessionMachine> m;
    int64_t corr = 0;
    explicit Rig(const std::string& user = "ws1", const std::string& pass = "correct horse battery",
                 bool forever = false) {
        cfg.username = user; cfg.password = pass; cfg.reconnectForeverAfterLive = forever;
        m = std::make_unique<sm::SessionMachine>(io, cfg);
    }
    void connectAuthRequired(int64_t now = 1) {
        m->connect(0);
        corr = pc::MessageHeader::decode(io.frames.back().data(), io.frames.back().size())->correlationId;
        auto r = authRequired(corr);
        m->onConnectResponse(r.code, -1, r.assignedSessionId, corr, now, 0, {}, r.authMode, r.authState);
    }
    [[nodiscard]] pc::AuthClientFirst clientFirst() const {
        return pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    }
    // Plays the broker's AuthServerFirst; returns the AuthClientFinal the machine answers.
    pc::AuthClientFinal serverFirst(Server& srv, const pc::AuthClientFirst& cf, std::string& combined, int64_t now = 2) {
        auto sf = srv.first(cf, combined);
        m->onAuthServerFirst(sf, now);
        EXPECT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFinal));
        return pc::AuthClientFinal::decode(io.frames.back().data(), io.frames.back().size()).value();
    }
};

} // namespace

TEST(SessionMachineAuthRegression, FullExchangeReachesConnectedOnlyAfterTheServerProvesThePassword) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    const int64_t corr = pc::MessageHeader::decode(io.frames.back().data(), io.frames.back().size())->correlationId;
    auto r = authRequired(corr);
    m.onConnectResponse(r.code, -1, r.assignedSessionId, corr, 1, 0, {}, r.authMode, r.authState);
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating);
    ASSERT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFirst));
    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    EXPECT_EQ(cf.username, "ws1");
    EXPECT_EQ(cf.sessionId, 42);
    EXPECT_EQ(cf.correlationId, corr);

    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    m.onAuthServerFirst(sf, 2);
    ASSERT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFinal));
    auto fin = pc::AuthClientFinal::decode(io.frames.back().data(), io.frames.back().size()).value();
    ASSERT_EQ(fin.count, 1);
    const std::string cfb = rs::clientFirstBare("ws1", cf.clientNonce);
    const std::string am = rs::authMessage(cfb, rs::serverFirst(combined, srv.verifiers[0].salt, 4096),
                                           rs::clientFinalWithoutProof(combined));
    ASSERT_TRUE(rs::verifyClientProof(cr::Digest::Sha512, srv.verifiers[0].storedKey, am, fin.proofs[0]));
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating) << "not Connected until the server signs";

    auto sig = rs::serverSignature(cr::Digest::Sha512, srv.verifiers[0].serverKey, am).value();
    const std::string sigStr(reinterpret_cast<const char*>(sig.bytes.data()), sig.size);
    m.onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = sigStr}, 3);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.brokerAuthMode(), pc::AuthMode::Required);
    EXPECT_FALSE(m.passwordChangeRequired());
}

TEST(SessionMachineAuthRegression, AWrongServerSignatureIsRejectedNeverConnected) {
    Rig t; t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; (void)t.serverFirst(srv, t.clientFirst(), combined);
    const std::string zeros(64, '\0');
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = zeros}, 3);
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    EXPECT_EQ(t.m->rejectCode(), static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
}

TEST(SessionMachineAuthRegression, AMissingServerSignatureIsRejectedNeverConnected) {
    Rig t; t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; (void)t.serverFirst(srv, t.clientFirst(), combined);
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0}, 3);   // OK but unsigned
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    EXPECT_EQ(t.m->rejectCode(), 44);
}

TEST(SessionMachineAuthRegression, AWrongPasswordIsAnotherProofTheServerRejectsAndItsSignatureIsNotAccepted) {
    // A server that does not know our password signs with ITS ServerKey: the client's expected signature
    // (derived from the password it holds) differs, so the "OK" is refused.
    Rig t("ws1", "a different password!"); t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto cf = t.clientFirst();
    auto fin = t.serverFirst(srv, cf, combined);
    const std::string am = srv.authMessage(cf, combined, 0);
    EXPECT_FALSE(rs::verifyClientProof(cr::Digest::Sha512, srv.verifiers[0].storedKey, am, fin.proofs[0]))
        << "the proof of the wrong password must not verify";
    const std::string sig = srv.signature(cf, combined, 0);
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = sig}, 3);
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected) << "a signature the client's password cannot produce";
}

TEST(SessionMachineAuthRegression, TwoVerifiersGetTwoProofsAndEitherPasswordWorks) {
    for (const char* mine : {"old password 123", "new password 456"}) {
        SCOPED_TRACE(mine);
        Rig t("ws1", mine); t.connectAuthRequired();
        Server srv;
        srv.verifiers.push_back(*rs::makeVerifier("old password 123"));
        srv.verifiers.push_back(*rs::makeVerifier("new password 456"));
        std::string combined; auto cf = t.clientFirst();
        auto fin = t.serverFirst(srv, cf, combined);
        ASSERT_EQ(fin.count, 2);
        const std::size_t matching = std::string_view(mine) == "old password 123" ? 0 : 1;
        for (std::size_t i = 0; i < 2; ++i) {
            const bool ok = rs::verifyClientProof(cr::Digest::Sha512, srv.verifiers[i].storedKey,
                                                  srv.authMessage(cf, combined, i), fin.proofs[i]);
            EXPECT_EQ(ok, i == matching) << "exactly the matching verifier's proof verifies (verifier " << i << ")";
        }
        const std::string sig = srv.signature(cf, combined, matching);
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = sig}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Connected);
    }
}

TEST(SessionMachineAuthRegression, AuthenticationFailedIsTerminalUnlessRateLimited) {
    {   // 44, retryAfterMs 0 -> Rejected(44)
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
        EXPECT_EQ(t.m->rejectCode(), 44);
    }
    {   // 44 with retryAfterMs, a plain session (never reconnects forever) -> Rejected too
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 800}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
        EXPECT_EQ(t.m->authRetryAfterMs(), 800u);
    }
    {   // reconnectForeverAfterLive + a previous Live episode: 44 with retryAfterMs 800 -> Backoff >= now + 800 ms
        Rig t("ws1", "correct horse battery", /*forever=*/true);
        t.m->connect(0);
        const int64_t c0 = t.io.lastCorrelation();
        t.m->onConnectResponse(0, -1, 42, c0, 1);                // a dev-mode style OK: Connected, Live
        ASSERT_EQ(t.m->state(), sm::SessionState::Connected);
        ASSERT_TRUE(t.m->everLive());
        t.m->forceReconnect(10);                                 // the next episode must log in
        const int64_t c1 = t.io.lastCorrelation();
        auto r = authRequired(c1);
        t.m->onConnectResponse(r.code, -1, 43, c1, 11, 0, {}, r.authMode, r.authState);
        ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
        const int64_t now = 1'000'000'000;
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 800}, now);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_GE(t.m->nextAttemptNs(), now + 800'000'000LL) << "D27: honour the broker's retryAfterMs";
        const std::size_t before = t.io.frames.size();
        t.m->tick(now + 799'000'000LL);
        EXPECT_EQ(t.io.frames.size(), before) << "no attempt before the broker's retry-after";
        t.m->tick(now + 800'000'000LL);
        EXPECT_EQ(t.m->state(), sm::SessionState::Connecting) << "then a fresh ConnectRequest";
        EXPECT_EQ(t.io.countOf(pc::TemplateId::AuthClientFirst), 1);
    }
    {   // the same without retryAfterMs: terminal even for a reconnect-forever session
        Rig t("ws1", "correct horse battery", true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        t.m->forceReconnect(10);
        auto r = authRequired(t.io.lastCorrelation());
        t.m->onConnectResponse(r.code, -1, 43, t.io.lastCorrelation(), 11, 0, {}, r.authMode, r.authState);
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44}, 20);
        EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    }
}

TEST(SessionMachineAuthRegression, ADevModeBrokerSkipsAuthentication) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "pw";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    const int64_t corr = io.lastCorrelation();
    m.onConnectResponse(0, -1, 42, corr, 1, 0, {}, pc::AuthMode::DevDisabled, pc::AuthState::Anonymous);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.brokerAuthMode(), pc::AuthMode::DevDisabled);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
}

TEST(SessionMachineAuthRegression, APreV9BrokerConnectsAtOnceWithoutAuthentication) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "pw";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1);   // defaults: InProcess / Anonymous (no v9 trailer)
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
}

TEST(SessionMachineAuthRegression, AResumedAuthenticatedSessionDoesNotReauthenticate) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "pw";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1, 0, {}, pc::AuthMode::Required,
                        pc::AuthState::ResumedAuthenticated);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
}

TEST(SessionMachineAuthRegression, NoDataFrameLeavesBeforeConnected) {
    Rig t; t.connectAuthRequired();
    EXPECT_EQ(t.m->state(), sm::SessionState::Authenticating);
    EXPECT_FALSE(t.m->canSendData());
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto cf = t.clientFirst();
    (void)t.serverFirst(srv, cf, combined);
    EXPECT_FALSE(t.m->canSendData());
    // Ticking while Authenticating sends no keepalive / data: only the two auth frames and the ConnectRequest.
    t.m->tick(2'000'000'000LL);
    for (const auto& f : t.io.frames) {
        const auto tid = pc::MessageHeader::decode(f.data(), f.size())->templateId;
        EXPECT_TRUE(tid == static_cast<uint16_t>(pc::TemplateId::Connect) ||
                    tid == static_cast<uint16_t>(pc::TemplateId::AuthClientFirst) ||
                    tid == static_cast<uint16_t>(pc::TemplateId::AuthClientFinal)) << "template " << tid;
    }
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = srv.signature(cf, combined, 0)}, 3);
    EXPECT_TRUE(t.m->canSendData());
}

TEST(SessionMachineAuthRegression, ABrokerNonceThatDoesNotExtendOursIsRejected) {
    Rig t; t.connectAuthRequired();
    auto cf = t.clientFirst();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    const std::string other = "someoneElsesNonce" + std::string(cf.clientNonce);
    sf.combinedNonce = other;                      // does not START with our nonce
    t.m->onAuthServerFirst(sf, 2);
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    EXPECT_EQ(t.m->rejectCode(), 44);
    EXPECT_EQ(t.io.countOf(pc::TemplateId::AuthClientFinal), 0) << "no proof is ever sent to a nonce we did not agree";
}

TEST(SessionMachineAuthRegression, AnEchoedClientNonceWithNoServerPartIsRejected) {
    Rig t; t.connectAuthRequired();
    auto cf = t.clientFirst();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    srv.snonce = "";                               // the broker contributed nothing
    std::string combined; auto sf = srv.first(cf, combined);
    t.m->onAuthServerFirst(sf, 2);
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
}

TEST(SessionMachineAuthRegression, NoCredentialsStillSendsAuthClientFirstAndTheBrokerDecides) {
    Rig t("", ""); t.connectAuthRequired();
    EXPECT_EQ(t.m->state(), sm::SessionState::Authenticating) << "the client never short-circuits";
    ASSERT_EQ(t.io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFirst));
    // (the strict decoder rejects an empty username — the broker answers 44; exactly that is what the client sees)
    EXPECT_FALSE(pc::AuthClientFirst::decode(t.io.frames.back().data(), t.io.frames.back().size()).has_value());
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44}, 3);
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    EXPECT_EQ(t.m->rejectCode(), 44);
}

TEST(SessionMachineAuthRegression, AFinalBeforeTheProofWasSentIsRejected) {
    Rig t; t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    const std::string sig(64, 'x');
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = sig}, 3);   // skipped the server-first
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
}

TEST(SessionMachineAuthRegression, ADuplicateServerFirstSendsNoSecondProof) {
    Rig t; t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto cf = t.clientFirst();
    auto sf = srv.first(cf, combined);
    t.m->onAuthServerFirst(sf, 2);
    t.m->onAuthServerFirst(sf, 3);
    EXPECT_EQ(t.io.countOf(pc::TemplateId::AuthClientFinal), 1);
    EXPECT_EQ(t.m->state(), sm::SessionState::Authenticating);
}

TEST(SessionMachineAuthRegression, TheMustChangePasswordFlagIsLatchedOnTheConnectedSession) {
    Rig t; t.connectAuthRequired();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto cf = t.clientFirst();
    (void)t.serverFirst(srv, cf, combined);
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .flags = pc::kAuthFlagMustChangePassword,
                                               .serverSignature = srv.signature(cf, combined, 0)}, 3);
    EXPECT_EQ(t.m->state(), sm::SessionState::Connected);
    EXPECT_TRUE(t.m->passwordChangeRequired());
}

TEST(SessionMachineAuthRegression, ALoginThatNeverAnswersTimesOutLikeAStuckHandshake) {
    Rig t; t.connectAuthRequired(/*now=*/1);
    ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
    t.m->tick(1 + 5'000'000'000LL + 1);
    EXPECT_EQ(t.m->state(), sm::SessionState::Disconnected) << "transient (retriable), not a verdict";
    EXPECT_EQ(t.m->lastConnectError(), sm::SessionMachine::kHandshakeTimedOut);
}

TEST(SessionMachineAuthRegression, AnErrorResponseAuthenticationFailedEndsTheLogin) {
    Rig t; t.connectAuthRequired();
    t.m->onErrorResponse(44, "authentication deadline", t.corr, 3, 42);   // D13: the broker's auth deadline
    EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
    EXPECT_EQ(t.m->rejectCode(), 44);
}

TEST(SessionMachineAuthRegression, ASessionRevokedNoticeIsTerminalAndNeverResumed) {
    CapturingIo io; sm::SessionConfig cfg;
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);
    m.onErrorResponse(46, "revoked", /*corr=*/0, 5, /*noticeSessionId=*/99);     // another session's notice
    EXPECT_EQ(m.state(), sm::SessionState::Connected) << "a notice naming another session moves nothing";
    m.onErrorResponse(46, "revoked", 0, 6, 42);
    EXPECT_EQ(m.state(), sm::SessionState::Rejected);
    EXPECT_EQ(m.rejectCode(), 46);
    const std::size_t before = io.frames.size();
    m.tick(10'000'000'000LL);
    m.tick(20'000'000'000LL);
    EXPECT_EQ(io.frames.size(), before) << "not reconnected (D26)";
}

TEST(SessionMachineAuthRegression, TheSaltedPasswordCacheIsSharedAcrossSessions) {
    sm::ScramCache cache;
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    for (int i = 0; i < 3; ++i) {
        Rig t; t.m->useScramCache(&cache); t.connectAuthRequired();
        std::string combined; (void)t.serverFirst(srv, t.clientFirst(), combined);
    }
    EXPECT_EQ(cache.pbkdf2Computations(), 1u) << "PBKDF2 once per verifier";
    EXPECT_EQ(cache.hits(), 2u);
}

TEST(ScramCacheTest, EvictsTheOldestAndZeroesWhatItDrops) {
    sm::ScramCache cache;
    std::vector<uint8_t> salt(16, 1);
    for (uint32_t i = 0; i < sm::ScramCache::kCapacity + 2; ++i) {
        salt[0] = static_cast<uint8_t>(i);
        rs::SaltedPassword sp; sp.bytes.size = 64; sp.bytes.bytes.fill(static_cast<uint8_t>(i + 1));
        cache.put(salt, 4096, sp);
    }
    EXPECT_EQ(cache.size(), sm::ScramCache::kCapacity);
    salt[0] = 0;   EXPECT_EQ(cache.find(salt, 4096), nullptr) << "the oldest was evicted";
    salt[0] = 9;   ASSERT_NE(cache.find(salt, 4096), nullptr);
    salt[0] = 9;   EXPECT_EQ(cache.find(salt, 4097), nullptr) << "iterations are part of the key";
    cache.clear();
    EXPECT_EQ(cache.size(), 0u);
}

// ---- the TLS path: SCRAM runs AFTER the handshake's done() and before Connected (D11) ----------------------

namespace {
std::string slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary); std::ostringstream ss; ss << f.rdbuf(); return ss.str();
}
std::string testdata(const char* name) { return std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name; }
} // namespace

TEST(SessionMachineAuthRegression, OverTlsTheLoginFollowsTheHandshakeAndConnectedComesLast) {
    const std::string serverPem = slurp(testdata("server.pem")), serverKey = slurp(testdata("server.key")),
                      caPem = slurp(testdata("ca.pem"));
    ASSERT_FALSE(serverPem.empty());
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    CapturingIo io; sm::SessionConfig cfg;
    cfg.offerCrypto = true; cfg.tlsTrustPem = caPem; cfg.tlsExpectHost = "localhost";
    cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    std::size_t delivered = 0;
    constexpr int64_t kSid = 42;
    for (int i = 0; i < 20 && m.state() != sm::SessionState::Authenticating &&
                    m.state() != sm::SessionState::Connected; ++i) {
        ASSERT_GT(io.frames.size(), delivered);
        const auto& frame = io.frames[delivered];
        const bool first = delivered == 0;
        ++delivered;
        std::string_view peer; int64_t corr = 0; uint8_t cipher = 0;
        if (first) {
            auto req = rune::client::codec::decodeConnectRequest(frame.data(), frame.size());
            ASSERT_TRUE(req && !req->cipherList.empty());
            peer = req->handshakeBytes; corr = req->correlationId; cipher = req->cipherList[0];
        } else {
            auto hc = rune::client::codec::decodeHandshakeContinue(frame.data(), frame.size());
            ASSERT_TRUE(hc);
            peer = hc->handshakeBytes; corr = hc->correlationId;
        }
        ASSERT_NE(srv.readInbound(reinterpret_cast<const uint8_t*>(peer.data()), peer.size()), cr::HsState::Failed);
        uint8_t out[16384];
        const std::size_t n = srv.writeOutbound(out, sizeof out);
        const std::string_view outView(reinterpret_cast<const char*>(out), n);
        if (first) m.onConnectResponse(0, -1, kSid, corr, 1, cipher, outView, pc::AuthMode::Required,
                                       pc::AuthState::MustAuthenticate);
        else m.onHandshakeContinueResponse(0, outView, corr, 1);
    }
    ASSERT_EQ(m.state(), sm::SessionState::Authenticating)
        << "TLS done() must lead to Authenticating, never straight to Connected (state "
        << static_cast<int>(m.state()) << ")";
    EXPECT_TRUE(m.cryptoState().encrypted) << "the login runs under the negotiated keys";
    ASSERT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFirst));
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 1);

    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    Server s; s.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = s.first(cf, combined);
    m.onAuthServerFirst(sf, 2);
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating);
    m.onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = s.signature(cf, combined, 0)}, 3);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
}
