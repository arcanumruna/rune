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
                 bool forever = false, bool allowUnauthenticated = false) {
        cfg.username = user; cfg.password = pass; cfg.reconnectForeverAfterLive = forever;
        cfg.allowUnauthenticatedBroker = allowUnauthenticated;
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
    {   // 44 with retryAfterMs before the session was ever Live -> Backoff (Issue #519), not a verdict
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 800}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_EQ(t.m->authRetryAfterMs(), 800u);
    }
    {   // reconnectForeverAfterLive + a previous Live episode: 44 with retryAfterMs 800 -> Backoff >= now + 800 ms
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);   // episode 1 is a dev-style OK
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
        Rig t("ws1", "correct horse battery", true, true);
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
    CapturingIo io; sm::SessionConfig cfg;   // no credentials: a dev-mode broker is the expected peer
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    const int64_t corr = io.lastCorrelation();
    m.onConnectResponse(0, -1, 42, corr, 1, 0, {}, pc::AuthMode::DevDisabled, pc::AuthState::Anonymous);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.brokerAuthMode(), pc::AuthMode::DevDisabled);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
}

TEST(SessionMachineAuthRegression, APreV9BrokerConnectsAtOnceWithoutAuthentication) {
    CapturingIo io; sm::SessionConfig cfg;
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1);   // defaults: InProcess / Anonymous (no v9 trailer)
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
}

// Review B I3: a client WITH credentials never takes a broker's word that it need not log in.  Connected needs a
// completed login (server signature verified) or the resume of a session THIS machine authenticated.
TEST(SessionMachineAuthRegression, ACredentialedClientRefusesABrokerThatSkipsTheLoginRegression) {
    struct Case { const char* name; pc::AuthMode mode; pc::AuthState state; bool preV9; };
    const Case cases[] = {
        {"DevDisabled",                pc::AuthMode::DevDisabled, pc::AuthState::Anonymous, false},
        {"InProcess",                  pc::AuthMode::InProcess,   pc::AuthState::Anonymous, false},
        {"pre-v9 (no trailer)",        pc::AuthMode::InProcess,   pc::AuthState::Anonymous, true},
        {"Required+Anonymous",         pc::AuthMode::Required,    pc::AuthState::Anonymous, false},
        {"Required+Resumed on first",  pc::AuthMode::Required,    pc::AuthState::ResumedAuthenticated, false},
    };
    for (const bool allow : {false, true}) {
        for (const Case& c : cases) {
            SCOPED_TRACE(std::string(c.name) + (allow ? " allowUnauthenticatedBroker" : ""));
            CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
            cfg.allowUnauthenticatedBroker = allow;
            sm::SessionMachine m(io, cfg);
            m.connect(0);
            if (c.preV9) m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1);
            else m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1, 0, {}, c.mode, c.state);
            if (allow) {
                EXPECT_EQ(m.state(), sm::SessionState::Connected);
                EXPECT_FALSE(m.authenticated()) << "an accepted unauthenticated broker is still not a login";
            } else {
                EXPECT_EQ(m.state(), sm::SessionState::Rejected);
                EXPECT_EQ(m.rejectCode(), 44);
                EXPECT_FALSE(m.rejectDetail().empty()) << "the refusal names its case";
            }
            EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
        }
    }
    // The detail names the case (the operator's next step).
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "x";
    sm::SessionMachine a(io, cfg);
    a.connect(0);
    a.onConnectResponse(0, -1, 42, io.lastCorrelation(), 1, 0, {}, pc::AuthMode::Required, pc::AuthState::Anonymous);
    EXPECT_NE(a.rejectDetail().find("rune security bootstrap"), std::string::npos) << a.rejectDetail();
    CapturingIo io2;
    sm::SessionMachine b(io2, cfg);
    b.connect(0);
    b.onConnectResponse(0, -1, 42, io2.lastCorrelation(), 1, 0, {}, pc::AuthMode::DevDisabled, pc::AuthState::Anonymous);
    EXPECT_NE(b.rejectDetail().find("allowUnauthenticatedBroker"), std::string::npos) << b.rejectDetail();
}

TEST(SessionMachineAuthRegression, AnEarnedResumeIsAcceptedAndAnUnearnedOneIsNot) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    auto r = authRequired(io.lastCorrelation());
    m.onConnectResponse(r.code, -1, 42, io.lastCorrelation(), 1, 0, {}, r.authMode, r.authState);
    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    m.onAuthServerFirst(sf, 2);
    m.onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = srv.signature(cf, combined, 0)}, 3);
    ASSERT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_TRUE(m.authenticated());
    const int firstsBefore = io.countOf(pc::TemplateId::AuthClientFirst);
    // The link drops; the machine resumes (the hint is its own authenticated id).
    m.onTransportDisconnected(10);
    m.tick(11);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    m.onConnectResponse(0, -1, 42, io.lastCorrelation(), 12, 0, {}, pc::AuthMode::Required, pc::AuthState::ResumedAuthenticated);
    EXPECT_EQ(m.state(), sm::SessionState::Connected) << "the broker kept this machine's own authenticated session";
    EXPECT_TRUE(m.authenticated());
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), firstsBefore) << "no second login";
    // ... but "resumed" under ANOTHER session id was never authenticated by this machine.
    m.onTransportDisconnected(20);
    m.tick(21);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);
    m.onConnectResponse(0, -1, 77, io.lastCorrelation(), 22, 0, {}, pc::AuthMode::Required, pc::AuthState::ResumedAuthenticated);
    EXPECT_EQ(m.state(), sm::SessionState::Rejected);
    EXPECT_EQ(m.rejectCode(), 44);
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

// Review B M2: the broker's auth deadline (D13) is a SESSION-level refusal: correlation id 0, naming the session.
TEST(SessionMachineAuthRegression, ASessionLevelAuthenticationFailedEndsTheLoginRegression) {
    Rig t; t.connectAuthRequired();
    t.m->onErrorResponse(44, "authentication deadline", /*corr=*/0, 3, /*noticeSessionId=*/99);   // another session's
    EXPECT_EQ(t.m->state(), sm::SessionState::Authenticating) << "a notice naming another session moves nothing";
    t.m->onErrorResponse(44, "authentication deadline", 0, 4, 42);
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

// ---- Task 8 gap (a): a refused login offer is re-sent, not left to time out ---------------------------------

namespace {
struct RefusingIo : CapturingIo {
    bool accept = true;
    int  refused = 0;
    bool sendFrame(const uint8_t* b, std::size_t n) override {
        if (!accept) { ++refused; return false; }
        return CapturingIo::sendFrame(b, n);
    }
};
} // namespace

TEST(SessionMachineAuthRegression, ARefusedAuthClientFirstIsResentWhenTheTransportRecoversRegression) {
    RefusingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    const int64_t corr = io.lastCorrelation();
    io.accept = false;                                     // the publication is back-pressured from here
    auto r = authRequired(corr);
    m.onConnectResponse(r.code, -1, r.assignedSessionId, corr, 1, 0, {}, r.authMode, r.authState);
    ASSERT_EQ(m.state(), sm::SessionState::Authenticating);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0);
    m.tick(1'000'000); m.tick(2'000'000);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 0) << "still refused";
    EXPECT_GE(io.refused, 2) << "re-offered every tick";
    io.accept = true;
    m.tick(3'000'000);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 1);
    m.tick(4'000'000); m.tick(5'000'000);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFirst), 1) << "exactly once on the wire";
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating);
    // The login then completes normally.
    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    m.onAuthServerFirst(sf, 6);
    m.onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = srv.signature(cf, combined, 0)}, 7);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
}

TEST(SessionMachineAuthRegression, ARefusedAuthClientFinalIsResentWhenTheTransportRecoversRegression) {
    RefusingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    auto r = authRequired(io.lastCorrelation());
    m.onConnectResponse(r.code, -1, r.assignedSessionId, io.lastCorrelation(), 1, 0, {}, r.authMode, r.authState);
    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    io.accept = false;
    m.onAuthServerFirst(sf, 2);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFinal), 0);
    m.tick(1'000'000);
    io.accept = true;
    m.tick(2'000'000);
    ASSERT_EQ(io.countOf(pc::TemplateId::AuthClientFinal), 1);
    m.tick(3'000'000);
    EXPECT_EQ(io.countOf(pc::TemplateId::AuthClientFinal), 1);
    auto fin = pc::AuthClientFinal::decode(io.frames.back().data(), io.frames.back().size()).value();
    EXPECT_TRUE(rs::verifyClientProof(cr::Digest::Sha512, srv.verifiers[0].storedKey,
                                      srv.authMessage(cf, combined, 0), fin.proofs[0]))
        << "the re-sent frame is the original proof";
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

// Review B I2: the broker's final-leg HandshakeFailed arrives while the machine is Authenticating (a TLS 1.3 client is
// done once it has SENT its Finished): terminal Rejected(HandshakeFailed), not a transient timeout.
TEST(SessionMachineAuthRegression, AFinalLegHandshakeFailedWhileAuthenticatingIsRetriedNotRejectedRegression) {
    const std::string serverPem = slurp(testdata("server.pem")), serverKey = slurp(testdata("server.key")),
                      caPem = slurp(testdata("ca.pem"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    CapturingIo io; sm::SessionConfig cfg;
    cfg.offerCrypto = true; cfg.tlsTrustPem = caPem; cfg.tlsExpectHost = "localhost";
    cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    std::size_t delivered = 0;
    constexpr int64_t kSid = 42;
    for (int i = 0; i < 20 && m.state() != sm::SessionState::Authenticating; ++i) {
        ASSERT_GT(io.frames.size(), delivered);
        const auto& frame = io.frames[delivered];
        const bool first = delivered++ == 0;
        std::string_view peer; int64_t corr = 0; uint8_t cipher = 0;
        if (first) {
            auto req = rune::client::codec::decodeConnectRequest(frame.data(), frame.size());
            peer = req->handshakeBytes; corr = req->correlationId; cipher = req->cipherList[0];
        } else {
            auto hc = rune::client::codec::decodeHandshakeContinue(frame.data(), frame.size());
            peer = hc->handshakeBytes; corr = hc->correlationId;
        }
        ASSERT_NE(srv.readInbound(reinterpret_cast<const uint8_t*>(peer.data()), peer.size()), cr::HsState::Failed);
        uint8_t out[16384];
        const std::size_t n = srv.writeOutbound(out, sizeof out);
        const std::string_view outView(reinterpret_cast<const char*>(out), n);
        if (first) m.onConnectResponse(0, -1, kSid, corr, 1, cipher, outView, pc::AuthMode::Required, pc::AuthState::MustAuthenticate);
        else m.onHandshakeContinueResponse(0, outView, corr, 1);
    }
    ASSERT_EQ(m.state(), sm::SessionState::Authenticating);
    const std::size_t sentBefore = io.frames.size();
    m.onHandshakeContinueResponse(static_cast<int16_t>(pc::ErrorCode::HandshakeFailed), {}, m.correlationId(), 2, kSid);
    // PR #476 review (supersedes N4-I1): the verdict is in clear, before the broker proved the keys - a transient connect
    // failure, retried as a fresh TLS episode within the connect budget, never terminal.
    EXPECT_NE(m.state(), sm::SessionState::Rejected);
    EXPECT_EQ(m.state(), sm::SessionState::Connecting) << "retried";
    EXPECT_EQ(m.lastConnectError(), static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
    EXPECT_FALSE(m.cryptoState().encrypted) << "the keys of the failed episode are gone";
    ASSERT_GT(io.frames.size(), sentBefore) << "a fresh ConnectRequest";
    auto again = rune::client::codec::decodeConnectRequest(io.frames.back().data(), io.frames.back().size());
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(again->cipherList.empty()) << "a fresh TLS offer";
}

// Issue #481: a session the app connected ends Disconnected once its unproven final-leg retries are spent (no background
// retries the app never asked for); a node session the agent revives on demand keeps the paced Backoff instead (a
// Disconnected one would be revived every duty cycle).  RED (old): Backoff for both.
TEST(SessionMachineAuthRegression, UnprovenFinalLegExhaustionEndsDisconnectedUnlessRevivedOnDemandRegression) {
    const std::string serverPem = slurp(testdata("server.pem")), serverKey = slurp(testdata("server.key")),
                      caPem = slurp(testdata("ca.pem"));
    constexpr int64_t kSid = 42;
    for (const bool revivedOnDemand : {false, true}) {
        SCOPED_TRACE(revivedOnDemand ? "revived on demand" : "app-connected");
        CapturingIo io; sm::SessionConfig cfg;
        cfg.offerCrypto = true; cfg.tlsTrustPem = caPem; cfg.tlsExpectHost = "localhost";
        cfg.username = "ws1"; cfg.password = "correct horse battery";
        cfg.revivedOnDemand = revivedOnDemand;
        sm::SessionMachine m(io, cfg);
        m.connect(0);
        for (int episode = 0; episode <= cfg.maxConnectRetries; ++episode) {
            // One fresh TLS episode to Authenticating (the client is done once it sent its Finished).
            cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
            ASSERT_TRUE(srv.valid());
            ASSERT_FALSE(io.frames.empty());
            std::size_t delivered = io.frames.size() - 1;   // this episode's ConnectRequest
            bool first = true;
            for (int i = 0; i < 20 && m.state() != sm::SessionState::Authenticating; ++i) {
                ASSERT_GT(io.frames.size(), delivered);
                const auto& frame = io.frames[delivered++];
                std::string_view peer; int64_t corr = 0; uint8_t cipher = 0;
                if (first) {
                    auto req = rune::client::codec::decodeConnectRequest(frame.data(), frame.size());
                    ASSERT_TRUE(req.has_value());
                    peer = req->handshakeBytes; corr = req->correlationId; cipher = req->cipherList[0];
                } else {
                    auto hc = rune::client::codec::decodeHandshakeContinue(frame.data(), frame.size());
                    ASSERT_TRUE(hc.has_value());
                    peer = hc->handshakeBytes; corr = hc->correlationId;
                }
                ASSERT_NE(srv.readInbound(reinterpret_cast<const uint8_t*>(peer.data()), peer.size()), cr::HsState::Failed);
                uint8_t out[16384];
                const std::size_t n = srv.writeOutbound(out, sizeof out);
                const std::string_view outView(reinterpret_cast<const char*>(out), n);
                if (first) m.onConnectResponse(0, -1, kSid, corr, 1, cipher, outView, pc::AuthMode::Required,
                                               pc::AuthState::MustAuthenticate);
                else m.onHandshakeContinueResponse(0, outView, corr, 1);
                first = false;
            }
            ASSERT_EQ(m.state(), sm::SessionState::Authenticating) << "episode " << episode;
            m.onHandshakeContinueResponse(static_cast<int16_t>(pc::ErrorCode::HandshakeFailed), {}, m.correlationId(), 2,
                                          kSid);
            if (episode < cfg.maxConnectRetries) {
                ASSERT_EQ(m.state(), sm::SessionState::Connecting) << "retried within the budget, episode " << episode;
            }
        }
        EXPECT_NE(m.state(), sm::SessionState::Rejected) << "never terminal";
        EXPECT_EQ(m.lastConnectError(), static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
        if (!revivedOnDemand) {
            ASSERT_EQ(m.state(), sm::SessionState::Disconnected) << "the app's failed connect ends the machine";
            const std::size_t sent = io.frames.size();
            m.tick(60'000'000'000LL);   // far past any Backoff delay
            EXPECT_EQ(m.state(), sm::SessionState::Disconnected);
            EXPECT_EQ(io.frames.size(), sent) << "no background retry the app never asked for";
        } else {
            ASSERT_EQ(m.state(), sm::SessionState::Backoff) << "a node session is paced, never revived per duty cycle";
            EXPECT_GT(m.nextAttemptNs(), 2) << "the Backoff delay lies ahead";
        }
    }
}

// Issues #15 / #111: AuthUnavailable (48) - the broker cannot vouch for its security state right now (booting, wiped,
// partitioned).  Never a verdict on the credentials (never Rejected), and the broker closed that session: the retry is a
// FRESH connect (sessionId dropped) - a reconnect-forever session that was Live backs off (the client's seed failover
// moves it to another broker on its odd Backoff attempts), a first connect fails retriably (Disconnected, code 48).
TEST(ClientAuthUnavailableRegression, BacksOffAndRotatesSeedsRegression) {
    constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
    {   // a first connect: a retriable failure the caller sees as 48 - never Rejected / AuthenticationFailed
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff) << "Issue #461: retried within its own budget";
        EXPECT_EQ(t.m->lastConnectError(), kUnavailable);
        EXPECT_EQ(t.m->sessionId(), 0) << "the broker ended that session: never resumed";
        EXPECT_EQ(t.m->authUnavailableCount(), 1u);
        t.m->tick(t.m->nextAttemptNs());
        EXPECT_EQ(t.m->state(), sm::SessionState::Connecting);
    }
    {   // reconnect-forever after Live: the re-login answered 48 -> Backoff, then a fresh ConnectRequest
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_TRUE(t.m->everLive());
        t.m->forceReconnect(10);
        auto r = authRequired(t.io.lastCorrelation());
        t.m->onConnectResponse(r.code, -1, 43, t.io.lastCorrelation(), 11, 0, {}, r.authMode, r.authState);
        ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, 20);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff) << "retriable: back off, never Rejected";
        EXPECT_EQ(t.m->sessionId(), 0);
        t.m->tick(t.m->nextAttemptNs());
        EXPECT_EQ(t.m->state(), sm::SessionState::Connecting) << "a fresh attempt";
    }
    {   // Connected, then the broker stays stale past R: the session-level 48 (corr 0) ends it retriably
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connected);
        t.m->onErrorResponse(kUnavailable, "stale", /*corr=*/0, 50, /*noticeSessionId=*/42);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_EQ(t.m->sessionId(), 0) << "reconnect FRESH elsewhere, never resume the ended session";
        EXPECT_EQ(t.m->authUnavailableCount(), 1u);
    }
    {   // the same for a plain session: Disconnected (the application reconnects), never Rejected
        Rig t("ws1", "correct horse battery", /*forever=*/false, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        t.m->onErrorResponse(kUnavailable, "stale", 0, 50, 42);
        EXPECT_EQ(t.m->state(), sm::SessionState::Disconnected);
        EXPECT_NE(t.m->state(), sm::SessionState::Rejected);
    }
    {   // a 48 on a request's own correlation id is not a session end
        Rig t("ws1", "correct horse battery", true, true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        t.m->onErrorResponse(kUnavailable, "", /*corr=*/77, 50, 42);
        EXPECT_EQ(t.m->state(), sm::SessionState::Connected);
    }
}

// Issues #461 / #519: before the session was ever Live a transient refusal (48, or a rate-limited 44) is retried with
// Backoff within maxUnavailableRetries; the exhausted budget ends Disconnected carrying the code, never Rejected.
TEST(ClientAuthUnavailableRegression, AFirstConnectRetriesAuthUnavailableWithinItsBudgetThenDisconnectsRegression) {
    constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
    Rig t;
    t.m->connect(0);
    int64_t now = 1;
    const int budget = t.cfg.maxUnavailableRetries;
    for (int i = 0; i <= budget; ++i) {
        const int64_t corr = t.io.lastCorrelation();
        if (i % 2 == 1) {   // the Connect itself answered 48
            ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << i;
            t.m->onConnectResponse(48, -1, 0, corr, ++now);
        } else {            // the login answered 48
            auto r = authRequired(corr);
            t.m->onConnectResponse(r.code, -1, 42, corr, ++now, 0, {}, r.authMode, r.authState);
            ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating) << i;
            t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, ++now);
        }
        if (i == budget) break;
        ASSERT_EQ(t.m->state(), sm::SessionState::Backoff) << i;
        EXPECT_EQ(t.m->unavailableRetries(), i + 1);
        now = t.m->nextAttemptNs();
        t.m->tick(now);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << i;
    }
    EXPECT_EQ(t.m->state(), sm::SessionState::Disconnected) << "budget spent: Disconnected, never Rejected";
    EXPECT_EQ(t.m->lastConnectError(), kUnavailable);
    EXPECT_EQ(t.m->rejectCode(), 0);
    t.m->connect(now + 1);
    EXPECT_EQ(t.m->state(), sm::SessionState::Connecting);
    EXPECT_EQ(t.m->unavailableRetries(), 0) << "a fresh connect() renews the budget";
}

TEST(ClientAuthUnavailableRegression, AForeverSessionNeverLiveAlsoRetriesAuthUnavailableRegression) {
    Rig t("ws1", "correct horse battery", /*forever=*/true);
    t.connectAuthRequired();
    ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
    ASSERT_FALSE(t.m->everLive());
    t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, 3);
    EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
    EXPECT_EQ(t.m->rejectCode(), 0);
}

TEST(ClientAuthRateLimitRegression, ARateLimited44BeforeLiveBacksOffAtLeastRetryAfterRegression) {
    {
        Rig t; t.connectAuthRequired();
        const int64_t T = 1'000'000'000;
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 500}, T);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_GE(t.m->nextAttemptNs(), T + 500'000'000LL);
        EXPECT_EQ(t.m->rejectCode(), 0);
        t.m->tick(t.m->nextAttemptNs() - 1);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        // The budget spent: Disconnected carrying 44, never Rejected.
        int64_t now = t.m->nextAttemptNs();
        for (int i = 0; i < t.cfg.maxUnavailableRetries + 2 && t.m->state() != sm::SessionState::Disconnected; ++i) {
            t.m->tick(now);
            ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << i;
            const int64_t corr = t.io.lastCorrelation();
            auto r = authRequired(corr);
            t.m->onConnectResponse(r.code, -1, 42, corr, now, 0, {}, r.authMode, r.authState);
            t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 500}, now);
            if (t.m->state() == sm::SessionState::Backoff) now = t.m->nextAttemptNs();
        }
        EXPECT_EQ(t.m->state(), sm::SessionState::Disconnected);
        EXPECT_EQ(t.m->lastConnectError(), 44);
        EXPECT_EQ(t.m->rejectCode(), 0);
    }
    {   // control: a plain 44 stays a verdict
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 0}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Rejected);
        EXPECT_EQ(t.m->rejectCode(), 44);
    }
}

// Adversarial review of PR #459 (issues #15 / #111): a 48 notice that names ANOTHER session (one this machine already
// left: a late notice from the stale broker after the client moved) never ends the current session; a broker that keeps
// answering 48 to every re-login is retried with a growing backoff, forever, and never turns into Rejected.
TEST(ClientAuthUnavailableRegression, AStaleNoticeNeverEndsTheNewSessionAndRepeated48sKeepRetryingAdversarialRegression) {
    constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
    {
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connected);
        t.m->onErrorResponse(kUnavailable, "stale", /*corr=*/0, 50, /*noticeSessionId=*/41);
        EXPECT_EQ(t.m->state(), sm::SessionState::Connected) << "a notice for session 41 never ends session 42";
        EXPECT_EQ(t.m->sessionId(), 42);
        EXPECT_EQ(t.m->authUnavailableCount(), 0u);
    }
    {
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_TRUE(t.m->everLive());
        int64_t now = 10;
        t.m->forceReconnect(now);
        int64_t lastDelay = -1;
        for (int i = 0; i < 40; ++i) {
            ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << i;
            auto r = authRequired(t.io.lastCorrelation());
            t.m->onConnectResponse(r.code, -1, 100 + i, t.io.lastCorrelation(), ++now, 0, {}, r.authMode, r.authState);
            ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating) << i;
            t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, ++now);
            ASSERT_EQ(t.m->state(), sm::SessionState::Backoff) << i << ": never Rejected";
            const int64_t delay = t.m->nextAttemptNs() - now;
            EXPECT_GE(delay, lastDelay) << i << ": the backoff never shrinks";
            lastDelay = delay;
            now = t.m->nextAttemptNs();
            t.m->tick(now);
        }
        EXPECT_EQ(t.m->authUnavailableCount(), 40u);
        EXPECT_EQ(t.m->rejectCode(), 0);
    }
}

// Issue #206: a broker whose `__security` log halted answers the retriable AuthUnavailable (48) - to a login and, at
// session level (corr 0), to a live session - and the client fails over to another broker: Disconnected (the caller's
// connect retry) or Backoff (a reconnect-forever session that was Live), never the terminal Rejected.
TEST(SessionMachineAuthRegression, AHaltedBrokerAnswerFailsOverInsteadOfRejectingRegression) {
    constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
    {   // a first login answered 48
        Rig t; t.connectAuthRequired();
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, 3);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff) << "Issue #461: a bounded retry, then Disconnected";
        EXPECT_EQ(t.m->rejectCode(), 0);
        EXPECT_EQ(t.m->lastConnectError(), kUnavailable);
    }
    {   // a re-login of a reconnect-forever session answered 48: Backoff, then a fresh attempt
        Rig t("ws1", "correct horse battery", /*forever=*/true, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        t.m->forceReconnect(10);
        auto r = authRequired(t.io.lastCorrelation());
        t.m->onConnectResponse(r.code, -1, 43, t.io.lastCorrelation(), 11, 0, {}, r.authMode, r.authState);
        ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
        t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 48}, 20);
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_EQ(t.m->rejectCode(), 0);
        t.m->tick(t.m->nextAttemptNs());
        EXPECT_EQ(t.m->state(), sm::SessionState::Connecting) << "a fresh attempt (the seed failover picks the broker)";
    }
    for (const bool forever : {true, false}) {   // a Connected session ended by the halt: ErrorResponse{48, corr 0}
        Rig t("ws1", "correct horse battery", forever, /*allowUnauthenticated=*/true);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connected);
        t.m->onErrorResponse(kUnavailable, "security log halted on this broker - reconnect to another broker",
                             /*corr=*/0, 50, /*noticeSessionId=*/42);
        EXPECT_EQ(t.m->state(), forever ? sm::SessionState::Backoff : sm::SessionState::Disconnected) << forever;
        EXPECT_EQ(t.m->rejectCode(), 0) << forever;
        EXPECT_EQ(t.m->sessionId(), 0) << forever << ": reconnect fresh, never resume the ended session";
    }
}
