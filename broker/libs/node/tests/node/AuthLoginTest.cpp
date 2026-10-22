// AuthLoginTest.cpp - authentication Task 11 against ONE real BrokerNode (authentication ENABLED, real Aeron UDP): the
// SCRAM login in the session handshake as raw frames (AuthWireClient + the rune-crypto client).  Failure
// indistinguishability (Review Focus 1), two active passwords, rotation, the version gate, the pre-authentication
// gate, the auth deadline, the throttle's retry-after, the must-change-password rule, dev mode, resume, the logs that
// name the reason and the wire that does not, and the 1'000'229-231 counters.
#include "support/AuthBroker.h"

#include <rune/node/RoleCounter.h>

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kPwA = "correct horse battery";
constexpr const char* kPwB = "second password here";

bool sameBodyDifferentHeader(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
    return a.size() == b.size() && a.size() > h && std::equal(a.begin() + h, a.end(), b.begin() + h);
}

}  // namespace

TEST(AuthLoginRegression, FailureAnswersAreIndistinguishableRegression) {
    LogCapture log;
    AuthBroker b("indist");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    b.createUser("off", kPwA);
    b.disableUser("off");
    AuthWireClient& w = b.wire();

    struct Case { const char* what; std::string user; std::string pw; };
    const std::vector<Case> cases = {{"wrong password", "ws1", "not the password"},
                                     {"unknown user", "nobody", kPwA},
                                     {"disabled user (right password)", "off", kPwA},
                                     {"empty username", "", kPwA}};
    std::vector<AuthWireClient::Login> out;
    for (const Case& c : cases) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        out.push_back(w.login(b.channel(), sid, c.user, {c.pw, c.pw}));
        const auto& l = out.back();
        ASSERT_TRUE(l.first.has_value()) << c.what;
        EXPECT_EQ(l.first->count, 2) << c.what << ": review A - always two salts";
        EXPECT_EQ(l.first->salts[0].salt.size(), 16u) << c.what;
        EXPECT_EQ(l.first->salts[0].iterations, 4096u) << c.what;
        EXPECT_EQ(l.first->salts[1].salt.size(), 16u) << c.what;
        EXPECT_EQ(l.firstBytes.size(), out[0].firstBytes.size()) << c.what << ": one frame shape";
        ASSERT_TRUE(l.fin.has_value()) << c.what;
        EXPECT_EQ(l.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << c.what;
        EXPECT_EQ(l.fin->retryAfterMs, 0u) << c.what;
        EXPECT_EQ(l.fin->detail, "authentication failed") << c.what;
        EXPECT_TRUE(l.fin->serverSignature.empty()) << c.what;
        EXPECT_TRUE(sameBodyDifferentHeader(l.finalBytes, out[0].finalBytes))
            << c.what << ": the AuthServerFinal is byte-identical except the header ids";
    }
    // The control: the right password for an enabled account succeeds, with a signature the client can verify.
    const int64_t sid = w.connect(b.channel());
    const auto ok = w.login(b.channel(), sid, "ws1", {kPwA, kPwA});
    EXPECT_TRUE(ok.ok());
    EXPECT_TRUE(ok.serverSignatureValid());
    EXPECT_EQ(ok.firstBytes.size(), out[0].firstBytes.size()) << "a real user's first leg has the same shape too";
}

TEST(AuthLoginRegression, UnknownUserGetsTheSameFakeSaltEveryTime) {
    AuthBroker b("fakesalt");
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    std::vector<AuthWireClient::Login> logins;
    for (int i = 0; i < 3; ++i) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        logins.push_back(w.login(b.channel(), sid, "nobody", {"x", "x"}, 0, /*sendFinal=*/false));
        ASSERT_TRUE(logins.back().first.has_value());
    }
    for (int i = 1; i < 3; ++i)
        for (int s = 0; s < 2; ++s)
            EXPECT_EQ(logins[i].first->salts[s].salt, logins[0].first->salts[s].salt) << "slot " << s << " is stable across attempts";
    EXPECT_NE(logins[0].first->salts[0].salt, logins[0].first->salts[1].salt);
    // Another unknown name gets different salts (they are keyed on the name).
    const int64_t sid = w.connect(b.channel());
    const auto other = w.login(b.channel(), sid, "someone-else", {"x", "x"}, 0, false);
    ASSERT_TRUE(other.first.has_value());
    EXPECT_NE(other.first->salts[0].salt, logins[0].first->salts[0].salt);
}

TEST(AuthLoginTest, BothActivePasswordsLogIn) {
    AuthBroker b("twopw");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    b.addPassword("ws1", kPwB);
    AuthWireClient& w = b.wire();
    for (const char* pw : {kPwA, kPwB}) {
        const auto l = w.connectAndLogin(b.channel(), "ws1", pw);
        EXPECT_TRUE(l.ok()) << pw;
        EXPECT_TRUE(l.serverSignatureValid()) << pw << ": the broker signs with the ServerKey of the verifier that matched";
        EXPECT_FALSE(l.mustChangePassword());
    }
    EXPECT_FALSE(w.connectAndLogin(b.channel(), "ws1", "a third password").ok());
}

TEST(AuthLoginTest, RotationAddLoginDropOldFails) {
    AuthBroker b("rotate");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    b.addPassword("ws1", kPwB);
    AuthWireClient& w = b.wire();
    EXPECT_TRUE(w.connectAndLogin(b.channel(), "ws1", kPwB).ok()) << "the new password works while both are active";
    b.dropOldestPassword("ws1");
    EXPECT_FALSE(w.connectAndLogin(b.channel(), "ws1", kPwA).ok()) << "the dropped password is refused";
    EXPECT_TRUE(w.connectAndLogin(b.channel(), "ws1", kPwB).ok());
}

TEST(AuthLoginRegression, PreV9ClientIsRefusedAtConnectRegression) {
    LogCapture log;
    AuthBroker b("v8");
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel(), pc::kClientVersionCatalog);
    EXPECT_EQ(sid, -1);
    ASSERT_TRUE(w.lastConnect().has_value());
    EXPECT_EQ(w.lastConnect()->code, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion)) << "non-retriable for every client";
    EXPECT_EQ(w.lastConnect()->assignedSessionId, 0) << "no session was created";
    EXPECT_GE(log.count("auth: refused client version 8 at connect (authentication requires 9)"), 1);
    // A v9 client right after still works: the refusal created no state.
    EXPECT_TRUE(w.connectAndLogin(b.channel(), "admin", AuthBroker::kAdminPassword).ok());
}

// X103 (review I2): a bare pre-v9 Connect from any UDP sender is refused with a log line; a flood logs the first and every
// 1024th only.  RED: drop the due(5) gate - every refusal logs.
TEST(AuthLoginRegression, VersionRefusalLogLinesAreRateLimitedRegression) {
    LogCapture log;
    AuthBroker b("v8flood");
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    constexpr int kFlood = 40;
    for (int i = 0; i < kFlood; ++i) EXPECT_EQ(w.connect(b.channel(), pc::kClientVersionCatalog), -1);
    EXPECT_EQ(log.count("auth: refused client version 8 at connect"), 1) << "first only; the 1025th is beyond this flood";
}

TEST(AuthLoginRegression, RequestBeforeAuthenticationIsAnsweredAuthenticationFailedAndEndsTheSession) {
    AuthBroker b("preauth");
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(w.lastConnect().has_value());
    EXPECT_EQ(w.lastConnect()->authMode, pc::AuthMode::Required);
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate);
    const int64_t corr = 4242;
    ASSERT_TRUE(w.raw().produce(b.channel(), AuthWireClient::kClientStream, sid, corr, /*topicId=*/1, 0, 1, makeBatch(1)));
    const auto r = w.raw().awaitProduceResponse(corr);
    ASSERT_TRUE(r.has_value()) << "answered, never hung";
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed)) << "in the request's own response type";
    // The session is gone: the broker no longer knows it, so a Ping has nowhere to be answered (an unknown session has no
    // reply channel - the gateway counts it unanswerable; the plan's "UnknownSession" cannot be sent to nobody).
    const auto ping = w.raw().pingError(b.channel(), AuthWireClient::kClientStream, sid, std::chrono::steady_clock::now() + 1s);
    EXPECT_FALSE(ping.has_value());
    EXPECT_FALSE(b.node().gatewayHasSessionForTest(sid));
}

TEST(AuthLoginRegression, SessionThatNeverAuthenticatesIsEndedAtTheDeadline) {
    AuthBroker::Options o;
    o.authTimeoutMs = 300;
    AuthBroker b("deadline", o);
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    const int64_t connectCorr = w.lastConnect()->correlationId;
    const auto frame = w.raw().awaitAuthFrame(connectCorr, std::chrono::steady_clock::now() + 10s);
    ASSERT_TRUE(frame.has_value()) << "the deadline is announced, not silent";
    EXPECT_EQ(frame->templateId, static_cast<uint16_t>(pc::TemplateId::AuthServerFinal));
    const auto fin = pc::AuthServerFinal::decode(frame->bytes.data(), frame->bytes.size());
    ASSERT_TRUE(fin.has_value());
    EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(b.node().gatewayHasSessionForTest(sid));
}

TEST(AuthLoginRegression, RateLimitedLoginCarriesRetryAfter) {
    AuthBroker b("throttle");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    AuthWireClient& w = b.wire();
    for (int i = 0; i < 3; ++i) EXPECT_EQ(w.connectAndLogin(b.channel(), "ws1", "wrong password!").errorCode(),
                                          static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    const auto limited = w.login(b.channel(), sid, "ws1", {kPwA, kPwA});
    ASSERT_TRUE(limited.fin.has_value());
    EXPECT_EQ(limited.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_GT(limited.fin->retryAfterMs, 0u) << "D27: the client backs off at least this long";
    EXPECT_LE(limited.fin->retryAfterMs, 30'000u);
    EXPECT_FALSE(limited.first.has_value()) << "refused before any salt is revealed";
    EXPECT_FALSE(b.node().gatewayHasSessionForTest(sid));
    // The same user from nobody else is not delayed (no lockout) - here the delay simply passes.
    std::this_thread::sleep_for(std::chrono::milliseconds(limited.fin->retryAfterMs + 50));
    EXPECT_TRUE(w.connectAndLogin(b.channel(), "ws1", kPwA).ok());
}

TEST(AuthLoginTest, BootstrapAdminMustChangePasswordFirst) {
    AuthBroker b("mustchange");
    AuthWireClient& w = b.wire();
    const int64_t s0 = w.connect(b.channel());
    ASSERT_GT(s0, 0);
    const auto boot = w.bootstrap(b.channel(), s0, b.keyContents(), "admin", AuthBroker::kBootstrapPassword);
    ASSERT_EQ(boot.errorCode, 0) << boot.detail;
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    const auto login = w.login(b.channel(), sid, "admin", {AuthBroker::kBootstrapPassword, AuthBroker::kBootstrapPassword});
    ASSERT_TRUE(login.ok());
    EXPECT_TRUE(login.mustChangePassword()) << "the one-time password flags must-change";
    // A DefineTopic -> DefineTopicResponse{47}; the session stays.
    const auto refused = w.raw().defineTopicUntil(b.channel(), AuthWireClient::kClientStream, sid, "t1", 1, false, {},
                                                  std::chrono::steady_clock::now() + 10s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired));
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid));
    // Another account's password: refused the same way; its own: OK.
    const auto other = w.alterPassword(b.channel(), sid, "someone", pc::UserChangeKind::SetPassword, "another password 1");
    EXPECT_EQ(other.errorCode, static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired));
    const auto set = w.alterPassword(b.channel(), sid, "admin", pc::UserChangeKind::SetPassword, AuthBroker::kAdminPassword);
    ASSERT_EQ(set.errorCode, 0) << set.detail;
    // The same session is free now.
    const auto allowed = w.raw().defineTopicUntil(b.channel(), AuthWireClient::kClientStream, sid, "t1", 1, false, {},
                                                  std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(allowed.has_value());
    // Task 12: the must-change-password rule is gone (not 47) and the Authorizer answers instead: the admin holds ADMIN ON
    // CLUSTER, which grants no CREATE.
    EXPECT_EQ(allowed->errorCode, static_cast<uint16_t>(pc::ErrorCode::NotAuthorized))
        << "the same session is free of the rule: its DefineTopic is judged by its grants";
    // And the new password logs in without the flag.
    const auto again = w.connectAndLogin(b.channel(), "admin", AuthBroker::kAdminPassword);
    EXPECT_TRUE(again.ok());
    EXPECT_FALSE(again.mustChangePassword());
}

TEST(AuthLoginRegression, DevModeSessionsAreAnonymousAndNeverAskForCredentials) {
    AuthBroker::Options o;
    o.mode = AuthMode::DevDisabled;
    AuthBroker b("dev", o);
    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(w.lastConnect().has_value());
    EXPECT_TRUE(w.lastConnect()->v9);
    EXPECT_EQ(w.lastConnect()->authMode, pc::AuthMode::DevDisabled);
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::Anonymous);
    const auto def = w.raw().defineTopicUntil(b.channel(), AuthWireClient::kClientStream, sid, "t1", 1, false, {},
                                              std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(def.has_value());
    EXPECT_EQ(def->errorCode, 0) << "no login, full access";
    // A login attempt is answered (not silently ignored) and does not end the session.
    const auto l = w.login(b.channel(), sid, "ws1", {"x", "x"});
    EXPECT_TRUE(l.fin.has_value());
    EXPECT_NE(l.errorCode(), 0u);
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid));
}

// Issue #10: a Connect never resumes an authenticated session by naming its id - the plaintext resume-by-id (D11) let
// any UDP sender that knows a session id and its reply endpoint take it over.  Even from the same endpoint the hint is
// a fresh session that must log in, and the original session is untouched.  RED: restore resumeAuthenticated_.
TEST(AuthLoginRegression, AResumeHintNeverResumesAnAuthenticatedSessionRegression) {
    AuthBroker b("resume");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    AuthWireClient& w = b.wire();
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(w.login(b.channel(), sid, "ws1", {kPwA, kPwA}).ok());
    w.raw().setConnectHint(sid);
    const int64_t again = w.connect(b.channel());
    ASSERT_GT(again, 0);
    EXPECT_NE(again, sid) << "a new session, never the authenticated one";
    ASSERT_TRUE(w.lastConnect().has_value());
    EXPECT_EQ(w.lastConnect()->authMode, pc::AuthMode::Required);
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate) << "it must log in";
    const auto def = w.raw().defineTopicUntil(b.channel(), AuthWireClient::kClientStream, again, "t-resume", 1, false, {},
                                              std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(def.has_value());
    EXPECT_EQ(def->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << "not authenticated";
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid)) << "the original session is untouched";
}

TEST(AuthLoginRegression, LoginLogsNameTheReasonButTheWireDoesNot) {
    LogCapture log;
    AuthBroker b("logs");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    AuthWireClient& w = b.wire();
    const auto bad = w.connectAndLogin(b.channel(), "ws1", "wrong password!");
    const auto unknown = w.connectAndLogin(b.channel(), "nobody", kPwA);
    ASSERT_TRUE(bad.fin.has_value());
    ASSERT_TRUE(unknown.fin.has_value());
    EXPECT_GE(log.count("auth: login failed user=ws1 source=127.0.0.1"), 1);
    EXPECT_GE(log.count("reason=bad-proof"), 1);
    EXPECT_GE(log.count("auth: login failed user=nobody"), 1);
    EXPECT_GE(log.count("reason=unknown-user"), 1);
    for (const auto* l : {&bad, &unknown}) {
        const std::string wire(reinterpret_cast<const char*>(l->finalBytes.data()), l->finalBytes.size());
        for (const char* leak : {"bad-proof", "unknown-user", "unknown", "disabled", "proof", "password"})
            EXPECT_EQ(wire.find(leak), std::string::npos) << "the wire says nothing about why (" << leak << ")";
    }
    const auto ok = w.connectAndLogin(b.channel(), "ws1", kPwA);
    ASSERT_TRUE(ok.ok());
    EXPECT_GE(log.count("auth: login ok user=ws1 source=127.0.0.1"), 1);
    EXPECT_GE(log.count("authenticated over plaintext (no TLS): frames are not integrity-protected"), 1)
        << "D12: said once per plaintext session";
}

TEST(AuthLoginRegression, CountersCountOkFailedAndThrottled) {
    AuthBroker b("counters");
    b.bootstrapAdmin();   // the admin's one-time login = 1 ok
    b.createUser("ws1", kPwA);
    ASSERT_TRUE(b.awaitCounter(kLoginsOkCounterTypeId, 1)) << "registered; the bootstrap flow's one login";
    ASSERT_TRUE(b.awaitCounter(kLoginsFailedCounterTypeId, 0));
    ASSERT_TRUE(b.awaitCounter(kLoginsRateLimitedCounterTypeId, 0));
    AuthWireClient& w = b.wire();
    ASSERT_TRUE(w.connectAndLogin(b.channel(), "ws1", kPwA).ok());
    for (int i = 0; i < 3; ++i) ASSERT_FALSE(w.connectAndLogin(b.channel(), "nobody", "x").ok());
    const int64_t sid = w.connect(b.channel());
    const auto limited = w.login(b.channel(), sid, "nobody", {"x", "x"});
    ASSERT_GT(limited.fin->retryAfterMs, 0u);
    EXPECT_TRUE(b.awaitCounter(kLoginsOkCounterTypeId, 2));
    EXPECT_TRUE(b.awaitCounter(kLoginsFailedCounterTypeId, 3));
    EXPECT_TRUE(b.awaitCounter(kLoginsRateLimitedCounterTypeId, 1));
}
