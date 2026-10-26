// AuthLoginTest.cpp - authentication Task 11 against ONE real BrokerNode (authentication ENABLED, real Aeron UDP): the
// SCRAM login in the session handshake as raw frames (AuthWireClient + the rune-crypto client).  Failure
// indistinguishability (Review Focus 1), two active passwords, rotation, the version gate, the pre-authentication
// gate, the auth deadline, the throttle's retry-after, the must-change-password rule, dev mode, resume, the logs that
// name the reason and the wire that does not, and the 1'000'229-231 counters.
#include "support/AuthBroker.h"

#include <rune/node/RuneMetricSlot.h>
#include <rune/node/RuneMetrics.h>

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

// #136: the same parity for a user with TWO passwords (both slots carry a real salt): a wrong password for it, an
// unknown user and a disabled two-password user (right password) get byte-identical first-leg shapes and finals.
TEST(AuthLoginRegression, FailureAnswersAreIndistinguishableForATwoPasswordUserRegression) {
    AuthBroker b("indist2");
    b.bootstrapAdmin();
    b.createUser("two", kPwA);
    b.addPassword("two", kPwB);
    b.createUser("off2", kPwA);
    b.addPassword("off2", kPwB);
    b.disableUser("off2");
    AuthWireClient& w = b.wire();

    struct Case { const char* what; std::string user; std::array<std::string, 2> pw; };
    const std::vector<Case> cases = {{"wrong password, two-password user", "two", {"not the password", "nor this one"}},
                                     {"unknown user", "nobody", {kPwA, kPwB}},
                                     {"disabled two-password user (right passwords)", "off2", {kPwA, kPwB}}};
    std::vector<AuthWireClient::Login> out;
    for (const Case& c : cases) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        out.push_back(w.login(b.channel(), sid, c.user, c.pw));
        const auto& l = out.back();
        ASSERT_TRUE(l.first.has_value()) << c.what;
        EXPECT_EQ(l.first->count, 2) << c.what;
        for (int slot = 0; slot < 2; ++slot) {
            EXPECT_EQ(l.first->salts[slot].salt.size(), 16u) << c.what << " slot " << slot;
            EXPECT_EQ(l.first->salts[slot].iterations, 4096u) << c.what << " slot " << slot;
        }
        EXPECT_EQ(l.firstBytes.size(), out[0].firstBytes.size()) << c.what << ": one frame shape";
        ASSERT_TRUE(l.fin.has_value()) << c.what;
        EXPECT_EQ(l.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << c.what;
        EXPECT_EQ(l.fin->retryAfterMs, 0u) << c.what;
        EXPECT_TRUE(sameBodyDifferentHeader(l.finalBytes, out[0].finalBytes))
            << c.what << ": the AuthServerFinal is byte-identical except the header ids";
    }
    // The control: the enabled two-password user logs in through either password.
    for (const char* pw : {kPwA, kPwB}) {
        const auto ok = w.connectAndLogin(b.channel(), "two", pw);
        EXPECT_TRUE(ok.ok()) << pw;
        EXPECT_EQ(ok.firstBytes.size(), out[0].firstBytes.size()) << "the real user's first leg has the same shape";
    }
}

// #136: the login throttle is keyed on (user, source host), not on the session: six sessions open at once - over two
// client connections - each make ONE wrong-password attempt for the same user, and together they meet the throttle
// exactly as six consecutive attempts would: three verified failures, then refusals with a retry-after and no salt.
TEST(AuthLoginRegression, ParallelSessionsShareTheLoginThrottleRegression) {
    AuthBroker b("parthrottle");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    AuthWireClient& w1 = b.wire();
    AuthWireClient& w2 = b.wire();
    std::vector<std::pair<AuthWireClient*, int64_t>> sessions;
    for (int i = 0; i < 6; ++i) {   // every session exists before any attempt
        AuthWireClient& w = i % 2 == 0 ? w1 : w2;
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0) << i;
        sessions.emplace_back(&w, sid);
    }
    std::vector<int> verdicts;   // 0 = verified failure, 1 = throttled
    for (const auto& [w, sid] : sessions) {
        const auto l = w->login(b.channel(), sid, "ws1", {"a wrong guess", "a wrong guess"});
        ASSERT_TRUE(l.fin.has_value()) << "session " << sid;
        EXPECT_EQ(l.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        if (l.first.has_value() && l.fin->retryAfterMs == 0) verdicts.push_back(0);
        else if (!l.first.has_value() && l.fin->retryAfterMs > 0) verdicts.push_back(1);
        else verdicts.push_back(-1);
    }
    EXPECT_EQ(verdicts, (std::vector<int>{0, 0, 0, 1, 1, 1})) << "the sessions share one (user, host) throttle";
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

// Adversarial review of PR #522 (#131 / #132) over real UDP: first legs abandoned by a Disconnect are charged, and
// first legs whose finals land inside the throttle's delay are charged too (the refused final is no verdict) - so N
// parked first legs move the schedule by N failures, not by the 3 verified ones.  The known pair stays usable.
TEST(AuthLoginRegression, AbandonedAndThrottleRefusedFirstLegsAreChargedOnARealBrokerAdversarialRegression) {
    AuthBroker b("abandoned");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    b.createUser("ws2", kPwA);
    AuthWireClient& w = b.wire();
    // (a) three first legs ended by a Disconnect: the third failure of the (ws1, host) pair starts its delay.
    for (int i = 0; i < 3; ++i) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        const auto l = w.login(b.channel(), sid, "ws1", {kPwA, kPwA}, 0, /*sendFinal=*/false);
        ASSERT_TRUE(l.first.has_value()) << i;
        ASSERT_TRUE(w.raw().disconnect(b.channel(), AuthWireClient::kClientStream, sid));
    }
    {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        const auto limited = w.login(b.channel(), sid, "ws1", {kPwA, kPwA});
        ASSERT_TRUE(limited.fin.has_value());
        EXPECT_FALSE(limited.first.has_value()) << "three abandoned first legs put the pair inside its delay";
        EXPECT_GT(limited.fin->retryAfterMs, 0u);
    }
    // (b) eight parked first legs for ws2, then eight wrong finals: 3 verified, 5 refused - and charged.
    std::vector<AuthWireClient::Login> parked;
    for (int i = 0; i < 8; ++i) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        parked.push_back(w.login(b.channel(), sid, "ws2", {kPwA, kPwA}, 0, /*sendFinal=*/false));
        ASSERT_TRUE(parked.back().first.has_value()) << i;
    }
    int verified = 0, limited = 0;
    for (auto& l : parked) {
        w.finish(b.channel(), l, "ws2", {"a wrong guess", "another wrong guess"});
        ASSERT_TRUE(l.fin.has_value());
        EXPECT_EQ(l.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        if (l.fin->retryAfterMs > 0) ++limited; else ++verified;
    }
    EXPECT_EQ(verified, 3);
    EXPECT_EQ(limited, 5);
    const int64_t sid = w.connect(b.channel());
    ASSERT_GT(sid, 0);
    const auto after = w.login(b.channel(), sid, "ws2", {kPwA, kPwA});
    ASSERT_TRUE(after.fin.has_value());
    EXPECT_FALSE(after.first.has_value());
    // 8 pair failures: min(250 ms << 5, 30 s) = 8 s from the last charge; 3 failures alone would be 250 ms.
    EXPECT_GT(after.fin->retryAfterMs, 4'000u) << "the refused finals' first legs were charged";
    EXPECT_LE(after.fin->retryAfterMs, 8'000u);
}

TEST(AuthLoginRegression, CountersCountOkFailedAndThrottled) {
    AuthBroker b("counters");
    b.bootstrapAdmin();   // the admin's one-time login = 1 ok
    b.createUser("ws1", kPwA);
    ASSERT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsOk), 1)) << "registered; the bootstrap flow's one login";
    ASSERT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsFailed), 0));
    ASSERT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsRateLimited), 0));
    AuthWireClient& w = b.wire();
    ASSERT_TRUE(w.connectAndLogin(b.channel(), "ws1", kPwA).ok());
    for (int i = 0; i < 3; ++i) ASSERT_FALSE(w.connectAndLogin(b.channel(), "nobody", "x").ok());
    const int64_t sid = w.connect(b.channel());
    const auto limited = w.login(b.channel(), sid, "nobody", {"x", "x"});
    ASSERT_GT(limited.fin->retryAfterMs, 0u);
    EXPECT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsOk), 2));
    EXPECT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsFailed), 3));
    EXPECT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsRateLimited), 1));
}

// ── arcanum-metrics plan Task 9 (spec §5.3; D58, D59): the three metric writers ────────────────────────────────
namespace {

/// A broker whose provisioner builds on the first demand: a 1-byte cap makes every build unpreallocated, a 1%
/// threshold posts a demand after ~10 KiB of a 1 MiB segment.
AuthBroker::Options provisionOnFirstDemand() {
    AuthBroker::Options o;
    o.configure = [](NodeConfig& c) {
        c.segmentProvisionCapBytes = 1;
        c.segmentProvisionThresholdPct = 1;
    };
    return o;
}

/// Creates a user that may create and write `topic`, logs it in, defines the 1-partition topic and produces until
/// the provisioner publishes an unpreallocated build (bounded).  `eachRound` runs after every produce (e.g. an
/// observer publish).
template <typename F>
::testing::AssertionResult driveProvisioner(AuthBroker& b, const std::string& topic, F&& eachRound) {
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    using SP = rune::protocol::SecurityPatternKind;
    const std::string user = topic + "_writer";
    b.createUser(user, kPwA, {AuthGrant{SO::Create, ST::Topic, SP::Exact, topic},
                              AuthGrant{SO::Write, ST::Topic, SP::Exact, topic}});
    AuthWireClient& w = b.wire();   // one client: wire() makes a fresh one per call
    const auto login = w.connectAndLogin(b.channel(), user, kPwA);
    if (!login.ok()) return ::testing::AssertionFailure() << "login " << user << " failed";
    const int64_t sid = login.sessionId;
    ShardTestClient& c = w.raw();
    const auto def = c.defineTopicUntil(b.channel(), AuthWireClient::kClientStream, sid, topic, 1, false, {},
                                        ShardTestClient::Clock::now() + std::chrono::seconds(20));
    if (!def || def->errorCode != 0)
        return ::testing::AssertionFailure() << "define " << topic << " failed: " << (def ? def->errorCode : -1);
    const auto built = [&] {
        const auto v = b.counter(typeIdOf(RuneMetric::ProvisionUnpreallocBuilds));
        return v && *v >= 1;
    };
    // ~96 KiB per batch (under the client's 128 KiB message cap): the default 64 MiB roll's 1% threshold (~671 KB)
    // is crossed within a few produces.
    const std::string value(4096, 'p');
    rune::protocol::RecordBatchBuilder bb(0, 1000);
    for (int i = 0; i < 24; ++i) bb.append("k", value, 1000 + i);
    const auto batch = bb.build();
    for (int i = 0; i < 60 && !built(); ++i) {
        if (!c.produceUntilOk(b.channel(), AuthWireClient::kClientStream, sid, def->topicId, 0, 1, batch))
            return ::testing::AssertionFailure() << "produce " << i << " failed";
        eachRound();
    }
    for (int i = 0; i < 500 && !built(); ++i) {   // the build itself runs on the provisioner thread
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        eachRound();
    }
    if (!built()) return ::testing::AssertionFailure() << "the provisioner never published an unpreallocated build";
    return ::testing::AssertionSuccess();
}

}  // namespace

// D58: every kRuneMetrics row is written only by its declared MetricOwner - the control thread, the role observer
// (publishRole) or the segment provisioner - across a run that moves all three: the election, logins ok and failed,
// a topic DDL, and produce that drives the provisioner.  The debug owner-tag record holds one bit per writer; a row
// written by a foreign owner also trips the slot's assert.  (Suite BrokerMetricsRegression so it runs with the
// metrics regression set; it lives here for the authenticated wire rig.)
// RED: hand the provisioner the observer's role slot (BrokerNode's provSlots) -> the role row's record gains the
// Provisioner bit and the assert fires.
TEST(BrokerMetricsRegression, OwnerTableRegression) {
#ifdef NDEBUG
    GTEST_SKIP() << "the owner-tag record is a debug-build check (D58)";
#else
    AuthBroker b("owner_table", provisionOnFirstDemand());
    b.bootstrapAdmin();
    b.createUser("ot1", kPwA);
    AuthWireClient& w = b.wire();
    ASSERT_TRUE(w.connectAndLogin(b.channel(), "ot1", kPwA).ok());
    ASSERT_FALSE(w.connectAndLogin(b.channel(), "ot1", "not the password").ok());
    ASSERT_TRUE(driveProvisioner(b, "ottopic", [] {}));
    b.node().publishRole();
    ASSERT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsFailed), 1));

    for (const RuneMetricDef& d : kRuneMetrics) {
        const std::uint8_t by = b.node().metricWrittenByForTest(d.id);
        EXPECT_EQ(by & static_cast<std::uint8_t>(~metricOwnerBit(d.owner)), 0)
            << "row " << typeIdOf(d.id) << " (" << d.labelPrefix << ") written by owners 0x" << std::hex << int(by);
    }
    // Not vacuous: each of the three writers wrote its rows in this run.
    for (RuneMetric m : {RuneMetric::Role, RuneMetric::Term, RuneMetric::OutOfSyncPartitions}) {
        EXPECT_EQ(b.node().metricWrittenByForTest(m), metricOwnerBit(MetricOwner::Observer)) << typeIdOf(m);
    }
    EXPECT_EQ(b.node().metricWrittenByForTest(RuneMetric::ProvisionUnpreallocBuilds),
              metricOwnerBit(MetricOwner::Provisioner));
    for (RuneMetric m : {RuneMetric::LoginsOk, RuneMetric::LoginsFailed, RuneMetric::CatalogAppliedOffset}) {
        EXPECT_EQ(b.node().metricWrittenByForTest(m), metricOwnerBit(MetricOwner::Control)) << typeIdOf(m);
    }
    EXPECT_EQ(b.counter(typeIdOf(RuneMetric::Role)), static_cast<std::int64_t>(RoleCode::Leader));
#endif
}

// D59: the control thread (logins, DDL), the observer (publishRole on this thread) and the provisioner (builds) write
// their own rows while a reader thread scans every row of the file.  Each read is a whole, sane value.  The race
// detector is the point: this runs under TSan at checkpoint B.
TEST(BrokerMetricsRegression, ThreeWritersUnderTsanRegression) {
    AuthBroker b("three_writers", provisionOnFirstDemand());
    b.bootstrapAdmin();
    const std::string path = b.node().metricsFilePath();
    const std::int32_t nodeId = b.node().nodeIdForTest();
    std::atomic<bool> stop{false};
    std::atomic<int> scans{0};
    std::atomic<int> bad{0};
    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            for (const RuneMetricDef& d : kRuneMetrics) {
                const auto v = rune::node::test::readRuneMetricAt(path, typeIdOf(d.id), nodeId);
                if (!v) { bad.fetch_add(1, std::memory_order_relaxed); continue; }
                if (d.id == RuneMetric::Role && (*v < -1 || *v > 2)) bad.fetch_add(1, std::memory_order_relaxed);
                if (d.owner == MetricOwner::Provisioner && *v < 0) bad.fetch_add(1, std::memory_order_relaxed);
            }
            scans.fetch_add(1, std::memory_order_relaxed);
        }
    });
    AuthWireClient& w = b.wire();
    for (int i = 0; i < 2; ++i) EXPECT_FALSE(w.connectAndLogin(b.channel(), "nobody" + std::to_string(i), "x").ok());
    const auto r = driveProvisioner(b, "twtopic", [&] { b.node().publishRole(); });
    for (int i = 0; i < 200; ++i) b.node().publishRole();
    for (int i = 0; i < 1000 && scans.load(std::memory_order_relaxed) < 3; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    stop.store(true, std::memory_order_release);
    reader.join();
    ASSERT_TRUE(r);
    EXPECT_GE(scans.load(), 3);
    EXPECT_EQ(bad.load(), 0) << "a row was missing or out of range mid-write";
    EXPECT_EQ(b.counter(typeIdOf(RuneMetric::Role)), static_cast<std::int64_t>(RoleCode::Leader));
    EXPECT_TRUE(b.awaitCounter(typeIdOf(RuneMetric::LoginsFailed), 2));
}
