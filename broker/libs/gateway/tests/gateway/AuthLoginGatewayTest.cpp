// AuthLoginGatewayTest.cpp - authentication Task 11 at the gateway (no Aeron): the SCRAM login state machine of a
// session, the pre-authentication gate, the throttle, the auth deadline, the must-change-password rule, the counters
// and the logs.  makeForTest + the apply*ForTest seams; the "client" is ScramTestClient.
#include "support/ScramTestClient.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <random>

using namespace rune::gateway;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

struct RecordingSink final : IAdminRequestSink {
    int securityDdls = 0, otherSubmits = 0;
    std::string lastAuthenticated;
    AdminSubmit submitDefineTopic(int64_t, int64_t, std::string_view, uint32_t, std::string_view, bool, const uint8_t*, uint32_t,
                                  uint8_t, int16_t, const rune::protocol::TopicConfigView&, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t) override { return true; }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitCatalogSubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitCatalogUnsubscribe(int64_t, int64_t) override { return true; }
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl&, const DdlMetaView& m) override {
        ++securityDdls; lastAuthenticated = std::string(m.authenticated); return AdminSubmit::Accepted;
    }
    AdminSubmit submitSecurityBootstrap(const pc::SecurityBootstrap&, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    bool submitSecuritySubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitSecurityUnsubscribe(int64_t, int64_t) override { return true; }
};

struct Frame { uint16_t tid = 0; int64_t sid = 0, corr = 0; std::vector<uint8_t> bytes; };

struct TempDir {
    fs::path path;
    TempDir() { std::random_device rd; path = fs::temp_directory_path() / ("rune-auth-gw-" + std::to_string(rd())); fs::create_directories(path); }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Rig {
    TempDir dir;
    std::unique_ptr<TopicRegistry> registry;
    SecurityEventChain chain;
    SecurityView view;
    ClientGateway gw;
    RecordingSink admin;
    std::vector<Frame> seen;
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink;
    std::shared_ptr<spdlog::logger> previous;
    int64_t corr = 100;

    explicit Rig(SecurityEventChain c = makeAuthTestChain())
        : chain(std::move(c)), view(chain.state()),
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { onFrame(b, n); return 1; }, 1)) {
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setAdminSink(&admin);
        gw.setAuthMode(GatewayAuthMode::Required, &view);
        gw.setAuthTimeoutNs(1'000'000'000);
        logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
        previous = spdlog::default_logger();
        auto capture = std::make_shared<spdlog::logger>("auth-gw-capture", logSink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~Rig() { spdlog::set_default_logger(previous); }
    [[nodiscard]] bool logContains(const std::string& text) const {
        for (const std::string& l : logSink->last_formatted()) if (l.find(text) != std::string::npos) return true;
        return false;
    }
    void onFrame(const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        seen.push_back(Frame{h->templateId, h->sessionId, h->correlationId, std::vector<uint8_t>(b, b + n)});
    }
    /// A new session awaiting its login from `host`.
    int64_t newSession(const std::string& host = "10.0.0.9", int64_t now = 0) {
        const int64_t sid = gw.injectTestSession(now, pc::kClientVersionAuth);
        gw.setSessionAuthForTest(sid, AuthAwaitingFirst{now + 1'000'000'000, 50});
        gw.setSessionSourceHostForTest(sid, host);
        return sid;
    }
    [[nodiscard]] const Frame* last(uint16_t tid, int64_t sid) const {
        for (auto it = seen.rbegin(); it != seen.rend(); ++it) if (it->tid == tid && it->sid == sid) return &*it;
        return nullptr;
    }
    void send(const std::vector<uint8_t>& f) { gw.applyClientFrameForTest(f.data(), f.size()); }
    struct Outcome { std::optional<pc::AuthServerFinal> fin; std::vector<uint8_t> finBytes, firstBytes; int64_t sid = 0; };
    /// The whole exchange; passwords[i] tried against slot i.
    Outcome login(const std::string& user, const std::array<std::string, 2>& passwords, const std::string& host = "10.0.0.9") {
        Outcome o;
        o.sid = newSession(host, gw_now);
        const std::string nonce = "nonce-" + std::to_string(++corr);
        pc::AuthClientFirst cf;
        cf.sessionId = o.sid; cf.correlationId = ++corr; cf.username = user; cf.clientNonce = nonce;
        std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
        const std::size_t before = seen.size();
        send(b);
        const Frame* f1 = nullptr;
        for (std::size_t i = before; i < seen.size(); ++i) if (seen[i].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFirst)) f1 = &seen[i];
        const Frame* fin = nullptr;
        if (f1 != nullptr) {
            o.firstBytes = f1->bytes;
            const auto first = pc::AuthServerFirst::decode(f1->bytes.data(), f1->bytes.size());
            auto ans = scramAnswer(user, nonce, *first, passwords, o.sid, cf.correlationId);
            std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
            const std::size_t mid = seen.size();
            send(fb);
            for (std::size_t i = mid; i < seen.size(); ++i) if (seen[i].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFinal)) fin = &seen[i];
        } else {
            for (std::size_t i = before; i < seen.size(); ++i) if (seen[i].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFinal)) fin = &seen[i];
        }
        if (fin != nullptr) { o.finBytes = fin->bytes; o.fin = pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size()); }
        return o;
    }
    int64_t gw_now = 0;
};

std::vector<uint8_t> defineTopicFrame(int64_t sid, int64_t corr) {
    pc::DefineTopic m;
    m.sessionId = sid; m.correlationId = corr; m.topicName = "t"; m.partitionCount = 1;
    std::vector<uint8_t> b(m.encodedSize() + 64); b.resize(m.encode(b.data())); return b;
}

std::vector<uint8_t> alterUserFrame(int64_t sid, int64_t corr, const std::string& name, pc::UserChangeKind change) {
    pc::SecurityDdl m;
    m.sessionId = sid; m.correlationId = corr; m.ext.clientName = "t"; m.ext.requestId = {1, static_cast<uint64_t>(corr)};
    m.kind = pc::SecurityOpKind::AlterUser; m.name = name; m.change = change;
    const auto v = scramVerifierOf("new password 12345", 7);
    m.verifier = pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(v.salt.data()), 16), 4096,
        std::string_view(reinterpret_cast<const char*>(v.storedKey.data()), 64),
        std::string_view(reinterpret_cast<const char*>(v.serverKey.data()), 64)};
    std::vector<uint8_t> b(m.encodedSize()); b.resize(m.encode(b.data())); return b;
}

}  // namespace

TEST(SessionAuthRegression, GateForIsTheOneRuleOfWhatAnUnauthenticatedSessionMaySend) {
    using T = pc::TemplateId;
    const SessionAuth first = AuthAwaitingFirst{}, final = AuthAwaitingFinal{}, pcr = AuthPasswordChangeRequired{7, "u"},
                      authed = AuthAuthenticated{7, "u", PermissionTable::allowAll()}, anon = AuthAnonymous{};
    for (const SessionAuth* a : {&first, &final, &pcr, &authed, &anon})
        for (const T t : {T::Connect, T::HandshakeContinue, T::Keepalive, T::Ping, T::Disconnect})
            EXPECT_EQ(gateFor(*a, t, false, false), AuthGate::Allow) << "session-level frames are never gated";
    EXPECT_EQ(gateFor(first, T::AuthClientFirst, false, false), AuthGate::Allow);
    EXPECT_EQ(gateFor(first, T::AuthClientFinal, false, false), AuthGate::AnswerAuthFailedAndEnd);
    EXPECT_EQ(gateFor(first, T::SecurityBootstrap, false, false), AuthGate::AnswerAuthFailedAndEnd) << "window closed";
    EXPECT_EQ(gateFor(first, T::SecurityBootstrap, true, false), AuthGate::Allow) << "window open";
    EXPECT_EQ(gateFor(first, T::Produce, true, false), AuthGate::AnswerAuthFailedAndEnd);
    EXPECT_EQ(gateFor(final, T::AuthClientFinal, false, false), AuthGate::Allow);
    EXPECT_EQ(gateFor(final, T::AuthClientFirst, false, false), AuthGate::AnswerAuthFailedAndEnd);
    EXPECT_EQ(gateFor(final, T::SecurityBootstrap, true, false), AuthGate::AnswerAuthFailedAndEnd);
    EXPECT_EQ(gateFor(pcr, T::SecurityDdl, false, true), AuthGate::Allow) << "ALTER USER <self> SET PASSWORD";
    EXPECT_EQ(gateFor(pcr, T::SecurityDdl, false, false), AuthGate::AnswerPasswordChangeRequired);
    EXPECT_EQ(gateFor(pcr, T::DefineTopic, false, true), AuthGate::AnswerPasswordChangeRequired);
    EXPECT_EQ(gateFor(pcr, T::AuthClientFirst, false, false), AuthGate::AnswerPasswordChangeRequired);
    EXPECT_EQ(gateFor(authed, T::DefineTopic, false, false), AuthGate::Allow);
    EXPECT_EQ(gateFor(anon, T::Produce, false, false), AuthGate::Allow);
}

TEST(AuthLoginGatewayTest, ALoginWithAnyActivePasswordAuthenticatesTheSessionAndCountsIt) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_TRUE(a.fin.has_value());
    EXPECT_EQ(a.fin->errorCode, 0);
    EXPECT_EQ(a.fin->flags, 0);
    EXPECT_EQ(a.fin->serverSignature.size(), 64u);
    const auto* st = std::get_if<AuthAuthenticated>(rig.gw.sessionAuthForTest(a.sid));
    ASSERT_NE(st, nullptr);
    EXPECT_EQ(st->user, "ws1");
    const auto b = rig.login("ws1", {"", kTestPasswordB});
    EXPECT_EQ(b.fin->errorCode, 0) << "the second active password logs in too (D5)";
    EXPECT_EQ(rig.gw.loginsOk(), 2u);
    EXPECT_EQ(rig.gw.loginsFailed(), 0u);
    EXPECT_TRUE(rig.logContains("auth: login ok user=ws1"));
    EXPECT_TRUE(rig.logContains("authenticated over plaintext (no TLS): frames are not integrity-protected"));
}

TEST(AuthLoginRegression, FailureAnswersAreIndistinguishableOnTheWireRegression) {
    Rig rig;
    const auto wrong = rig.login("ws2", {"not the password", ""});
    const auto unknown = rig.login("nobody", {kTestPasswordA, kTestPasswordA});
    const auto disabled = rig.login("off", {kTestPasswordA, ""});
    const auto empty = rig.login("", {kTestPasswordA, kTestPasswordA});
    for (const auto* o : {&wrong, &unknown, &disabled, &empty}) {
        ASSERT_TRUE(o->fin.has_value());
        EXPECT_EQ(o->fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        EXPECT_EQ(o->fin->retryAfterMs, 0u);
        EXPECT_EQ(o->fin->detail, "authentication failed");
        EXPECT_FALSE(rig.gw.hasSessionForTest(o->sid)) << "a failed login ends the session";
        ASSERT_FALSE(o->firstBytes.empty());
        const auto first = pc::AuthServerFirst::decode(o->firstBytes.data(), o->firstBytes.size());
        EXPECT_EQ(first->count, 2) << "review A";
        EXPECT_EQ(first->salts[0].salt.size(), 16u);
        EXPECT_EQ(first->salts[0].iterations, 4096u);
        EXPECT_EQ(o->firstBytes.size(), wrong.firstBytes.size()) << "AuthServerFirst has one shape";
    }
    // The AuthServerFinal frames are byte-identical except the header's session and correlation ids.
    for (const auto* o : {&unknown, &disabled, &empty}) {
        ASSERT_EQ(o->finBytes.size(), wrong.finBytes.size());
        EXPECT_TRUE(std::equal(o->finBytes.begin() + pc::MessageHeader::ENCODED_SIZE, o->finBytes.end(),
                               wrong.finBytes.begin() + pc::MessageHeader::ENCODED_SIZE))
            << "the body (code, flags, retry, detail) is identical";
    }
    EXPECT_EQ(rig.gw.loginsFailed(), 4u);
    // The logs name the reason; the wire does not.
    EXPECT_TRUE(rig.logContains("reason=bad-proof"));
    EXPECT_TRUE(rig.logContains("reason=unknown-user"));
    EXPECT_TRUE(rig.logContains("reason=disabled"));
    EXPECT_TRUE(rig.logContains("reason=no-credentials"));
}

TEST(AuthLoginRegression, RateLimitedLoginCarriesRetryAfterAndIsNotCountedAsAFailure) {
    Rig rig;
    for (int i = 0; i < 3; ++i) (void)rig.login("ws2", {"wrong password", ""});
    EXPECT_EQ(rig.gw.loginsFailed(), 3u);
    // The 3rd failure (2 free) delays the next attempt by 250 ms: an attempt right now is answered rate-limited.
    const auto limited = rig.login("ws2", {kTestPasswordA, ""});
    ASSERT_TRUE(limited.fin.has_value());
    EXPECT_EQ(limited.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_GT(limited.fin->retryAfterMs, 0u);
    EXPECT_LE(limited.fin->retryAfterMs, 250u);
    EXPECT_TRUE(limited.firstBytes.empty()) << "refused before any salt is revealed";
    EXPECT_EQ(rig.gw.loginsRateLimited(), 1u);
    EXPECT_EQ(rig.gw.loginsFailed(), 3u) << "an attempt inside the delay is not counted";
    EXPECT_TRUE(rig.logContains("auth: rate limited user=ws2 source=10.0.0.9 retry_after_ms="));
    // The same user from ANOTHER address is untouched (no lockout), and waiting it out works.
    EXPECT_EQ(rig.login("ws2", {kTestPasswordA, ""}, "10.0.0.77").fin->errorCode, 0);
    rig.gw_now = 300'000'000; rig.gw.setNowNsForTest(300'000'000);
    EXPECT_EQ(rig.login("ws2", {kTestPasswordA, ""}).fin->errorCode, 0);
}

TEST(AuthLoginRegression, SessionThatNeverAuthenticatesIsEndedAtTheDeadlineRegression) {
    Rig rig;
    const int64_t sid = rig.newSession();
    rig.gw.reapExpiredForTest(500'000'000);
    EXPECT_TRUE(rig.gw.hasSessionForTest(sid)) << "inside the deadline";
    rig.gw.reapExpiredForTest(1'000'000'001);
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), sid);
    ASSERT_NE(f, nullptr) << "answered, never silently dropped";
    const auto fin = pc::AuthServerFinal::decode(f->bytes.data(), f->bytes.size());
    EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_EQ(f->corr, 50) << "carries the Connect's correlation id (the client matches the episode by it)";
    EXPECT_TRUE(rig.logContains("reason=timeout"));
    // Mid-exchange too: a session stuck after AuthClientFirst.
    const int64_t mid = rig.newSession();
    pc::AuthClientFirst cf; cf.sessionId = mid; cf.correlationId = 61; cf.username = "ws1"; cf.clientNonce = "n";
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data())); rig.send(b);
    ASSERT_TRUE(std::holds_alternative<AuthAwaitingFinal>(*rig.gw.sessionAuthForTest(mid)));
    rig.gw.reapExpiredForTest(2'000'000'000);
    EXPECT_FALSE(rig.gw.hasSessionForTest(mid));
}

TEST(AuthLoginRegression, RequestBeforeAuthenticationIsAnsweredAuthenticationFailedAndEndsTheSessionRegression) {
    Rig rig;
    const int64_t sid = rig.newSession();
    pc::Produce m;
    m.sessionId = sid; m.correlationId = 9; m.topicId = 1; m.partition = 0; m.partitionKnown = 1; m.acks = 1; m.producerId = -1;
    m.baseSequence = -1;
    rune::protocol::RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    std::vector<uint8_t> batch = rb.build();
    m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> b(m.encodedSize()); b.resize(m.encode(b.data()));
    rig.send(b);
    const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::ProduceResponse), sid);
    ASSERT_NE(f, nullptr) << "in the request's own response type";
    const auto r = pc::ProduceResponse::decode(f->bytes.data(), f->bytes.size());
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    // A request in AwaitingFinal ends it too (only AuthClientFinal is allowed there).
    const int64_t s2 = rig.newSession();
    pc::AuthClientFirst cf; cf.sessionId = s2; cf.correlationId = 61; cf.username = "ws1"; cf.clientNonce = "n";
    std::vector<uint8_t> cb(cf.encodedSize()); cb.resize(cf.encode(cb.data())); rig.send(cb);
    rig.send(defineTopicFrame(s2, 62));
    EXPECT_FALSE(rig.gw.hasSessionForTest(s2));
    EXPECT_EQ(rig.admin.otherSubmits, 0);
}

TEST(AuthLoginTest, ASessionThatMustChangeItsPasswordMayOnlyChangeItsOwnAndThenIsFree) {
    Rig rig;
    const auto o = rig.login("admin", {kTestAdminPassword, ""});
    ASSERT_TRUE(o.fin.has_value());
    EXPECT_EQ(o.fin->errorCode, 0);
    EXPECT_EQ(o.fin->flags & pc::kAuthFlagMustChangePassword, pc::kAuthFlagMustChangePassword);
    ASSERT_TRUE(std::holds_alternative<AuthPasswordChangeRequired>(*rig.gw.sessionAuthForTest(o.sid)));
    // A DefineTopic -> DefineTopicResponse{47}, the session STAYS.
    rig.send(defineTopicFrame(o.sid, 70));
    const Frame* d = rig.last(static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse), o.sid);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(pc::DefineTopicResponse::decode(d->bytes.data(), d->bytes.size())->errorCode,
              static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired));
    EXPECT_TRUE(rig.gw.hasSessionForTest(o.sid));
    EXPECT_EQ(rig.admin.otherSubmits, 0);
    // Someone else's password, or a non-SET change: refused the same way.
    rig.send(alterUserFrame(o.sid, 71, "ws1", pc::UserChangeKind::SetPassword));
    rig.send(alterUserFrame(o.sid, 72, "admin", pc::UserChangeKind::AddPassword));
    EXPECT_EQ(rig.admin.securityDdls, 0);
    for (const int64_t c : {71, 72}) {
        const Frame* f = nullptr;
        for (const Frame& x : rig.seen) if (x.corr == c) f = &x;
        ASSERT_NE(f, nullptr) << c;
        EXPECT_EQ(pc::DdlResponse::decode(f->bytes.data(), f->bytes.size())->errorCode,
                  static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired)) << c;
    }
    // ALTER USER admin SET PASSWORD reaches the admin ring, stamped with the user.
    rig.send(alterUserFrame(o.sid, 73, "admin", pc::UserChangeKind::SetPassword));
    EXPECT_EQ(rig.admin.securityDdls, 1);
    EXPECT_EQ(rig.admin.lastAuthenticated, "admin");
    EXPECT_TRUE(std::holds_alternative<AuthPasswordChangeRequired>(*rig.gw.sessionAuthForTest(o.sid))) << "not before it applied";
    // The SecurityView applies the SetPassword (mustChangePassword cleared): the session is free.
    const auto ev = rig.chain.emit(rune::security::AlterUserOps{rune::security::SetPassword{scramVerifierOf("new password 12345", 7), false}},
                                   "admin", "ALTER USER admin SET PASSWORD '<redacted>';");
    const auto bytes = rune::security::encode(ev);
    ASSERT_TRUE(bytes.has_value());
    auto applied = rig.view.onApplied(static_cast<int64_t>(rig.chain.count()) - 1, *bytes);
    ASSERT_TRUE(applied.has_value()) << applied.error().detail;
    rig.gw.onSecurityApplied(*applied);
    EXPECT_TRUE(std::holds_alternative<AuthAuthenticated>(*rig.gw.sessionAuthForTest(o.sid)));
    rig.send(defineTopicFrame(o.sid, 74));
    // Task 12: free of the must-change rule, the session is judged by its grants (admin holds ADMIN, not CREATE).
    EXPECT_EQ(rig.admin.otherSubmits, 0) << "NotAuthorized, not forwarded";
    const Frame* denied = rig.last(static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse), o.sid);
    ASSERT_NE(denied, nullptr);
    EXPECT_EQ(pc::DefineTopicResponse::decode(denied->bytes.data(), denied->bytes.size())->errorCode,
              static_cast<uint16_t>(pc::ErrorCode::NotAuthorized));
}

TEST(AuthLoginRegression, ALoginAgainstAHaltedSecurityLogFailsClosedRegression) {
    Rig rig;
    rune::security::ApplyError err; err.offset = 3; err.detail = "test halt";
    rig.view.onHalted(err);
    const auto o = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_TRUE(o.fin.has_value());
    EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_TRUE(rig.logContains("reason=security-halted"));
    EXPECT_FALSE(rig.gw.hasSessionForTest(o.sid));
}

TEST(AuthLoginRegression, ADevOrInProcessGatewayNeverGatesAndAnswersALoginAttemptWithoutEndingTheSession) {
    Rig rig;
    rig.gw.setAuthMode(GatewayAuthMode::Dev, &rig.view);
    const int64_t sid = rig.gw.injectTestSession(0, pc::kClientVersionAuth);   // AuthAnonymous
    pc::AuthClientFirst cf; cf.sessionId = sid; cf.correlationId = 5; cf.username = "ws1"; cf.clientNonce = "n";
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data())); rig.send(b);
    EXPECT_TRUE(rig.gw.hasSessionForTest(sid));
    const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), sid);
    ASSERT_NE(f, nullptr);
    EXPECT_NE(pc::AuthServerFinal::decode(f->bytes.data(), f->bytes.size())->errorCode, 0);
    rig.send(defineTopicFrame(sid, 6));
    EXPECT_EQ(rig.admin.otherSubmits, 1) << "anonymous: allowed";
}

// The plan's 5 ms login gate, from the broker's side: the SERVER half of a full login (AuthClientFirst + AuthClientFinal
// handled on the control thread) is hashes and HMACs only - the server never runs PBKDF2 - so it costs microseconds and
// can never stall the control loop.  (The end-to-end wire gate is ClientLoginBrokerRegression's AuthGatesRegression.)
TEST(AuthGatesRegression, ServerSideLoginCostIsMicrosecondsNotMillisecondsRegression) {
    Rig rig;
    // Precompute the client's side so only the broker's work is timed.
    const std::string nonce = "bench-nonce-0123456789";
    std::vector<double> usec;
    for (int i = 0; i < 100; ++i) {
        const int64_t sid = rig.newSession("10.0.0." + std::to_string(i % 200));
        pc::AuthClientFirst cf; cf.sessionId = sid; cf.correlationId = 1000 + i; cf.username = "ws2"; cf.clientNonce = nonce;
        std::vector<uint8_t> first(cf.encodedSize()); first.resize(cf.encode(first.data()));
        const std::size_t before = rig.seen.size();
        const auto t0 = std::chrono::steady_clock::now();
        rig.send(first);
        const auto t1 = std::chrono::steady_clock::now();
        const Frame* f1 = nullptr;
        for (std::size_t k = before; k < rig.seen.size(); ++k) if (rig.seen[k].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFirst)) f1 = &rig.seen[k];
        ASSERT_NE(f1, nullptr);
        const auto sf = pc::AuthServerFirst::decode(f1->bytes.data(), f1->bytes.size());
        auto ans = scramAnswer("ws2", nonce, *sf, {kTestPasswordA, ""}, sid, cf.correlationId);
        std::vector<uint8_t> fin(ans->final.encodedSize()); fin.resize(ans->final.encode(fin.data()));
        const auto t2 = std::chrono::steady_clock::now();
        rig.send(fin);
        const auto t3 = std::chrono::steady_clock::now();
        ASSERT_TRUE(std::holds_alternative<AuthAuthenticated>(*rig.gw.sessionAuthForTest(sid)));
        usec.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count() + std::chrono::duration<double, std::micro>(t3 - t2).count());
    }
    std::sort(usec.begin(), usec.end());
    std::fprintf(stderr, "[ login ] server-side cost of a full SCRAM login (Debug build): p50 = %.1f us, p99 = %.1f us\n",
                 usec[50], usec[99]);
    EXPECT_LT(usec[50], 2000.0) << "far below the 5 ms gate: no PBKDF2 on the control thread";
}

// Review C: an unknown user's fake salts advertise the iteration count of the MOST COMMON real account, so a cluster
// that raised its count is not trivially distinguishable (a fake at 4096 would stand out).
TEST(AuthLoginRegression, FakeSaltsAdvertiseTheMostCommonRealIterationCountRegression) {
    namespace sec = rune::security;
    SecurityEventChain chain;
    sec::BootstrapOps b; b.verifier = scramVerifierOf(kTestAdminPassword, 9); b.fakeSaltSeed.fill(0x42);   // 4096, one account
    (void)chain.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    for (const char* name : {"ws1", "ws2", "ws3"})
        (void)chain.emit(sec::CreateUserOps{scramVerifierOf(kTestPasswordA, 1, 8192)}, name, std::string("CREATE USER ") + name + " WITH PASSWORD '<redacted>';");
    Rig rig(std::move(chain));
    EXPECT_EQ(rig.view.fakeIterations(), 8192u) << "3 accounts at 8192 beat 1 at 4096";
    const auto unknown = rig.login("nobody", {"x", "x"});
    const auto real = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_FALSE(unknown.firstBytes.empty());
    ASSERT_FALSE(real.firstBytes.empty());
    const auto u = pc::AuthServerFirst::decode(unknown.firstBytes.data(), unknown.firstBytes.size());
    const auto r = pc::AuthServerFirst::decode(real.firstBytes.data(), real.firstBytes.size());
    EXPECT_EQ(u->salts[0].iterations, 8192u);
    EXPECT_EQ(u->salts[1].iterations, 8192u);
    EXPECT_EQ(r->salts[0].iterations, 8192u);
    EXPECT_EQ(r->salts[1].iterations, 8192u) << "the one-password user's pad uses it too";
    EXPECT_EQ(Rig().view.fakeIterations(), 4096u) << "the default when the accounts use it";
}
