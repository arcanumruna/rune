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

#include <map>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>
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
                                  uint8_t, int16_t, const rune::protocol::TopicConfigView&, const DdlMetaView&, std::string_view) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t) override { return true; }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t, const uint8_t*, uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitCatalogSubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitCatalogUnsubscribe(int64_t, int64_t) override { return true; }
    /// `securityDdls` counts the AUTHORIZED forwards; a denied one (PR #410 review F1: forwarded with the gateway's
    /// verdict, which the leader answers) is recorded in `denied` by correlation id.
    std::map<int64_t, std::string> denied;
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl& w, const DdlMetaView& m, std::string_view denial, bool) override {
        if (!denial.empty()) { denied[w.correlationId] = std::string(denial); return AdminSubmit::Accepted; }
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
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
                 if (refuse) return -2;   // issue #119: egress back-pressure (reliable sessions only)
                 onFrame(b, n);
                 return 1;
             }, 1)) {
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
    /// Issue #119: `reliable` sessions answer through the production spill path (a placeholder egress the rig's
    /// TestEgressFn intercepts); `refuse` back-pressures their egress.
    bool reliable = false;
    bool refuse = false;
    /// Issue #119: replace the (empty) registry by one holding topic 0 "events" with `partitions` partitions.
    void withTopic(int32_t partitions) {
        rune::node::TopicSpec ts;
        ts.name = "events"; ts.partitionCount = partitions; ts.topicId = 0;
        const fs::path d = dir.path / "topics";
        fs::create_directories(d);
        auto next = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, d.string());
        gw.setTopicRegistry(next.get());
        registry = std::move(next);
    }
    /// A new session awaiting its login from `host`.
    int64_t newSession(const std::string& host = "10.0.0.9", int64_t now = 0) {
        const int64_t sid = reliable ? gw.injectReliableTestSessionForTest(now, pc::kClientVersionAuth)
                                     : gw.injectTestSession(now, pc::kClientVersionAuth);
        gw.setSessionAuthForTest(sid, AuthAwaitingFirst{now + 1'000'000'000, 50});
        gw.setSessionSourceHostForTest(sid, host);
        return sid;
    }
    [[nodiscard]] const Frame* last(uint16_t tid, int64_t sid) const {
        for (auto it = seen.rbegin(); it != seen.rend(); ++it) if (it->tid == tid && it->sid == sid) return &*it;
        return nullptr;
    }
    void send(const std::vector<uint8_t>& f) { gw.applyClientFrameForTest(f.data(), f.size()); }
    struct Outcome {
        std::optional<pc::AuthServerFinal> fin; std::vector<uint8_t> finBytes, firstBytes; int64_t sid = 0;
        // Issue #114: what proves this session's own credential changes (the real client keeps the same).
        std::string combinedNonce;
        std::optional<rune::crypto::scram::SaltedPassword> salted;
    };
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
            o.combinedNonce = std::string(first->combinedNonce);
            if (fin != nullptr)
                if (const auto f = pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size()); f && f->serverSignature.size() == 64)
                    for (uint8_t i = 0; i < ans->final.count; ++i)
                        if (ans->salted[i] && std::memcmp(ans->expectedSignature[i].data(), f->serverSignature.data(), 64) == 0)
                            o.salted = ans->salted[i];
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

/// `signer` (issue #114): sign the change with that login's nonce and salted password (`nonce` overrides the nonce: a
/// proof made for another session).  nullptr = no proof at all.
std::vector<uint8_t> alterUserFrame(int64_t sid, int64_t corr, const std::string& name, pc::UserChangeKind change,
                                    const Rig::Outcome* signer = nullptr, std::optional<std::string> nonce = std::nullopt) {
    pc::SecurityDdl m;
    m.sessionId = sid; m.correlationId = corr; m.ext.clientName = "t"; m.ext.requestId = {1, static_cast<uint64_t>(corr)};
    m.kind = pc::SecurityOpKind::AlterUser; m.name = name; m.change = change;
    const auto v = scramVerifierOf("new password 12345", 7);
    m.verifier = pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(v.salt.data()), 16), 4096,
        std::string_view(reinterpret_cast<const char*>(v.storedKey.data()), 64),
        std::string_view(reinterpret_cast<const char*>(v.serverKey.data()), 64)};
    std::array<uint8_t, pc::kCredentialProofBytes> proof{};
    if (signer != nullptr && signer->salted) {
        const auto p = rune::crypto::scram::clientProof(rune::crypto::Digest::Sha512, *signer->salted,
                                                        pc::credentialChangeMessage(nonce ? *nonce : signer->combinedNonce, m));
        if (p) std::memcpy(proof.data(), p->bytes.data(), proof.size());
        m.flags |= pc::kSecurityDdlFlagCredentialProof;
        m.credentialProof = std::string_view(reinterpret_cast<const char*>(proof.data()), proof.size());
    }
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
    // ALTER USER admin SET PASSWORD (proven with the one-time password, issue #114) reaches the admin ring, stamped
    // with the user.
    rig.send(alterUserFrame(o.sid, 73, "admin", pc::UserChangeKind::SetPassword, &o));
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
    rig.gw.onSecurityApplied(static_cast<int64_t>(rig.chain.count()) - 1, *applied);
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
    // #206: fail closed (no session) but retriable - this broker cannot vouch for its security state.
    EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable));
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

// Issue #130: the throttle was checked only at the first leg, so N sessions that all sent AuthClientFirst before any
// final got N verified guesses per delay.  Every proof verification is now gated by the throttle: of 20 parallel
// sessions (one host, one user) only the free failures plus the one that starts the delay are verified; every other
// final is refused with 44 + retryAfterMs, before its proof is looked at, and is not counted.
TEST(LoginThrottleRegression, ParallelSessionsDoNotMultiplyGuessesRegression) {
    Rig rig;
    constexpr int kSessions = 20;
    struct Pending { int64_t sid = 0, corr = 0; std::string nonce; std::vector<uint8_t> first; };
    std::vector<Pending> pending;
    for (int i = 0; i < kSessions; ++i) {
        Pending p;
        p.sid = rig.newSession("10.0.0.9", 0);
        p.nonce = "par-nonce-" + std::to_string(i);
        pc::AuthClientFirst cf;
        cf.sessionId = p.sid; cf.correlationId = ++rig.corr; cf.username = "ws1"; cf.clientNonce = p.nonce;
        p.corr = cf.correlationId;
        std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
        rig.send(b);
        const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), p.sid);
        ASSERT_NE(f, nullptr) << "no failure yet: every first leg passes";
        p.first = f->bytes;
        pending.push_back(std::move(p));
    }
    int verified = 0, limited = 0;
    for (const Pending& p : pending) {
        const auto first = pc::AuthServerFirst::decode(p.first.data(), p.first.size());
        ASSERT_TRUE(first.has_value());
        auto ans = scramAnswer("ws1", p.nonce, *first, {"a wrong guess", "another wrong guess"}, p.sid, p.corr);
        ASSERT_TRUE(ans.has_value());
        std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
        rig.send(fb);
        const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), p.sid);
        ASSERT_NE(f, nullptr);
        const auto fin = pc::AuthServerFinal::decode(f->bytes.data(), f->bytes.size());
        ASSERT_TRUE(fin.has_value());
        EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        if (fin->retryAfterMs > 0) ++limited; else ++verified;
    }
    // 2 free pair failures + the 3rd, which starts the 250 ms delay.
    EXPECT_EQ(verified, 3) << "one guess per allowed slot, not one per session";
    EXPECT_EQ(limited, kSessions - 3);
    EXPECT_EQ(rig.gw.loginsFailed(), 3u) << "a refused final is not counted as a failure";
    EXPECT_EQ(rig.gw.loginsRateLimited(), static_cast<uint64_t>(kSessions - 3));
}

// Issue #114 at the gateway: a user's own credential change without ADMIN needs a proof of a CURRENT password over the
// SESSION's login nonce and the request.  No proof, a proof made for another session, a proof under a wrong password:
// each answered 45 and never forwarded; the right one is forwarded.  Every bad proof counts on the login throttle.
TEST(AuthLoginRegression, OwnCredentialChangeNeedsAProofBoundToTheSessionRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    const auto b = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    ASSERT_EQ(b.fin->errorCode, 0);
    ASSERT_TRUE(a.salted.has_value());
    ASSERT_NE(a.combinedNonce, b.combinedNonce) << "every login has its own nonce";
    // A denied request carrying a request id is forwarded with the verdict (PR #410 review F1): the leader answers 45.
    auto answerCode = [&](int64_t corr) -> int {
        for (auto it = rig.seen.rbegin(); it != rig.seen.rend(); ++it)
            if (it->corr == corr && it->tid == static_cast<uint16_t>(pc::TemplateId::DdlResponse))
                return pc::DdlResponse::decode(it->bytes.data(), it->bytes.size())->errorCode;
        return rig.admin.denied.count(corr) != 0 ? 45 : -1;
    };
    Rig::Outcome wrongPassword = a;
    const std::array<uint8_t, 16> salt{};
    wrongPassword.salted = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, "not the password", salt, 4096);
    // Each forged change at a time past the previous throttle delay (2 free, then 250 ms, 500 ms ...), so every one of
    // them reaches the proof check.
    struct Forged { int64_t sid; const Rig::Outcome* signer; std::optional<std::string> nonce; const char* what; };
    const Forged forged[] = {
        {b.sid, nullptr, std::nullopt, "no proof (what a hijacked session can send)"},
        {b.sid, &a, std::nullopt, "A's proof replayed on session B"},
        {a.sid, &a, b.combinedNonce, "a proof over another session's nonce"},
        {a.sid, &wrongPassword, std::nullopt, "a proof under a wrong password"},
    };
    int64_t now = 0;
    int64_t corr = 80;
    for (const Forged& f : forged) {
        rig.send(alterUserFrame(f.sid, ++corr, "ws1", pc::UserChangeKind::AddPassword, f.signer, f.nonce));
        EXPECT_EQ(answerCode(corr), 45) << f.what;
        now += 2'000'000'000; rig.gw_now = now; rig.gw.setNowNsForTest(now);
    }
    EXPECT_EQ(rig.admin.securityDdls, 0) << "nothing forwarded";
    EXPECT_TRUE(rig.logContains("sent a change of its own password without a valid proof"));
    // Bad proofs count on the login throttle: right after one, even a right proof is refused ...
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::AddPassword, &wrongPassword));
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::AddPassword, &a));
    EXPECT_EQ(answerCode(corr), 45) << "throttled like a login";
    EXPECT_EQ(rig.admin.securityDdls, 0);
    // ... and after the delay the session's own proof is accepted (also for SET and DROP).
    now += 60'000'000'000; rig.gw_now = now; rig.gw.setNowNsForTest(now);
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::AddPassword, &a));
    EXPECT_EQ(rig.admin.securityDdls, 1) << "the session's own proof is accepted";
    EXPECT_TRUE(rig.logContains("adds a password to its own account"));
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::SetPassword, &a));
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::DropPassword, &a));
    EXPECT_EQ(rig.admin.securityDdls, 3);
}

// ── Adversarial: the SCRAM login legs and the throttle (the area of #130) ───────────────────────────────────────────
namespace {
struct FirstLeg { int64_t sid = 0, corr = 0; std::string nonce; std::optional<pc::AuthServerFirst> first; std::vector<uint8_t> bytes; };
FirstLeg sendFirst(Rig& rig, const std::string& user, const std::string& host = "10.0.0.9", int64_t now = 0) {
    FirstLeg f;
    f.sid = rig.newSession(host, now);
    f.nonce = "adv-" + std::to_string(++rig.corr);
    pc::AuthClientFirst cf;
    cf.sessionId = f.sid; cf.correlationId = ++rig.corr; cf.username = user; cf.clientNonce = f.nonce;
    f.corr = cf.correlationId;
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    rig.send(b);
    if (const Frame* sf = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), f.sid)) {
        f.bytes = sf->bytes;
        f.first = pc::AuthServerFirst::decode(f.bytes.data(), f.bytes.size());
    }
    return f;
}
std::vector<uint8_t> finalFor(const FirstLeg& f, const std::string& user, const std::array<std::string, 2>& pws, uint8_t count = 0) {
    auto ans = scramAnswer(user, f.nonce, *f.first, pws, f.sid, f.corr, count);
    std::vector<uint8_t> b(ans->final.encodedSize()); b.resize(ans->final.encode(b.data()));
    return b;
}
std::optional<pc::AuthServerFinal> lastFinal(const Rig& rig, int64_t sid) {
    const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), sid);
    if (f == nullptr) return std::nullopt;
    return pc::AuthServerFinal::decode(f->bytes.data(), f->bytes.size());
}
}  // namespace

// Out of order, replayed, malformed, oversize and late login legs: each is answered and ends the exchange; a session
// never authenticates off another session's final or a replay, and an authenticated session is not disturbed by one.
TEST(AuthLoginRegression, LoginLegsRefuseOutOfOrderReplayedMalformedAndLateFramesAdversarialRegression) {
    Rig rig;
    // A final before any first leg: the gate ends the session.
    {
        const int64_t sid = rig.newSession();
        pc::AuthClientFinal fin; fin.sessionId = sid; fin.correlationId = 5; fin.count = 2;
        std::vector<uint8_t> b(fin.encodedSize()); b.resize(fin.encode(b.data()));
        rig.send(b);
        EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << "AuthClientFinal in AwaitingFirst";
    }
    // A second first leg while the final is owed: ended too.
    {
        const FirstLeg f = sendFirst(rig, "ws1");
        ASSERT_TRUE(f.first.has_value());
        pc::AuthClientFirst again; again.sessionId = f.sid; again.correlationId = 6; again.username = "ws1"; again.clientNonce = "x";
        std::vector<uint8_t> b(again.encodedSize()); b.resize(again.encode(b.data()));
        rig.send(b);
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid)) << "AuthClientFirst in AwaitingFinal";
    }
    // One proof where two salts were listed, and a truncated final: failed logins, sessions ended.
    for (const bool truncate : {false, true}) {
        const FirstLeg f = sendFirst(rig, "ws1");
        ASSERT_TRUE(f.first.has_value());
        std::vector<uint8_t> b = finalFor(f, "ws1", {kTestPasswordA, ""}, truncate ? 0 : 1);
        if (truncate) b.pop_back();
        rig.send(b);
        const auto fin = lastFinal(rig, f.sid);
        ASSERT_TRUE(fin.has_value()) << truncate;
        EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << truncate;
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid)) << truncate;
    }
    // A username over 128 bytes is refused at the first leg, before any salt.
    {
        const FirstLeg f = sendFirst(rig, std::string(129, 'u'));
        EXPECT_FALSE(f.first.has_value());
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid));
    }
    // A valid final replayed on its own (now authenticated) session: answered, the session stays authenticated.  From
    // its own address: the episodes above charged (ws1, 10.0.0.9) three failures (#132: the second first leg is an
    // abandoned one), so that pair is inside its delay now.
    const FirstLeg okLeg = sendFirst(rig, "ws1", "10.0.0.40");
    ASSERT_TRUE(okLeg.first.has_value());
    const std::vector<uint8_t> okFinal = finalFor(okLeg, "ws1", {kTestPasswordA, ""});
    rig.send(okFinal);
    ASSERT_EQ(lastFinal(rig, okLeg.sid)->errorCode, 0);
    rig.send(okFinal);
    EXPECT_EQ(lastFinal(rig, okLeg.sid)->errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(std::holds_alternative<AuthAuthenticated>(*rig.gw.sessionAuthForTest(okLeg.sid)));
    // ... and on ANOTHER session awaiting its final: its nonce differs, the proof fails.
    {
        const FirstLeg other = sendFirst(rig, "ws1", "10.0.0.40");
        ASSERT_TRUE(other.first.has_value());
        std::vector<uint8_t> stolen = okFinal;
        pc::detail::putLE64(stolen.data(), 8, other.sid);
        rig.send(stolen);
        const auto fin = lastFinal(rig, other.sid);
        ASSERT_TRUE(fin.has_value());
        EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        EXPECT_FALSE(rig.gw.hasSessionForTest(other.sid));
    }
    // A correct final after the session's deadline: failed (timeout), never authenticated.
    {
        rig.gw_now = 10'000'000'000; rig.gw.setNowNsForTest(rig.gw_now);
        const FirstLeg late = sendFirst(rig, "ws2", "10.0.0.33", rig.gw_now);
        ASSERT_TRUE(late.first.has_value());
        rig.gw_now += 2'000'000'000; rig.gw.setNowNsForTest(rig.gw_now);
        rig.send(finalFor(late, "ws2", {kTestPasswordA, ""}));
        const auto fin = lastFinal(rig, late.sid);
        ASSERT_TRUE(fin.has_value());
        EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        EXPECT_TRUE(rig.logContains("reason=timeout"));
    }
}

// The host key of the throttle also gates the FINAL leg: many users guessed in parallel from one host get the host's
// 10 free failures plus the one that starts the delay - and while that delay runs, even a right password from that
// host is refused (no proof verified), while the same user from another host is untouched.
TEST(LoginThrottleRegression, TheHostKeyGatesParallelFinalsAcrossUsersAdversarialRegression) {
    Rig rig;
    constexpr int kUsers = 14;
    std::vector<FirstLeg> legs;
    for (int i = 0; i < kUsers; ++i) {
        legs.push_back(sendFirst(rig, "guess" + std::to_string(i)));
        ASSERT_TRUE(legs.back().first.has_value()) << i;
    }
    int verified = 0, limited = 0;
    for (int i = 0; i < kUsers; ++i) {
        rig.send(finalFor(legs[static_cast<std::size_t>(i)], "guess" + std::to_string(i), {"wrong", "wrong"}));
        const auto fin = lastFinal(rig, legs[static_cast<std::size_t>(i)].sid);
        ASSERT_TRUE(fin.has_value()) << i;
        EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
        if (fin->retryAfterMs > 0) ++limited; else ++verified;
    }
    EXPECT_EQ(verified, 11) << "the host's 10 free failures + the one that starts its delay";
    EXPECT_EQ(limited, kUsers - 11);
    const FirstLeg sameHost = sendFirst(rig, "ws1");   // the first leg is refused already (the host is delayed)
    EXPECT_FALSE(sameHost.first.has_value());
    EXPECT_GT(lastFinal(rig, sameHost.sid)->retryAfterMs, 0u);
    EXPECT_EQ(rig.login("ws1", {kTestPasswordA, ""}, "10.0.0.200").fin->errorCode, 0) << "another host is untouched";
}

// ── Adversarial: SecurityDdl from a non-admin session (the area of #114) ─────────────────────────────────────────
// Every truncation of a signed own-credential change, and the frame with a byte appended, is answered and never
// reaches the admin thread; the body cut exactly before its trailer decodes as an UNSIGNED change and is refused 45.
TEST(AuthLoginRegression, ASignedOwnChangeTruncatedAtEveryOffsetIsNeverForwardedAdversarialRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    const std::vector<uint8_t> full = alterUserFrame(a.sid, 900, "ws1", pc::UserChangeKind::AddPassword, &a);
    std::vector<std::vector<uint8_t>> bad;
    for (std::size_t n = pc::MessageHeader::ENCODED_SIZE; n < full.size(); ++n) bad.emplace_back(full.begin(), full.begin() + n);
    bad.push_back(full); bad.back().push_back(0x00);
    for (const auto& b : bad) {
        const std::size_t before = rig.seen.size();
        rig.admin.denied.clear();
        rig.send(b);
        bool answered = rig.admin.denied.count(900) != 0;   // forwarded with a denial: the leader answers 45
        for (std::size_t i = before; i < rig.seen.size(); ++i) answered |= rig.seen[i].corr == 900;
        EXPECT_TRUE(answered) << "length " << b.size();
        rig.gw_now += 60'000'000'000; rig.gw.setNowNsForTest(rig.gw_now);   // past any throttle delay
    }
    EXPECT_EQ(rig.admin.securityDdls, 0) << "nothing malformed or unsigned was forwarded";
    EXPECT_TRUE(rig.gw.hasSessionForTest(a.sid)) << "an authenticated session survives a bad request";
    rig.send(full);
    EXPECT_EQ(rig.admin.securityDdls, 1) << "the intact frame still goes through";
}

// A valid proof does not make someone ELSE's credential the session's own: another user's name (or the same letters in
// another case) still needs ADMIN, and a non-credential change of oneself (DISABLE) too.
TEST(AuthLoginRegression, AValidProofNeverStandsInForAdminAdversarialRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    int64_t corr = 950;
    for (const char* name : {"ws2", "WS1", "admin"}) {
        rig.send(alterUserFrame(a.sid, ++corr, name, pc::UserChangeKind::SetPassword, &a));
        // Forwarded with the verdict (PR #410 review F1): the leader answers 45 with it.
        ASSERT_EQ(rig.admin.denied.count(corr), 1u) << name;
        EXPECT_EQ(rig.admin.denied[corr], "not authorized: ADMIN on CLUSTER") << name;
    }
    rig.send(alterUserFrame(a.sid, ++corr, "ws1", pc::UserChangeKind::Disable, &a));
    EXPECT_EQ(rig.admin.securityDdls, 0);
}

// ── PR #410 review, adversarial pass ─────────────────────────────────────────────────────────────────────────────
// #130 over time: sessions PARKED in AwaitingFinal (first legs sent before any failure) are not a bank of guesses for
// later windows either.  After the first delay runs out, all the parked finals arrive at once: exactly ONE is verified
// (it starts the next, longer delay), every other one is refused with retryAfter before its proof is looked at.
TEST(LoginThrottleRegression, ParkedFinalsGetOneGuessPerDelayWindowAdversarialRegression) {
    Rig rig;
    constexpr int kSessions = 20;
    std::vector<FirstLeg> legs;
    for (int i = 0; i < kSessions; ++i) {
        legs.push_back(sendFirst(rig, "ws1"));
        ASSERT_TRUE(legs.back().first.has_value()) << i;
    }
    auto sendWrong = [&](const FirstLeg& f) {
        rig.send(finalFor(f, "ws1", {"a wrong guess", "another wrong guess"}));
        const auto fin = lastFinal(rig, f.sid);
        return fin ? std::optional<uint32_t>(fin->retryAfterMs) : std::nullopt;
    };
    for (int i = 0; i < 3; ++i) ASSERT_EQ(sendWrong(legs[static_cast<std::size_t>(i)]), 0u) << "free failures + the one starting the delay";
    // The 250 ms delay has passed; the 17 parked sessions are still inside their 1 s login deadline.
    rig.gw_now = 300'000'000; rig.gw.setNowNsForTest(rig.gw_now);
    int verified = 0, limited = 0;
    for (int i = 3; i < kSessions; ++i) {
        const auto r = sendWrong(legs[static_cast<std::size_t>(i)]);
        ASSERT_TRUE(r.has_value()) << i;
        if (*r > 0) ++limited; else ++verified;
    }
    EXPECT_EQ(verified, 1) << "one guess per delay window, not one per parked session";
    EXPECT_EQ(limited, kSessions - 4);
    EXPECT_EQ(rig.gw.loginsFailed(), 4u);
}

// Issue #132: a session that sent AuthClientFirst (and so learned the salts) and never sent the final was free - an
// attacker could probe names by first-leg latency without the throttle ever seeing it.  Every first leg left without a
// final (the deadline, a disconnect, any close) is charged as a failure, exactly once.
TEST(LoginThrottleRegression, AbandonedFirstLegsAreChargedToTheThrottleRegression) {
    Rig rig;
    std::vector<int64_t> sids;
    for (int i = 0; i < 3; ++i) {
        const FirstLeg f = sendFirst(rig, "ws2");
        ASSERT_TRUE(f.first.has_value()) << i;
        sids.push_back(f.sid);
    }
    rig.gw_now = 1'000'000'001; rig.gw.setNowNsForTest(rig.gw_now);
    rig.gw.reapExpiredForTest(rig.gw_now);
    for (const int64_t sid : sids) EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    EXPECT_EQ(rig.gw.loginsAbandoned(), 3u);
    // Three abandoned first legs = three failures of the (ws2, host) pair: 2 free, the 3rd starts the 250 ms delay.
    const auto limited = rig.login("ws2", {kTestPasswordA, ""});
    ASSERT_TRUE(limited.fin.has_value());
    EXPECT_EQ(limited.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_GT(limited.fin->retryAfterMs, 0u) << "first-leg-only probes are charged";
    EXPECT_TRUE(limited.firstBytes.empty());
    // A final leg is its own charge: a wrong proof is one failure, never a second one as abandoned at the close.
    rig.gw_now += 60'000'000'000; rig.gw.setNowNsForTest(rig.gw_now);
    const uint64_t failedBefore = rig.gw.loginsFailed();
    const FirstLeg w = sendFirst(rig, "ws1", "10.0.0.50", rig.gw_now);
    ASSERT_TRUE(w.first.has_value());
    rig.send(finalFor(w, "ws1", {"a wrong guess", "another wrong guess"}));
    EXPECT_FALSE(rig.gw.hasSessionForTest(w.sid));
    EXPECT_EQ(rig.gw.loginsFailed(), failedBefore + 1);
    EXPECT_EQ(rig.gw.loginsAbandoned(), 3u) << "the closing final is not an abandoned first leg";
    // A successful login is not charged at all.
    const FirstLeg ok = sendFirst(rig, "ws1", "10.0.0.51", rig.gw_now);
    rig.send(finalFor(ok, "ws1", {kTestPasswordA, ""}));
    ASSERT_EQ(lastFinal(rig, ok.sid)->errorCode, 0);
    EXPECT_EQ(rig.gw.loginsAbandoned(), 3u);
}

// ── Adversarial review of PR #522 (#131 / #132) ────────────────────────────────────────────────────────────────────
// A final refused by the throttle never looks at the proof - so it is no verdict, and the first leg it follows (allowed,
// its salts revealed) was abandoned.  Before the fix the refused final marked the episode charged: an attacker parked N
// first legs (all allowed: nothing charged yet), spent 3 wrong finals to open the 250 ms delay, then sent the other N-3
// finals into it - N first-leg probes for the price of 3 failures, and a 250 ms schedule.
TEST(LoginThrottleRegression, ThrottleRefusedFinalsStillChargeTheirFirstLegsAdversarialRegression) {
    Rig rig;
    constexpr int kSessions = 12;
    std::vector<FirstLeg> legs;
    for (int i = 0; i < kSessions; ++i) {
        legs.push_back(sendFirst(rig, "ws1"));
        ASSERT_TRUE(legs.back().first.has_value()) << i;
    }
    int verified = 0, limited = 0;
    for (const FirstLeg& f : legs) {
        rig.send(finalFor(f, "ws1", {"a wrong guess", "another wrong guess"}));
        const auto fin = lastFinal(rig, f.sid);
        ASSERT_TRUE(fin.has_value());
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid));
        if (fin->retryAfterMs > 0) ++limited; else ++verified;
    }
    ASSERT_EQ(verified, 3);
    ASSERT_EQ(limited, kSessions - 3);
    EXPECT_EQ(rig.gw.loginsFailed(), 3u) << "a refused final is still not a verified guess";
    EXPECT_EQ(rig.gw.loginsAbandoned(), static_cast<uint64_t>(kSessions - 3)) << "each refused final's first leg is charged";
    // 12 pair failures: the 10th over the free 2 -> min(250 ms << 9, 30 s) = 30 s, not the 250 ms of 3 failures.
    rig.gw_now = 1'000'000'000; rig.gw.setNowNsForTest(rig.gw_now);
    const FirstLeg after = sendFirst(rig, "ws1", "10.0.0.9", rig.gw_now);
    EXPECT_FALSE(after.first.has_value()) << "the parked probes moved the schedule";
    ASSERT_TRUE(lastFinal(rig, after.sid).has_value());
    EXPECT_EQ(lastFinal(rig, after.sid)->retryAfterMs, 29'000u);
}

// Every way an AuthAwaitingFinal session ends charges its first leg exactly once (closeSession_ is the one funnel):
// a Disconnect, a request before the final (the gate ends it), the auth deadline.  A session that never sent a first
// leg, an authenticated one, and a second close of the same session charge nothing.
TEST(LoginThrottleRegression, EveryCloseOfAnAwaitingFinalSessionChargesItOnceAdversarialRegression) {
    Rig rig;
    auto disconnect = [&](int64_t sid) {
        pc::Disconnect d; d.sessionId = sid; d.correlationId = ++rig.corr;
        std::vector<uint8_t> b(pc::Disconnect::ENCODED_SIZE); b.resize(d.encode(b.data()));
        rig.send(b);
    };
    uint64_t expected = 0;
    {   // Disconnect, then the same Disconnect again
        const FirstLeg f = sendFirst(rig, "ws1", "10.1.0.1");
        ASSERT_TRUE(f.first.has_value());
        disconnect(f.sid);
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid));
        EXPECT_EQ(rig.gw.loginsAbandoned(), ++expected) << "disconnect";
        disconnect(f.sid);
        EXPECT_EQ(rig.gw.loginsAbandoned(), expected) << "a second close of a gone session";
    }
    {   // a request before the final: the gate answers 44 and ends the session
        const FirstLeg f = sendFirst(rig, "ws1", "10.1.0.2");
        ASSERT_TRUE(f.first.has_value());
        rig.send(defineTopicFrame(f.sid, ++rig.corr));
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid));
        EXPECT_EQ(rig.gw.loginsAbandoned(), ++expected) << "gate";
    }
    {   // never a first leg (no salt revealed): a disconnect is not an abandoned first leg
        const int64_t sid = rig.newSession("10.1.0.3");
        disconnect(sid);
        EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
        EXPECT_EQ(rig.gw.loginsAbandoned(), expected) << "awaiting first";
    }
    {   // authenticated, then disconnected
        const auto ok = rig.login("ws1", {kTestPasswordA, ""}, "10.1.0.4");
        ASSERT_EQ(ok.fin->errorCode, 0);
        disconnect(ok.sid);
        EXPECT_FALSE(rig.gw.hasSessionForTest(ok.sid));
        EXPECT_EQ(rig.gw.loginsAbandoned(), expected) << "authenticated";
    }
    {   // the auth deadline: charged once although the reap also counts it a failed (timed-out) login
        const FirstLeg f = sendFirst(rig, "ws1", "10.1.0.5");
        ASSERT_TRUE(f.first.has_value());
        const uint64_t failedBefore = rig.gw.loginsFailed();
        rig.gw_now = 1'000'000'001; rig.gw.setNowNsForTest(rig.gw_now);
        rig.gw.reapExpiredForTest(rig.gw_now);
        EXPECT_FALSE(rig.gw.hasSessionForTest(f.sid));
        EXPECT_EQ(rig.gw.loginsAbandoned(), ++expected) << "deadline";
        EXPECT_EQ(rig.gw.loginsFailed(), failedBefore + 1);
        rig.gw.reapExpiredForTest(rig.gw_now + 1);
        EXPECT_EQ(rig.gw.loginsAbandoned(), expected) << "a second reap";
    }
}

// #131 through the gateway: an attacker spreading abandoned first legs and wrong finals for ws1 over 200 source
// hosts (the host is attacker-influenced) never delays ws1 at a host it logged in from, and delays it at a NEW host
// by the 2 s user cap at most - never a lockout, gone once the cap has passed.
TEST(LoginThrottleRegression, ManyHostsAgainstOneUserDelayAKnownPairNeverAndANewHostByTheCapAdversarialRegression) {
    Rig rig;
    ASSERT_EQ(rig.login("ws1", {kTestPasswordA, ""}, "10.0.0.9").fin->errorCode, 0) << "the user's own host";
    for (int h = 0; h < 200; ++h) {
        const std::string host = "198.51." + std::to_string(h / 250) + "." + std::to_string(h % 250);
        for (int k = 0; k < 2; ++k) {
            const FirstLeg f = sendFirst(rig, "ws1", host, rig.gw_now);
            if (!f.first) continue;   // the user component already refuses this host's legs
            if (k == 0) {
                rig.send(finalFor(f, "ws1", {"a wrong guess", "another wrong guess"}));
            } else {
                pc::Disconnect d; d.sessionId = f.sid; d.correlationId = ++rig.corr;
                std::vector<uint8_t> b(pc::Disconnect::ENCODED_SIZE); b.resize(d.encode(b.data()));
                rig.send(b);
            }
        }
    }
    EXPECT_GE(rig.gw.loginsFailed() + rig.gw.loginsAbandoned(), 11u) << "the campaign reached the user component";
    const auto home = rig.login("ws1", {kTestPasswordA, ""}, "10.0.0.9");
    ASSERT_TRUE(home.fin.has_value());
    EXPECT_EQ(home.fin->errorCode, 0) << "a known pair bypasses the user component";
    const FirstLeg fresh = sendFirst(rig, "ws1", "10.0.0.77", rig.gw_now);
    EXPECT_FALSE(fresh.first.has_value()) << "a new host is delayed by the user component";
    const auto refused = lastFinal(rig, fresh.sid);
    ASSERT_TRUE(refused.has_value());
    EXPECT_GT(refused->retryAfterMs, 0u);
    EXPECT_LE(refused->retryAfterMs, 2'000u) << "the user cap, never a lockout";
    rig.gw_now += static_cast<int64_t>(refused->retryAfterMs) * 1'000'000; rig.gw.setNowNsForTest(rig.gw_now);
    EXPECT_EQ(rig.login("ws1", {kTestPasswordA, ""}, "10.0.0.77").fin->errorCode, 0) << "the cap passed";
}

// The SecurityDdl trailer under version skew and garbage, at the gateway: an OLD client's own change (no trailer) and
// a FORCE-only trailer are refused 45 with the fixed text; an explicit zero flags byte, an unknown flag bit and a
// proof flag with a short proof are malformed (InvalidRequest); none reaches the admin thread.  A NEW client's frame
// with FORCE | CredentialProof and a valid proof goes through.
TEST(AuthLoginRegression, TheSecurityDdlTrailerUnderVersionSkewAndGarbageAdversarialRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    // The gateway's own answer, or (PR #410 review F1) the verdict it forwarded for the leader to answer 45 with.
    struct Answer { uint16_t errorCode; std::string detail; };
    auto answer = [&](int64_t corr) -> std::optional<Answer> {
        for (auto it = rig.seen.rbegin(); it != rig.seen.rend(); ++it)
            if (it->corr == corr && it->tid == static_cast<uint16_t>(pc::TemplateId::DdlResponse)) {
                const auto d = pc::DdlResponse::decode(it->bytes.data(), it->bytes.size());
                return Answer{d->errorCode, std::string(d->detail)};
            }
        if (const auto it = rig.admin.denied.find(corr); it != rig.admin.denied.end()) return Answer{45, it->second};
        return std::nullopt;
    };
    auto pastDelay = [&] { rig.gw_now += 60'000'000'000; rig.gw.setNowNsForTest(rig.gw_now); };
    // An old client: no trailer at all.
    rig.send(alterUserFrame(a.sid, 1001, "ws1", pc::UserChangeKind::AddPassword));
    ASSERT_TRUE(answer(1001).has_value());
    EXPECT_EQ(answer(1001)->errorCode, 45);
    EXPECT_NE(std::string(answer(1001)->detail).find("fresh proof of a current password"), std::string::npos);
    pastDelay();
    // Garbage trailers on an otherwise valid signed frame (the flags byte sits right before the 64-byte proof).
    const std::vector<uint8_t> good = alterUserFrame(a.sid, 1002, "ws1", pc::UserChangeKind::AddPassword, &a);
    const std::size_t flagsAt = good.size() - 1 - pc::kCredentialProofBytes;
    ASSERT_EQ(good[flagsAt], pc::kSecurityDdlFlagCredentialProof);
    struct Bad { std::vector<uint8_t> frame; int code; const char* what; };
    std::vector<Bad> bad;
    {   // FORCE only: a trailer of one byte, no proof
        std::vector<uint8_t> f(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(flagsAt) + 1);
        f[flagsAt] = pc::kSecurityDdlFlagForce;
        bad.push_back({f, 45, "FORCE without a proof"});
    }
    {   std::vector<uint8_t> f(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(flagsAt) + 1);
        f[flagsAt] = 0x00;
        bad.push_back({f, 39, "an explicit zero flags byte"}); }
    {   std::vector<uint8_t> f = good; f[flagsAt] = 0x06; bad.push_back({f, 39, "an unknown flag bit"}); }
    {   std::vector<uint8_t> f = good; f.resize(f.size() - 1); bad.push_back({f, 39, "a proof one byte short"}); }
    int64_t corr = 1100;
    for (Bad& b : bad) {
        pc::detail::putLE64(b.frame.data(), 16, ++corr);
        rig.send(b.frame);
        const auto d = answer(corr);
        ASSERT_TRUE(d.has_value()) << b.what;
        EXPECT_EQ(d->errorCode, b.code) << b.what << ": " << d->detail;
        pastDelay();
    }
    EXPECT_EQ(rig.admin.securityDdls, 0) << "nothing unsigned or malformed was forwarded";
    // A new client with FORCE | CredentialProof (FORCE is meaningless on ADD PASSWORD and outside the proof; the proof
    // is what counts): forwarded.
    std::vector<uint8_t> f = good;
    f[flagsAt] = pc::kSecurityDdlFlagForce | pc::kSecurityDdlFlagCredentialProof;
    rig.send(f);
    EXPECT_EQ(rig.admin.securityDdls, 1) << "a valid proof with FORCE set goes through";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issues #15 / #111 (decision M7): an authenticating broker whose `__security` state is not provably current (booting,
// wiped, new, partitioned) serves no login, `__security` read or security DDL: the retriable AuthUnavailable (48).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
/// The chain as it stood BEFORE ws1 was disabled (a broker that was down while the DISABLE committed).
SecurityEventChain chainBeforeDisable() {
    namespace sec = rune::security;
    SecurityEventChain c;
    sec::BootstrapOps b; b.verifier = scramVerifierOf(kTestAdminPassword, 9); b.fakeSaltSeed.fill(0x42);
    (void)c.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    (void)c.emit(sec::CreateUserOps{scramVerifierOf(kTestPasswordA, 1)}, "ws1", "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    return c;
}
}  // namespace

// Issue #15 (RED before the fix: the restarted broker verified the login against its stale local state - the user's
// old password logged in although the cluster had disabled the user).  Not current: AuthServerFinal{48} and the end of
// the session (never the terminal 44: the credentials may be fine); once current the login is judged normally.
TEST(AuthRestartRegression, ARestartedBrokerNeverAcceptsADisabledUsersOldPasswordRegression) {
    Rig rig(chainBeforeDisable());
    rig.gw.setSecurityServing(false);   // what BrokerNode sets at boot, until the admin thread vouches
    const auto o = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_TRUE(o.fin.has_value());
    EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable)) << "never verified against a stale view";
    EXPECT_TRUE(o.fin->serverSignature.empty());
    EXPECT_FALSE(rig.gw.hasSessionForTest(o.sid)) << "the session is ended; the client retries elsewhere";
    EXPECT_EQ(rig.gw.loginsOk(), 0u);
    EXPECT_EQ(rig.gw.loginsUnavailable(), 1u);
    EXPECT_EQ(rig.gw.loginsFailed(), 0u) << "not a failed login (no throttle charge)";
    EXPECT_TRUE(rig.logContains("reason=security-not-current"));
    // Caught up: the view now holds the DISABLE (applied before the verdict, same FIFO) - the old password is refused.
    const auto dis = rig.chain.emit(rune::security::AlterUserOps{rune::security::DisableUser{}}, "ws1", "ALTER USER ws1 DISABLE;");
    ASSERT_TRUE(rig.view.onApplied(static_cast<int64_t>(rig.chain.count()) - 1, *rune::security::encode(dis)).has_value());
    rig.gw.setSecurityServing(true);
    const auto after = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_TRUE(after.fin.has_value());
    EXPECT_EQ(after.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
}

TEST(AuthRestartRegression, AWipedBrokerAnswersRetriableUntilCaughtUpRegression) {
    Rig rig;
    rig.gw.setSecurityServing(false);
    for (int i = 0; i < 3; ++i) {
        const auto o = rig.login("ws1", {kTestPasswordA, ""});
        ASSERT_TRUE(o.fin.has_value());
        EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable)) << i;
    }
    // The view went stale BETWEEN the legs: the final leg is refused the same way.
    rig.gw.setSecurityServing(true);
    const int64_t sid = rig.newSession();
    pc::AuthClientFirst cf;
    cf.sessionId = sid; cf.correlationId = 901; cf.username = "ws1"; cf.clientNonce = "nonce-901";
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    rig.send(b);
    const Frame* f1 = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), sid);
    ASSERT_NE(f1, nullptr);
    const auto first = pc::AuthServerFirst::decode(f1->bytes.data(), f1->bytes.size());
    auto ans = scramAnswer("ws1", "nonce-901", *first, {kTestPasswordA, ""}, sid, 902);
    rig.gw.setSecurityServing(false);
    std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
    rig.send(fb);
    const Frame* fin = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), sid);
    ASSERT_NE(fin, nullptr);
    EXPECT_EQ(pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size())->errorCode,
              static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable));
    // Current again: the same password logs in.
    rig.gw.setSecurityServing(true);
    const auto ok = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_TRUE(ok.fin.has_value());
    EXPECT_EQ(ok.fin->errorCode, 0);
}

// Issue #15 (M8): a new / wiped broker's replica is empty - it must not tell a Connect "Anonymous" (the bootstrap window
// of a fresh cluster) while it is not current: the cluster may have been bootstrapped long ago.
TEST(AuthRestartRegression, ANewBrokerNeverAdvertisesTheBootstrapWindowRegression) {
    Rig rig;
    SecurityView empty{rune::security::SecurityState{}};
    rig.gw.setAuthMode(GatewayAuthMode::Required, &empty);
    rig.gw.setSecurityServing(false);
    EXPECT_EQ(rig.gw.connectAuthStateForTest(), pc::AuthState::MustAuthenticate) << "not current: never Anonymous";
    rig.gw.setSecurityServing(true);
    EXPECT_EQ(rig.gw.connectAuthStateForTest(), pc::AuthState::Anonymous) << "a current, never-bootstrapped cluster";
}

// Issue #111: a broker stale for longer than R ends its authenticated sessions retriably (session-level 48), so their
// clients move to a current broker; a session still logging in is not authenticated and is left to its own deadline.
TEST(AuthRestartRegression, AStaleBrokerEndsItsAuthenticatedSessionsRetriablyRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    const auto b = rig.login("ws2", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    ASSERT_EQ(b.fin->errorCode, 0);
    const int64_t pending = rig.newSession();
    rig.gw.setSecurityServing(false);
    EXPECT_TRUE(rig.gw.hasSessionForTest(a.sid)) << "stale but within R: authenticated sessions keep working";
    rig.gw.endSessionsSecurityUnavailable();
    for (const int64_t sid : {a.sid, b.sid}) {
        EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
        const Frame* e = rig.last(static_cast<uint16_t>(pc::TemplateId::ErrorResponse), sid);
        ASSERT_NE(e, nullptr) << sid;
        const auto er = pc::ErrorResponse::decode(e->bytes.data(), e->bytes.size());
        EXPECT_EQ(er->correlationId, 0) << "session level";
        EXPECT_EQ(er->code, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    }
    EXPECT_TRUE(rig.gw.hasSessionForTest(pending));
    EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), 2u);
    EXPECT_EQ(rig.gw.sessionsRevoked(), 0u) << "not a revocation (46 is terminal for the client)";
}

// Issue #111: while not current, security DDL / bootstrap and `__security` reads answer 48 too - none is built on (or
// served from) a stale state; DDL that does not touch `__security` is not affected.
TEST(AuthRestartRegression, SecurityDdlAndSecurityReadsAnswerAuthUnavailableWhileNotCurrentRegression) {
    Rig rig;
    struct { int64_t sid; } admin{rig.newSession()};
    rig.gw.setSessionAuthForTest(admin.sid, AuthAuthenticated{1, "admin", PermissionTable::allowAll(), "nonce"});
    rig.gw.setSecurityServing(false);
    pc::SecurityDdl m;
    m.sessionId = admin.sid; m.correlationId = 77; m.ext.clientName = "t"; m.ext.requestId = {1, 77};
    m.kind = pc::SecurityOpKind::CreateRole; m.name = "r";
    std::vector<uint8_t> b(m.encodedSize()); b.resize(m.encode(b.data()));
    rig.send(b);
    EXPECT_EQ(rig.admin.securityDdls, 0) << "never forwarded";
    const Frame* d = rig.last(static_cast<uint16_t>(pc::TemplateId::DdlResponse), admin.sid);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(pc::DdlResponse::decode(d->bytes.data(), d->bytes.size())->errorCode,
              static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable));
    pc::Subscribe s;
    s.sessionId = admin.sid; s.correlationId = 78; s.topicId = -7; s.partition = 0; s.partitionKnown = 1; s.fromOffset = 0;
    std::vector<uint8_t> sb(s.encodedSize()); sb.resize(s.encode(sb.data()));
    rig.send(sb);
    const Frame* r = rig.last(static_cast<uint16_t>(pc::TemplateId::SubscribeResponse), admin.sid);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(pc::SubscribeResponse::decode(r->bytes.data(), r->bytes.size())->errorCode,
              static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    rig.gw.setSecurityServing(true);
    rig.send(b);
    EXPECT_EQ(rig.admin.securityDdls, 1) << "current again: forwarded";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #459 (issues #15 / #111): sibling entry points to a login or a credential change while the
// broker is not current, and the protocol state around the 48.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

// Protocol state: a first leg answered 48 ended the session; a final leg replayed on that session id afterwards - even
// once the broker is current again - logs nobody in.  A final leg with no first leg at all is not a login either.
TEST(AuthRestartRegression, AFinalLegAfterAnUnavailableFirstLegNeverLogsInAdversarialRegression) {
    Rig rig;
    // The proof a real client would send, prepared on a CURRENT broker for a session that then goes away.
    const int64_t probe = rig.newSession();
    pc::AuthClientFirst cf;
    cf.sessionId = probe; cf.correlationId = 950; cf.username = "ws1"; cf.clientNonce = "nonce-950";
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    rig.send(b);
    const Frame* f1 = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), probe);
    ASSERT_NE(f1, nullptr);
    auto ans = scramAnswer("ws1", "nonce-950", *pc::AuthServerFirst::decode(f1->bytes.data(), f1->bytes.size()),
                           {kTestPasswordA, ""}, probe, 951);
    ASSERT_TRUE(ans.has_value());
    // Not current: the first leg of the SAME session id is answered 48 and the session ends.
    rig.gw.setSecurityServing(false);
    rig.send(b);
    EXPECT_FALSE(rig.gw.hasSessionForTest(probe));
    // Current again: the prepared final leg arrives on the ended session - nothing logs in.
    rig.gw.setSecurityServing(true);
    std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
    rig.send(fb);
    for (const Frame& x : rig.seen)
        if (x.tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFinal) && x.sid == probe)
            EXPECT_NE(pc::AuthServerFinal::decode(x.bytes.data(), x.bytes.size())->errorCode, 0) << "no login on an ended session";
    EXPECT_EQ(rig.gw.loginsOk(), 0u);
    // A final leg with no first leg on a fresh session, not current: never an OK either.
    rig.gw.setSecurityServing(false);
    const int64_t fresh = rig.newSession();
    ans->final.sessionId = fresh;
    std::vector<uint8_t> fb2(ans->final.encodedSize()); fb2.resize(ans->final.encode(fb2.data()));
    rig.send(fb2);
    EXPECT_EQ(rig.gw.loginsOk(), 0u);
}

// Escalation through a sibling op: while not current, a session's own credential changes - the must-change session's
// ALTER USER <self> SET PASSWORD (proven with the one-time password) and an authenticated user's own ADD PASSWORD
// (proven, #114) - are security DDL built on the stale view: 48, never forwarded, and the sessions stay.
TEST(AuthRestartRegression, OwnCredentialChangesAnswerAuthUnavailableWhileNotCurrentAdversarialRegression) {
    Rig rig;
    const auto mc = rig.login("admin", {kTestAdminPassword, ""});
    ASSERT_TRUE(mc.fin.has_value());
    ASSERT_TRUE(std::holds_alternative<AuthPasswordChangeRequired>(*rig.gw.sessionAuthForTest(mc.sid)));
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    rig.gw.setSecurityServing(false);
    rig.send(alterUserFrame(mc.sid, 990, "admin", pc::UserChangeKind::SetPassword, &mc));
    rig.send(alterUserFrame(a.sid, 991, "ws1", pc::UserChangeKind::AddPassword, &a));
    EXPECT_EQ(rig.admin.securityDdls, 0) << "never forwarded while not current";
    for (const int64_t c : {990, 991}) {
        const Frame* f = nullptr;
        for (const Frame& x : rig.seen) if (x.corr == c) f = &x;
        ASSERT_NE(f, nullptr) << c << ": answered, never hung";
        EXPECT_EQ(pc::DdlResponse::decode(f->bytes.data(), f->bytes.size())->errorCode,
                  static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable)) << c;
    }
    EXPECT_TRUE(rig.gw.hasSessionForTest(mc.sid));
    EXPECT_TRUE(rig.gw.hasSessionForTest(a.sid));
    EXPECT_EQ(rig.gw.loginsFailed(), 0u) << "a 48 is not a failed proof (no throttle charge)";
    rig.gw.setSecurityServing(true);
    rig.send(alterUserFrame(mc.sid, 992, "admin", pc::UserChangeKind::SetPassword, &mc));
    EXPECT_EQ(rig.admin.securityDdls, 1) << "current again: the proven change is forwarded";
}

// Ending the sessions after R twice (a second stale episode, or a repeated call) is idempotent: every session is told
// once, and a session that logs in between the two is ended only by the second.
TEST(AuthRestartRegression, EndingStaleSessionsTwiceTellsEachSessionOnceAdversarialRegression) {
    Rig rig;
    const auto a = rig.login("ws1", {kTestPasswordA, ""});
    ASSERT_EQ(a.fin->errorCode, 0);
    rig.gw.setSecurityServing(false);
    rig.gw.endSessionsSecurityUnavailable();
    rig.gw.setSecurityServing(true);
    const auto b = rig.login("ws2", {kTestPasswordA, ""});
    ASSERT_EQ(b.fin->errorCode, 0);
    rig.gw.setSecurityServing(false);
    rig.gw.endSessionsSecurityUnavailable();
    auto endsOf = [&](int64_t sid) {
        int n = 0;
        for (const Frame& x : rig.seen)
            if (x.tid == static_cast<uint16_t>(pc::TemplateId::ErrorResponse) && x.sid == sid && x.corr == 0) ++n;
        return n;
    };
    EXPECT_EQ(endsOf(a.sid), 1);
    EXPECT_EQ(endsOf(b.sid), 1);
    EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), 2u);
}

// ── #205 adversarial review (PR #516): a login against a view behind events this broker holds ───────────────────────
namespace {
constexpr uint16_t kAuthFailed = static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed);
constexpr uint16_t kAuthUnavailable = static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable);

/// The first leg only: the session, the client nonce and the decoded AuthServerFirst (nullopt = refused there).
struct SplitFirstLeg { int64_t sid = 0, corr = 0; std::string nonce; std::optional<pc::AuthServerFirst> first; std::vector<uint8_t> bytes; };
SplitFirstLeg splitFirstLeg(Rig& rig, const std::string& user, const std::string& host = "10.0.0.9") {
    SplitFirstLeg l;
    l.sid = rig.newSession(host);
    l.nonce = "nonce-split-" + std::to_string(++rig.corr);
    pc::AuthClientFirst cf;
    cf.sessionId = l.sid; cf.correlationId = ++rig.corr; cf.username = user; cf.clientNonce = l.nonce;
    l.corr = cf.correlationId;
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    rig.send(b);
    if (const Frame* f = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), l.sid)) {
        l.bytes = f->bytes;
        l.first = pc::AuthServerFirst::decode(l.bytes.data(), l.bytes.size());
    }
    return l;
}
std::optional<pc::AuthServerFinal> splitFinalLeg(Rig& rig, const SplitFirstLeg& l, const std::string& user, const std::string& pw) {
    auto ans = scramAnswer(user, l.nonce, *l.first, {pw, ""}, l.sid, l.corr);
    std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
    rig.send(fb);
    const Frame* fin = rig.last(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), l.sid);
    if (fin == nullptr) return std::nullopt;
    return pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size());
}
void applyCreateUser(Rig& rig, const std::string& name, const char* pw, uint8_t seed) {
    const auto ev = rig.chain.emit(rune::security::CreateUserOps{scramVerifierOf(pw, seed)}, name,
                                   "CREATE USER " + name + " WITH PASSWORD '<redacted>';");
    const auto bytes = rune::security::encode(ev);
    ASSERT_TRUE(bytes.has_value());
    const int64_t off = static_cast<int64_t>(rig.chain.count()) - 1;
    auto applied = rig.view.onApplied(off, *bytes);
    ASSERT_TRUE(applied.has_value()) << applied.error().detail;
    rig.gw.onSecurityApplied(off, *applied);
}
}  // namespace

// The verifiers a final leg is checked against are COPIED at the first leg.  A first leg that ran while the view was
// behind (the just-created user unknown: fake salt) and a final that arrives after the view caught up (the client spent
// its PBKDF2 time in between) used to be judged "not behind" and answered the terminal 44 - #205 again, in its most
// likely timing.  The proof failed against a superseded state: retriable 48; the next login succeeds.
TEST(AuthLaggingViewRegression, AFirstLegOnALaggingViewAndAFinalAfterItCaughtUpIsRetriableAdversarialRegression) {
    Rig rig;
    const int64_t applied = rig.view.appliedOffset();
    rig.gw.setSecurityKnownEnd(applied + 2);   // this broker holds one event (CREATE USER fresh) it has not applied
    const SplitFirstLeg l = splitFirstLeg(rig, "fresh");
    ASSERT_TRUE(l.first.has_value()) << "the unknown user gets the fake salt, never a refusal at the first leg";
    applyCreateUser(rig, "fresh", kTestPasswordA, 11);
    ASSERT_EQ(rig.view.appliedOffset(), applied + 1) << "precondition: the view is caught up before the final leg";
    const auto fin = splitFinalLeg(rig, l, "fresh", kTestPasswordA);
    ASSERT_TRUE(fin.has_value());
    EXPECT_EQ(fin->errorCode, kAuthUnavailable) << "checked against the lagging first-leg state: never the terminal 44";
    EXPECT_FALSE(rig.gw.hasSessionForTest(l.sid));
    const auto again = rig.login("fresh", {kTestPasswordA, ""});
    ASSERT_TRUE(again.fin.has_value());
    EXPECT_EQ(again.fin->errorCode, 0) << "the retry logs in";
}

// The same with a state change the first leg could not see at all (knownEnd did not cover it yet): the view applied a
// SET PASSWORD between the legs - the final's verifiers are the OLD ones, so the new password fails against a state
// that is no longer current.  Retriable, never terminal.
TEST(AuthLaggingViewRegression, AStateAppliedBetweenTheLegsMakesTheFailureRetriableAdversarialRegression) {
    Rig rig;
    const SplitFirstLeg l = splitFirstLeg(rig, "ws2");
    ASSERT_TRUE(l.first.has_value());
    const auto ev = rig.chain.emit(rune::security::AlterUserOps{rune::security::SetPassword{scramVerifierOf("brand new password 1", 12), false}},
                                   "ws2", "ALTER USER ws2 SET PASSWORD '<redacted>';");
    const int64_t off = static_cast<int64_t>(rig.chain.count()) - 1;
    auto a = rig.view.onApplied(off, *rune::security::encode(ev));
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    rig.gw.onSecurityApplied(off, *a);
    rig.gw.setSecurityKnownEnd(off + 1);   // caught up
    const auto fin = splitFinalLeg(rig, l, "ws2", "brand new password 1");
    ASSERT_TRUE(fin.has_value());
    EXPECT_EQ(fin->errorCode, kAuthUnavailable);
    const auto again = rig.login("ws2", {"brand new password 1", ""});
    ASSERT_TRUE(again.fin.has_value());
    EXPECT_EQ(again.fin->errorCode, 0);
    // A wrong password against a state that did not move between the legs is still terminal.
    const auto wrong = rig.login("ws2", {"not the password 99", ""}, "10.0.0.77");
    ASSERT_TRUE(wrong.fin.has_value());
    EXPECT_EQ(wrong.fin->errorCode, kAuthFailed);
}

// knownEnd bounds: only an end PAST the next event to apply makes the view "behind".  -1 (none posted), 0, the applied
// offset (a truncated log re-posted lower) and applied+1 (exactly caught up) keep the terminal 44; applied+2 and the
// i64 max answer 48.  A halted view fails closed but retriably (48, #206) however far behind it is, and a correct
// password is never refused for the lag.
TEST(AuthLaggingViewRegression, KnownEndBoundsHaltAndSuccessAdversarialRegression) {
    Rig rig;
    const int64_t applied = rig.view.appliedOffset();
    int host = 0;
    const auto wrongCode = [&](int64_t knownEnd) {
        rig.gw.setSecurityKnownEnd(knownEnd);
        const auto o = rig.login("ws1", {"wrong password 000", ""}, "10.1.0." + std::to_string(++host));
        return o.fin ? o.fin->errorCode : uint16_t{0xFFFF};
    };
    for (const int64_t k : {int64_t{-1}, int64_t{0}, applied, applied + 1})
        EXPECT_EQ(wrongCode(k), kAuthFailed) << "knownEnd " << k;
    for (const int64_t k : {applied + 2, std::numeric_limits<int64_t>::max()})
        EXPECT_EQ(wrongCode(k), kAuthUnavailable) << "knownEnd " << k;
    rig.gw.setSecurityKnownEnd(applied + 5);
    const auto ok = rig.login("ws1", {kTestPasswordA, ""}, "10.2.0.1");
    ASSERT_TRUE(ok.fin.has_value());
    EXPECT_EQ(ok.fin->errorCode, 0) << "behind is never a reason to refuse a valid proof";
    rune::security::ApplyError err; err.offset = applied + 1; err.detail = "test halt";
    rig.view.onHalted(err);
    const auto halted = rig.login("ws1", {kTestPasswordA, ""}, "10.2.0.2");
    ASSERT_TRUE(halted.fin.has_value());
    EXPECT_EQ(halted.fin->errorCode, kAuthUnavailable) << "a halted view fails closed but retriable (#206)";
}

// Enumeration: while behind, an unknown user, a wrong password and a disabled account answer byte-for-byte alike
// (apart from the session / correlation ids): 48, the same detail, no signature, no retry hint.
TEST(AuthLaggingViewRegression, WhileBehindUnknownWrongAndDisabledAnswerAlikeAdversarialRegression) {
    Rig rig;
    rig.gw.setSecurityKnownEnd(rig.view.appliedOffset() + 2);
    const auto unknown  = rig.login("nobody", {kTestPasswordA, ""}, "10.3.0.1");
    const auto wrong    = rig.login("ws1", {"wrong password 000", ""}, "10.3.0.2");
    const auto disabled = rig.login("off", {kTestPasswordA, ""}, "10.3.0.3");
    for (const auto* o : {&unknown, &wrong, &disabled}) {
        ASSERT_TRUE(o->fin.has_value());
        EXPECT_EQ(o->fin->errorCode, kAuthUnavailable);
        EXPECT_TRUE(o->fin->serverSignature.empty());
        EXPECT_EQ(o->fin->retryAfterMs, 0u);
        EXPECT_EQ(o->fin->detail, unknown.fin->detail);
        EXPECT_EQ(o->finBytes.size(), unknown.finBytes.size());
        EXPECT_EQ(o->firstBytes.size(), unknown.firstBytes.size()) << "the first legs are alike too (fake slots)";
    }
}

// Throttle bypass: a 48 for a failed proof is still a verified guess - charged.  The same (user, host) pair gets its
// two free failures, the third sets the delay, and the next attempt is the rate-limited 44 with retryAfterMs at the
// FIRST leg, never a fourth verified proof.
TEST(AuthLaggingViewRegression, FailedProofsWhileBehindAreChargedToTheThrottleAdversarialRegression) {
    Rig rig;
    rig.gw.setSecurityKnownEnd(rig.view.appliedOffset() + 2);
    for (int i = 0; i < 3; ++i) {
        const auto o = rig.login("ws1", {"guess " + std::to_string(i) + " xxxxxxxx", ""}, "10.4.0.1");
        ASSERT_TRUE(o.fin.has_value()) << i;
        EXPECT_EQ(o.fin->errorCode, kAuthUnavailable) << i;
    }
    EXPECT_EQ(rig.gw.loginsFailed(), 3u);
    const auto limited = rig.login("ws1", {kTestPasswordA, ""}, "10.4.0.1");
    ASSERT_TRUE(limited.fin.has_value());
    EXPECT_TRUE(limited.firstBytes.empty()) << "refused at the first leg";
    EXPECT_EQ(limited.fin->errorCode, kAuthFailed);
    EXPECT_GT(limited.fin->retryAfterMs, 0u);
    EXPECT_EQ(rig.gw.loginsRateLimited(), 1u);
}

// ── Issue #119: a session's final answers are never freed with its spill on close ───────────────────────────────────
// Revoke, a failed / unavailable login, the auth-gate end and the stale-security end send a session its LAST answer
// and end it.  Under egress back-pressure that answer was spilled, and the immediate closeSession_ freed it: the client
// saw the session vanish without its 46 / 44 (and lost retryAfterMs).  The session now enters SpillState::Closing: its
// chain drains in order, nothing new is accepted, and it is closed once drained or at its deadline.
namespace {
/// An authenticated (allow-all) session on the rig's current session kind.
int64_t authedSession(Rig& rig) {
    const int64_t sid = rig.newSession();
    rig.gw.setSessionAuthForTest(sid, AuthAuthenticated{7, "u", PermissionTable::allowAll(), {}});
    return sid;
}
void subscribe(Rig& rig, int64_t sid, int64_t corr, int32_t partition) {
    pc::Subscribe s;
    s.sessionId = sid; s.correlationId = corr; s.topicId = 0; s.partition = partition; s.partitionKnown = 1;
    s.fromOffset = -1;
    std::vector<uint8_t> b(s.encodedSize()); b.resize(s.encode(b.data()));
    rig.gw.applySubscribeForTest(b.data(), b.size());
}
/// The ErrorResponses `sid` received from index `from` on: (correlation id, code), in delivery order.
std::vector<std::pair<int64_t, int16_t>> errorsFrom(const Rig& rig, int64_t sid, std::size_t from) {
    std::vector<std::pair<int64_t, int16_t>> out;
    for (std::size_t i = from; i < rig.seen.size(); ++i) {
        if (rig.seen[i].sid != sid || rig.seen[i].tid != static_cast<uint16_t>(pc::TemplateId::ErrorResponse)) continue;
        const auto e = pc::ErrorResponse::decode(rig.seen[i].bytes.data(), rig.seen[i].bytes.size());
        if (e) out.emplace_back(e->correlationId, e->code);
    }
    return out;
}
std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr) {
    pc::Produce m;
    m.sessionId = sid; m.correlationId = corr; m.topicId = 0; m.partition = 0; m.partitionKnown = 1; m.acks = 1;
    m.producerId = -1; m.baseSequence = -1;
    rune::protocol::RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    std::vector<uint8_t> batch = rb.build();
    m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> b(m.encodedSize()); b.resize(m.encode(b.data()));
    return b;
}
constexpr int16_t kRevoked = static_cast<int16_t>(pc::ErrorCode::SessionRevoked);
}  // namespace

TEST(SessionClosingSpillRegression, RevokedSessionUnderBackPressureStillGetsSessionRevokedRegression) {
    Rig rig;
    rig.withTopic(2);
    rig.reliable = true;
    const int64_t sid = authedSession(rig);
    subscribe(rig, sid, 11, 0);
    subscribe(rig, sid, 12, 1);
    std::vector<int64_t> subscribed;
    for (const Frame& f : rig.seen)
        if (f.sid == sid && f.tid == static_cast<uint16_t>(pc::TemplateId::SubscribeResponse))
            if (const auto r = pc::SubscribeResponse::decode(f.bytes.data(), f.bytes.size()); r && r->errorCode == 0)
                subscribed.push_back(f.corr);
    ASSERT_EQ(subscribed, (std::vector<int64_t>{11, 12})) << "precondition: two gateway-pushed subscriptions";

    rig.refuse = true;
    const std::size_t mark = rig.seen.size();
    rig.gw.revokeAllSessionsForTest("test revoke");   // revokes every authenticated session
    EXPECT_EQ(rig.gw.sessionsRevoked(), 1u);
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid)) << "the revoke freed its spilled 46s with the session";
    EXPECT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
    EXPECT_EQ(rig.gw.closingSessions(), 1u);
    EXPECT_EQ(rig.seen.size(), mark) << "precondition: every 46 was back-pressured";

    rig.refuse = false;
    rig.gw.drainPendingResponses(1);
    const auto errs = errorsFrom(rig, sid, mark);
    ASSERT_EQ(errs.size(), 3u) << "one 46 per subscription, then the session-level 46";
    EXPECT_EQ(std::min(errs[0].first, errs[1].first), 11);
    EXPECT_EQ(std::max(errs[0].first, errs[1].first), 12);
    EXPECT_EQ(errs[2].first, 0) << "the session-level 46 comes last";
    for (const auto& [corr, code] : errs) EXPECT_EQ(code, kRevoked) << "corr " << corr;
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << "closed once its answers were delivered";
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
    EXPECT_EQ(rig.gw.closingDeadlineCloses(), 0u);
}

TEST(SessionClosingSpillRegression, ThrottledLoginFailureUnderBackPressureKeepsItsRetryAfterRegression) {
    Rig rig;
    for (int i = 0; i < 3; ++i) (void)rig.login("ws2", {"wrong password", ""});
    ASSERT_EQ(rig.gw.loginsFailed(), 3u) << "precondition: the throttle delay is active";
    rig.reliable = true;
    rig.refuse = true;
    const auto limited = rig.login("ws2", {kTestPasswordA, ""});
    EXPECT_FALSE(limited.fin.has_value()) << "precondition: the 44 was back-pressured";
    ASSERT_TRUE(rig.gw.hasSessionForTest(limited.sid)) << "the failed login freed its spilled 44 with the session";
    EXPECT_EQ(rig.gw.spillStateForTest(limited.sid), SpillState::Closing);

    rig.refuse = false;
    rig.gw.drainPendingResponses(1);
    const auto fin = lastFinal(rig, limited.sid);
    ASSERT_TRUE(fin.has_value()) << "the client never got its 44";
    EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_GT(fin->retryAfterMs, 0u) << "retryAfterMs was lost";
    EXPECT_FALSE(rig.gw.hasSessionForTest(limited.sid));
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
}

TEST(SessionClosingSpillRegression, AClosingSessionThatNeverDrainsIsClosedAtItsDeadlineRegression) {
    Rig rig;
    rig.reliable = true;
    constexpr int64_t t0 = 5'000;
    rig.gw.setNowNsForTest(t0);
    const std::size_t usedBefore = rig.gw.egressSpillUsedBytesForTest();
    const int64_t sid = authedSession(rig);
    rig.refuse = true;   // forever
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid));
    ASSERT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
    EXPECT_GT(rig.gw.egressSpillUsedBytesForTest(), usedBefore);
    const int64_t deadline = t0 + ClientGateway::kDefaultSessionTimeoutNs;
    rig.gw.drainPendingResponses(deadline - 1);
    EXPECT_TRUE(rig.gw.hasSessionForTest(sid)) << "closed before its deadline";
    EXPECT_EQ(rig.gw.closingDeadlineCloses(), 0u);
    for (int i = 0; i < 8 && rig.gw.hasSessionForTest(sid); ++i) rig.gw.drainPendingResponses(deadline);
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << "a Closing session that never drains is never closed";
    EXPECT_EQ(rig.gw.closingDeadlineCloses(), 1u);
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), usedBefore) << "the spill pool got its bytes back";
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
}

TEST(SessionClosingSpillRegression, AClosingSessionExecutesNoFrameAndAcceptsNoNewAnswerRegression) {
    Rig rig;
    rig.withTopic(1);
    rig.reliable = true;
    const int64_t sid = authedSession(rig);
    rig.refuse = true;
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
    const uint64_t revoked = rig.gw.sessionsRevoked();
    const std::size_t held = rig.gw.spilledBytesForTest(sid);
    const uint64_t answerDrops = rig.gw.closingAnswerDrops();

    // (a) A Produce and a DDL are consumed, counted and never executed.
    const auto pf = produceFrame(sid, 21);
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(pf.data(), pf.size()));
    const auto df = defineTopicFrame(sid, 22);
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(df.data(), df.size()));
    EXPECT_EQ(rig.gw.closingIngressDrops(), 2u);
    EXPECT_EQ(rig.registry->log(0, 0)->logEndOffset(), 0) << "a Closing session's Produce was appended";
    EXPECT_EQ(rig.admin.otherSubmits, 0) << "a Closing session's DDL was submitted";
    EXPECT_EQ(rig.gw.spilledBytesForTest(sid), held) << "an answer was queued behind the last one";
    EXPECT_EQ(rig.gw.closingAnswerDrops(), answerDrops) << "a frame was executed (its answer refused)";

    // (b) A second revoke sends nothing more and is not counted again.
    rig.gw.revokeAllSessionsForTest("test revoke");
    EXPECT_EQ(rig.gw.sessionsRevoked(), revoked);
    EXPECT_EQ(rig.gw.spilledBytesForTest(sid), held);
    EXPECT_EQ(rig.gw.closingAnswerDrops(), answerDrops);
    EXPECT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);

    // (c) A plaintext Disconnect closes it at once.
    pc::Disconnect d; d.sessionId = sid; d.correlationId = ++rig.corr;
    std::vector<uint8_t> b(pc::Disconnect::ENCODED_SIZE); b.resize(d.encode(b.data()));
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(b.data(), b.size()));
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
}

TEST(SessionClosingSpillRegression, NoBackPressureClosesAtOnceAsBeforeRegression) {
    Rig rig;
    rig.reliable = true;
    const int64_t sid = authedSession(rig);
    rig.gw.revokeAllSessionsForTest("test revoke");
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << "revoke: closed in the same call";
    EXPECT_EQ(errorsFrom(rig, sid, 0), (std::vector<std::pair<int64_t, int16_t>>{{0, kRevoked}}));
    const auto o = rig.login("ws2", {"wrong password", ""});
    ASSERT_TRUE(o.fin.has_value());
    EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(rig.gw.hasSessionForTest(o.sid)) << "failed login: closed in the same call";
    const int64_t gated = rig.newSession();
    const int64_t gateCorr = ++rig.corr;
    rig.send(defineTopicFrame(gated, gateCorr));
    EXPECT_FALSE(rig.gw.hasSessionForTest(gated)) << "auth-gate end: closed in the same call";
    EXPECT_TRUE(std::any_of(rig.seen.begin(), rig.seen.end(),
                            [&](const Frame& f) { return f.sid == gated && f.corr == gateCorr; }))
        << "the gate's 44 was delivered";
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
}

// ── Adversarial review of PR #525 (issue #119): every event against a Closing session ───────────────────────────────
namespace {
/// A spillable answer of about `blobBytes` for `sid` (a TopicSchemaResponse carrying an opaque blob).
std::vector<uint8_t> bigAnswer(int64_t sid, int64_t corr, std::size_t blobBytes) {
    std::vector<uint8_t> blob(blobBytes, 0x33);
    pc::TopicSchemaResponse r;
    r.sessionId = sid; r.correlationId = corr; r.topicId = 1; r.version = 1;
    r.descriptorBlob = blob.data(); r.descriptorLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> f(r.encodedSize());
    r.encode(f.data());
    return f;
}
std::vector<uint8_t> clientFirstFrame(int64_t sid, int64_t corr) {
    pc::AuthClientFirst cf;
    cf.sessionId = sid; cf.correlationId = corr; cf.username = "ws1"; cf.clientNonce = "nonce-closing";
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    return b;
}
std::vector<uint8_t> subscribeFrame(int64_t sid, int64_t corr) {
    pc::Subscribe s;
    s.sessionId = sid; s.correlationId = corr; s.topicId = 0; s.partition = 0; s.partitionKnown = 1; s.fromOffset = -1;
    std::vector<uint8_t> b(s.encodedSize()); b.resize(s.encode(b.data()));
    return b;
}
std::vector<uint8_t> disconnectFrame(int64_t sid, int64_t corr) {
    pc::Disconnect d; d.sessionId = sid; d.correlationId = corr;
    std::vector<uint8_t> b(pc::Disconnect::ENCODED_SIZE); b.resize(d.encode(b.data()));
    return b;
}
}  // namespace

// The auth-deadline reaper walks every session still AwaitingFirst / AwaitingFinal.  A login that failed under
// back-pressure stays in the table (Closing) in that auth state: each later reap pass must not count it failed again.
TEST(SessionClosingSpillRegression, TheAuthDeadlineReaperNeverRecountsAClosingLoginAdversarialRegression) {
    Rig rig;
    rig.reliable = true;
    rig.refuse = true;
    const auto o = rig.login("ws1", {kTestPasswordA, ""});   // the AuthServerFirst is spilled: no final is ever sent
    ASSERT_EQ(rig.gw.spillStateForTest(o.sid), SpillState::Spilled);
    const uint64_t failedBefore = rig.gw.loginsFailed();
    constexpr int64_t kPastAuthDeadline = 2'000'000'000;     // the rig's auth deadline is 1 s; the lease is 10 s
    rig.gw.reapExpiredForTest(kPastAuthDeadline);
    ASSERT_EQ(rig.gw.spillStateForTest(o.sid), SpillState::Closing) << "precondition: the timed-out login's 44 is owed";
    EXPECT_EQ(rig.gw.loginsFailed(), failedBefore + 1);
    const std::size_t held = rig.gw.spilledBytesForTest(o.sid);
    for (int i = 1; i <= 50; ++i) rig.gw.reapExpiredForTest(kPastAuthDeadline + i);
    EXPECT_EQ(rig.gw.loginsFailed(), failedBefore + 1) << "every reap pass counted the Closing login failed again";
    EXPECT_EQ(rig.gw.spilledBytesForTest(o.sid), held);
    EXPECT_EQ(rig.gw.loginsAbandoned(), 0u) << "charged before the session ended";

    rig.refuse = false;
    rig.gw.drainPendingResponses(kPastAuthDeadline + 100);
    EXPECT_FALSE(rig.gw.hasSessionForTest(o.sid));
    const auto fin = lastFinal(rig, o.sid);
    ASSERT_TRUE(fin.has_value()) << "the timed-out login's 44 was never delivered";
    EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_EQ(rig.gw.loginsAbandoned(), 1u) << "the unanswered first leg is charged once, at the close";
    EXPECT_EQ(rig.gw.loginsFailed(), failedBefore + 1);
}

// A login and a Subscribe on a Closing session: consumed, counted, never executed, nothing
// queued behind its last answer; a second end (stale security) on it sends nothing more.
TEST(SessionClosingSpillRegression, LoginSubscribeAndASecondEndOnAClosingSessionDoNothingAdversarialRegression) {
    Rig rig;
    rig.withTopic(1);
    rig.reliable = true;
    const int64_t sid = authedSession(rig);
    rig.refuse = true;
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
    const std::size_t held = rig.gw.spilledBytesForTest(sid);
    const uint64_t answerDrops = rig.gw.closingAnswerDrops();
    const uint64_t ended = rig.gw.sessionsEndedUnavailable();
    const uint64_t failed = rig.gw.loginsFailed();

    const auto first = clientFirstFrame(sid, 31);
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(first.data(), first.size()));
    const auto sub = subscribeFrame(sid, 32);
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(sub.data(), sub.size()));
    EXPECT_EQ(rig.gw.closingIngressDrops(), 2u);
    EXPECT_EQ(rig.gw.loginsFailed(), failed) << "a login on a Closing session was executed";
    EXPECT_EQ(rig.gw.spilledBytesForTest(sid), held) << "an answer was queued behind the last one";
    EXPECT_EQ(rig.gw.closingAnswerDrops(), answerDrops) << "a frame was executed (its answer refused)";

    rig.gw.endSessionsSecurityUnavailable();
    EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), ended) << "an already-ended session was ended again";
    EXPECT_EQ(rig.gw.spilledBytesForTest(sid), held);
    EXPECT_EQ(rig.gw.closingAnswerDrops(), answerDrops) << "a second last answer was sent to a Closing session";
    EXPECT_EQ(rig.gw.closingSessions(), 1u);

    rig.refuse = false;
    rig.gw.drainPendingResponses(1);
    EXPECT_EQ(errorsFrom(rig, sid, 0), (std::vector<std::pair<int64_t, int16_t>>{{0, kRevoked}}))
        << "exactly the revoke's 46 - no answer to the login, the Subscribe or the stale end";
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    for (const Frame& f : rig.seen)
        EXPECT_FALSE(f.sid == sid && (f.corr == 31 || f.corr == 32)) << "frame " << f.tid << " answered";
}

// Pool exhaustion: largestSpiller_ picks the Closing session (it holds the most).  It must leave Closing exactly
// (count, ready list, pool bytes) and become Expiring - SessionExpired is its definite end - while the pusher survives.
TEST(SessionClosingSpillRegression, PoolExhaustionExpiresTheLargestClosingSessionCleanlyAdversarialRegression) {
    Rig rig;
    rig.reliable = true;
    rig.refuse = true;
    const std::size_t unit = rune::gateway::SpillPool::bytesFor(bigAnswer(0, 0, 8 * 1024).size());
    const std::size_t quota = ClientGateway::spillSizing().sessionQuota;
    const int64_t closing = authedSession(rig);
    std::vector<int64_t> holders;
    for (int i = 0; i < 7; ++i) holders.push_back(rig.newSession());   // not authenticated: the revoke never ends them
    const int64_t pusher = rig.newSession();
    int64_t corr = 0;
    const auto fill = [&](int64_t sid, std::size_t target) {
        for (int guard = 0; guard < 100'000 && rig.gw.spilledBytesForTest(sid) + unit <= target; ++guard) {
            const auto f = bigAnswer(sid, ++corr, 8 * 1024);
            ASSERT_TRUE(rig.gw.sendControlFrame(sid, f.data(), f.size()));
        }
    };
    fill(closing, quota - 2 * unit);   // the largest holder, under its quota (room for the 46)
    for (const int64_t h : holders) fill(h, quota - 6 * unit);
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_EQ(rig.gw.spillStateForTest(closing), SpillState::Closing);
    ASSERT_EQ(rig.gw.closingSessions(), 1u);
    ASSERT_EQ(rig.gw.noHangStats().spillExpiries, 0u) << "precondition: the pool is not full yet";

    for (int k = 0; k < 10 && rig.gw.spillStateForTest(closing) == SpillState::Closing; ++k) {
        const auto f = bigAnswer(pusher, 9000 + k, 40 * 1024);
        ASSERT_TRUE(rig.gw.sendControlFrame(pusher, f.data(), f.size()));
    }
    EXPECT_EQ(rig.gw.spillStateForTest(closing), SpillState::Expiring) << "the largest (Closing) holder was not expired";
    EXPECT_EQ(rig.gw.closingSessions(), 0u) << "the Closing count leaked through the expiry";
    EXPECT_EQ(rig.gw.spilledBytesForTest(closing), 0u);
    EXPECT_EQ(rig.gw.spillStateForTest(pusher), SpillState::Spilled) << "the pusher was expired instead";
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), holders.size() + 1);

    rig.refuse = false;
    for (int i = 0; i < 4 && rig.gw.spillReadySessionsForTest() != 0; ++i) rig.gw.drainPendingResponses(1 + i);
    EXPECT_FALSE(rig.gw.hasSessionForTest(closing)) << "the expired session was never closed";
    const auto errs = errorsFrom(rig, closing, 0);
    ASSERT_EQ(errs.size(), 1u);
    EXPECT_EQ(errs[0].second, static_cast<int16_t>(pc::ErrorCode::SessionExpired));
    EXPECT_TRUE(rig.gw.hasSessionForTest(pusher));
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
    EXPECT_EQ(rig.gw.noHangStats().spillExpiring, 0u);
}

// Every way out of Closing - the lease reaper, an explicit close, the drain - leaves the count, the ready list and the
// pool exact (a missed decrement would keep every later frame on the slow admit path forever).
TEST(SessionClosingSpillRegression, EveryExitFromClosingLeavesNoStateBehindAdversarialRegression) {
    Rig rig;
    rig.reliable = true;
    constexpr int64_t kLater = 5'000'000'000;
    const int64_t reaped = authedSession(rig);                 // last seen at 0
    const int64_t closed = rig.newSession("10.0.0.9", kLater);  // last seen at kLater
    rig.gw.setSessionAuthForTest(closed, AuthAuthenticated{7, "u", PermissionTable::allowAll(), {}});
    const int64_t drained = rig.newSession("10.0.0.9", kLater);
    rig.gw.setSessionAuthForTest(drained, AuthAuthenticated{7, "u", PermissionTable::allowAll(), {}});
    rig.gw.setNowNsForTest(kLater);
    rig.refuse = true;
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_EQ(rig.gw.closingSessions(), 3u);

    rig.gw.reapExpiredForTest(ClientGateway::kDefaultSessionTimeoutNs + 1);   // only `reaped` is past its lease
    EXPECT_FALSE(rig.gw.hasSessionForTest(reaped));
    EXPECT_TRUE(rig.gw.hasSessionForTest(closed));
    EXPECT_EQ(rig.gw.closingSessions(), 2u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 2u);

    rig.gw.closeSessionForTest(closed);
    EXPECT_EQ(rig.gw.closingSessions(), 1u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 1u);

    rig.refuse = false;
    rig.gw.drainPendingResponses(kLater + 1);
    EXPECT_FALSE(rig.gw.hasSessionForTest(drained));
    EXPECT_EQ(errorsFrom(rig, drained, 0), (std::vector<std::pair<int64_t, int16_t>>{{0, kRevoked}}));
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
    EXPECT_EQ(rig.gw.closingDeadlineCloses(), 0u);
}

// One drain pass whose ready list is [Closing, Spilled, Closing]: both Closing sessions are closed inside the walk
// (head and tail of the list), the Spilled one in between is delivered and stays open - the walk never loses `next`.
TEST(SessionClosingSpillRegression, OneDrainPassClosesClosingSessionsAroundASpilledOneAdversarialRegression) {
    Rig rig;
    rig.reliable = true;
    rig.refuse = true;
    const int64_t c1 = authedSession(rig);
    const int64_t mid = rig.newSession();   // awaiting login: the revoke leaves it alone
    const int64_t c2 = authedSession(rig);
    const auto f1 = bigAnswer(c1, 1, 64);
    ASSERT_TRUE(rig.gw.sendControlFrame(c1, f1.data(), f1.size()));      // c1 joins the ready list first
    const auto fm = bigAnswer(mid, 2, 64);
    ASSERT_TRUE(rig.gw.sendControlFrame(mid, fm.data(), fm.size()));
    rig.gw.revokeAllSessionsForTest("test revoke");                       // c2 joins last
    ASSERT_EQ(rig.gw.spillStateForTest(c1), SpillState::Closing);
    ASSERT_EQ(rig.gw.spillStateForTest(mid), SpillState::Spilled);
    ASSERT_EQ(rig.gw.spillStateForTest(c2), SpillState::Closing);
    ASSERT_EQ(rig.gw.spillReadySessionsForTest(), 3u);

    rig.refuse = false;
    EXPECT_EQ(rig.gw.drainPendingResponses(1), 4) << "c1: its answer + 46, mid: 1, c2: 46";
    EXPECT_FALSE(rig.gw.hasSessionForTest(c1));
    EXPECT_FALSE(rig.gw.hasSessionForTest(c2));
    ASSERT_TRUE(rig.gw.hasSessionForTest(mid));
    EXPECT_EQ(rig.gw.spillStateForTest(mid), SpillState::Normal);
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
    // The list is intact afterwards: a new spill is linked and drained normally.
    rig.refuse = true;
    ASSERT_TRUE(rig.gw.sendControlFrame(mid, fm.data(), fm.size()));
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 1u);
    rig.refuse = false;
    EXPECT_EQ(rig.gw.drainPendingResponses(2), 1);
    EXPECT_EQ(rig.gw.spillReadySessionsForTest(), 0u);
}

// An encrypted Closing session: a plaintext Disconnect is unauthenticated at the gate (session ids are guessable) and
// must not end it; it ends at its deadline - exactly once, and at the deadline, never before.
TEST(SessionClosingSpillRegression, ASpoofedDisconnectNeverEndsAnEncryptedClosingSessionAdversarialRegression) {
    Rig rig;
    rig.reliable = true;
    constexpr int64_t t0 = 7'000;
    rig.gw.setNowNsForTest(t0);
    const int64_t sid = authedSession(rig);
    rig.refuse = true;
    rig.gw.revokeAllSessionsForTest("test revoke");
    ASSERT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
    rig.gw.sessionCryptoForTest(sid)->encrypted = true;
    const auto d = disconnectFrame(sid, 77);
    EXPECT_TRUE(rig.gw.applyClientFrameForTest(d.data(), d.size())) << "consumed (dropped), not deferred";
    EXPECT_TRUE(rig.gw.hasSessionForTest(sid)) << "a forged plaintext Disconnect ended an encrypted session";
    EXPECT_EQ(rig.gw.closingIngressDrops(), 1u);
    EXPECT_EQ(rig.gw.closingSessions(), 1u);
    const int64_t deadline = t0 + ClientGateway::kDefaultSessionTimeoutNs;
    for (int i = 0; i < 3; ++i) rig.gw.drainPendingResponses(deadline - 1);
    EXPECT_TRUE(rig.gw.hasSessionForTest(sid)) << "closed before its deadline";
    rig.gw.drainPendingResponses(deadline);
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    rig.gw.drainPendingResponses(deadline + 1);
    EXPECT_EQ(rig.gw.closingDeadlineCloses(), 1u);
    EXPECT_EQ(rig.gw.closingSessions(), 0u);
    EXPECT_EQ(rig.gw.egressSpillUsedBytesForTest(), 0u);
}

// ── Issue #206: a halted broker sends the client to another broker, it never rejects it for good ──────────────────────
// A halted `__security` view means THIS broker cannot vouch for its security state: the retriable AuthUnavailable (48),
// never the terminal AuthenticationFailed (44) / SessionRevoked (46).  Still fail-closed: nothing is granted here.
namespace {
/// One SCRAM login whose `betweenLegs` runs after the AuthServerFirst and before the AuthClientFinal is sent.
std::optional<pc::AuthServerFinal> loginWithBetweenLegs(Rig& rig, int64_t sid, const std::string& user, const std::string& pw,
                                                        const std::function<void()>& betweenLegs) {
    const std::string nonce = "nonce-" + std::to_string(++rig.corr);
    pc::AuthClientFirst cf;
    cf.sessionId = sid; cf.correlationId = ++rig.corr; cf.username = user; cf.clientNonce = nonce;
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    const std::size_t before = rig.seen.size();
    rig.send(b);
    const Frame* f1 = nullptr;
    for (std::size_t i = before; i < rig.seen.size(); ++i)
        if (rig.seen[i].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFirst)) f1 = &rig.seen[i];
    if (f1 == nullptr) return std::nullopt;
    const auto first = pc::AuthServerFirst::decode(f1->bytes.data(), f1->bytes.size());
    auto ans = scramAnswer(user, nonce, *first, {pw, ""}, sid, cf.correlationId);
    std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
    betweenLegs();
    const std::size_t mid = rig.seen.size();
    rig.send(fb);
    for (std::size_t i = mid; i < rig.seen.size(); ++i)
        if (rig.seen[i].tid == static_cast<uint16_t>(pc::TemplateId::AuthServerFinal))
            return pc::AuthServerFinal::decode(rig.seen[i].bytes.data(), rig.seen[i].bytes.size());
    return std::nullopt;
}
constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
}  // namespace

TEST(AuthLoginRegression, AHaltedBrokerAnswersBothLoginLegsRetriablyRegression) {
    // (a) Halted before the first leg: AuthServerFinal 48, no retry hint, the session ends; not a failed login.
    {
        Rig rig;
        rune::security::ApplyError err; err.offset = 3; err.detail = "test halt";
        rig.view.onHalted(err);
        const auto o = rig.login("ws1", {kTestPasswordA, ""});
        ASSERT_TRUE(o.fin.has_value());
        EXPECT_TRUE(o.firstBytes.empty()) << "answered at the first leg";
        EXPECT_EQ(o.fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable));
        EXPECT_EQ(o.fin->retryAfterMs, 0u);
        EXPECT_FALSE(rig.gw.hasSessionForTest(o.sid));
        EXPECT_EQ(rig.gw.loginsFailed(), 0u) << "a halt is no verdict on the credentials";
        EXPECT_EQ(rig.gw.loginsUnavailable(), 1u);
        EXPECT_TRUE(rig.logContains("reason=security-halted"));
    }
    // (b) Halted between the legs, repeatedly from the same (user, host): 48 each time, the throttle is never charged -
    // the same user from the same host logs in at once on a serving view afterwards (3 charged failures would make that
    // attempt the rate-limited 44).
    {
        Rig rig;
        std::vector<std::unique_ptr<SecurityView>> views;
        for (int i = 0; i < 4; ++i) {
            views.push_back(std::make_unique<SecurityView>(rig.chain.state()));
            SecurityView* v = views.back().get();
            rig.gw.setAuthMode(GatewayAuthMode::Required, v);
            const int64_t sid = rig.newSession("10.6.0.1");
            const auto fin = loginWithBetweenLegs(rig, sid, "ws1", kTestPasswordA, [v] {
                rune::security::ApplyError err; err.offset = 3; err.detail = "test halt between the legs";
                v->onHalted(err);
            });
            ASSERT_TRUE(fin.has_value()) << i;
            EXPECT_EQ(fin->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable)) << i;
            EXPECT_EQ(fin->retryAfterMs, 0u) << i;
            EXPECT_TRUE(fin->serverSignature.empty()) << i;
            EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << i;
        }
        EXPECT_EQ(rig.gw.loginsFailed(), 0u);
        EXPECT_EQ(rig.gw.loginsUnavailable(), 4u);
        EXPECT_TRUE(rig.logContains("reason=security-halted"));
        views.push_back(std::make_unique<SecurityView>(rig.chain.state()));   // "un-halted": a serving view again
        rig.gw.setAuthMode(GatewayAuthMode::Required, views.back().get());
        const auto ok = rig.login("ws1", {kTestPasswordA, ""}, "10.6.0.1");
        ASSERT_TRUE(ok.fin.has_value());
        EXPECT_EQ(ok.fin->errorCode, 0) << "the halted finals were charged to the throttle";
        EXPECT_EQ(rig.gw.loginsRateLimited(), 0u);
    }
}

TEST(AuthLoginRegression, AHaltEndsLiveSessionsRetriablyNotRevokedRegression) {
    // (a) Authenticated (with a gateway subscription) and must-change-password sessions: exactly one session-level 48
    // each, no 46 on any correlation id, counted as ended-unavailable (never revoked), closed.
    {
        Rig rig;
        rig.withTopic(1);
        const int64_t sid = authedSession(rig);
        subscribe(rig, sid, 11, 0);
        ASSERT_NE(rig.last(static_cast<uint16_t>(pc::TemplateId::SubscribeResponse), sid), nullptr);
        const int64_t pcr = rig.newSession();
        rig.gw.setSessionAuthForTest(pcr, AuthPasswordChangeRequired{8, "v"});
        const int64_t waiting = rig.newSession();   // awaiting its login: left alone
        const std::size_t mark = rig.seen.size();
        rig.gw.onSecurityHalted(0);
        using Errs = std::vector<std::pair<int64_t, int16_t>>;
        EXPECT_EQ(errorsFrom(rig, sid, mark), (Errs{{0, kUnavailable}})) << "no SessionRevoked on the subscription";
        EXPECT_EQ(errorsFrom(rig, pcr, mark), (Errs{{0, kUnavailable}}));
        EXPECT_TRUE(errorsFrom(rig, waiting, mark).empty());
        EXPECT_EQ(rig.gw.sessionsRevoked(), 0u);
        EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), 2u);
        EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
        EXPECT_FALSE(rig.gw.hasSessionForTest(pcr));
        EXPECT_TRUE(rig.gw.hasSessionForTest(waiting));
        const std::size_t after = rig.seen.size();
        rig.gw.onSecurityHalted(0);
        EXPECT_EQ(rig.seen.size(), after) << "a second halt sends nothing";
        EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), 2u);
    }
    // (b) Idempotent while the 48 is still spilled (Closing): a second halt queues and counts nothing more.
    {
        Rig rig;
        rig.reliable = true;
        const int64_t sid = authedSession(rig);
        rig.refuse = true;
        const std::size_t mark = rig.seen.size();
        rig.gw.onSecurityHalted(0);
        ASSERT_EQ(rig.gw.spillStateForTest(sid), SpillState::Closing);
        const std::size_t held = rig.gw.spilledBytesForTest(sid);
        rig.gw.onSecurityHalted(0);
        EXPECT_EQ(rig.gw.spilledBytesForTest(sid), held);
        EXPECT_EQ(rig.gw.sessionsEndedUnavailable(), 1u);
        EXPECT_EQ(rig.gw.sessionsRevoked(), 0u);
        rig.refuse = false;
        rig.gw.drainPendingResponses(1);
        EXPECT_EQ(errorsFrom(rig, sid, mark), (std::vector<std::pair<int64_t, int16_t>>{{0, kUnavailable}}));
        EXPECT_FALSE(rig.gw.hasSessionForTest(sid));
    }
}
