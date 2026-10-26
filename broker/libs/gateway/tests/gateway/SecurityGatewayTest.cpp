// SecurityGatewayTest.cpp — authentication Task 10 at the gateway (no Aeron): the cluster-key proof of
// SecurityBootstrap (D4), the `__security` DDL route (Redirect / NotLeader / Internal like catalog DDL), the Subscribe
// on -7, and the pre-authentication gate.  makeForTest + the apply*ForTest seams.
#include "support/SecurityEventChain.h"
#include <rune/gateway/BootstrapProof.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/SecurityMessages.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <map>
#include <random>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-sec-gw-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct RecordingSink final : IAdminRequestSink {
    int securityDdls = 0, bootstraps = 0, otherSubmits = 0;
    std::vector<std::pair<int64_t, int64_t>> secSubs, secUnsubs, catSubs, catUnsubs;
    std::string lastAuthenticated;
    AdminSubmit submitDefineTopic(int64_t, int64_t, std::string_view, uint32_t, std::string_view, bool, const uint8_t*,
                                  uint32_t, uint8_t, int16_t, const rune::protocol::TopicConfigView&,
                                  const DdlMetaView&, std::string_view) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t) override { return true; }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>,
                                       const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t, const uint8_t*,
                                uint32_t, const DdlMetaView&) override { ++otherSubmits; return AdminSubmit::Accepted; }
    bool submitCatalogSubscribe(int64_t s, int64_t c, int64_t) override { catSubs.push_back({s, c}); return true; }
    bool submitCatalogUnsubscribe(int64_t s, int64_t id) override { catUnsubs.push_back({s, id}); return true; }
    /// `securityDdls` counts the AUTHORIZED forwards; a denied one (PR #410 review F1: forwarded with the gateway's
    /// verdict, which the leader answers) is recorded in `denied` by correlation id.
    std::map<int64_t, std::string> denied;
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl& w, const DdlMetaView& m, std::string_view denial, bool) override {
        if (!denial.empty()) { denied[w.correlationId] = std::string(denial); return AdminSubmit::Accepted; }
        ++securityDdls; lastAuthenticated = std::string(m.authenticated); return AdminSubmit::Accepted;
    }
    AdminSubmit submitNamespaceDdl(const pc::NamespaceDdlRequest&, const DdlMetaView&, std::string_view, uint16_t) override { return AdminSubmit::Accepted; }
    AdminSubmit submitListNamespaces(int64_t, int64_t, uint32_t, uint16_t, uint32_t, bool) override { return AdminSubmit::Accepted; }
    AdminSubmit submitSecurityBootstrap(const pc::SecurityBootstrap&, const DdlMetaView&) override { ++bootstraps; return AdminSubmit::Accepted; }
    bool submitSecuritySubscribe(int64_t s, int64_t c, int64_t) override { secSubs.push_back({s, c}); return true; }
    bool submitSecurityUnsubscribe(int64_t s, int64_t id) override { secUnsubs.push_back({s, id}); return true; }
};

struct Seen { uint16_t tid = 0; int64_t corr = 0; int16_t code = 0; std::string text; };

struct Rig {
    TempDir dir;
    std::unique_ptr<TopicRegistry> registry;
    ClientGateway gw;
    RecordingSink admin;
    std::vector<Seen> seen;
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink;
    std::shared_ptr<spdlog::logger> previous;
    int64_t sid = 0, corr = 100;

    explicit Rig(int32_t clientVersion = pc::kClientVersionAuth)
        : gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { onFrame(b, n); return 1; }, 1)) {
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setAdminSink(&admin);
        sid = gw.injectTestSession(0, clientVersion);
        logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
        previous = spdlog::default_logger();
        auto capture = std::make_shared<spdlog::logger>("sec-gw-capture", logSink);
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
        Seen s;
        s.tid = h->templateId; s.corr = h->correlationId;
        if (auto e = pc::ErrorResponse::decode(b, n)) { s.code = e->code; s.text = std::string(e->message); }
        else if (auto d = pc::DdlResponse::decode(b, n)) { s.code = static_cast<int16_t>(d->errorCode); s.text = std::string(d->detail); }
        else if (auto r = pc::SubscribeResponse::decode(b, n)) { s.code = r->errorCode; }
        else if (auto p = pc::ProduceResponse::decode(b, n)) { s.code = p->errorCode; }
        seen.push_back(std::move(s));
    }
    [[nodiscard]] const Seen* answerFor(int64_t c) const {
        for (const auto& s : seen) if (s.corr == c) return &s;
        return nullptr;
    }
    std::vector<uint8_t> createRole(int64_t c) {
        pc::SecurityDdl m;
        m.sessionId = sid; m.correlationId = c; m.ext.clientName = "t"; m.ext.requestId = {1, static_cast<uint64_t>(c)};
        m.kind = pc::SecurityOpKind::CreateRole; m.name = "r";
        std::vector<uint8_t> b(m.encodedSize()); b.resize(m.encode(b.data())); return b;
    }
};

rune::peerlink::ClusterKeys keysFor(const std::string& a, const std::string& b = {}) {
    auto d = [](const std::string& s) {
        auto k = rune::crypto::deriveClusterKey(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
        return *k;
    };
    rune::peerlink::ClusterKeys keys;
    keys.current = d(a);
    if (!b.empty()) keys.next = d(b);
    return keys;
}

struct BootFrame {
    std::vector<uint8_t> salt = std::vector<uint8_t>(16, 7);
    std::array<uint8_t, 64> stored{}, server{};
    std::vector<uint8_t> bytes;
    BootFrame(int64_t sid, int64_t corr, const std::string& keyContents) {
        stored.fill(1); server.fill(2);
        pc::SecurityBootstrap m;
        m.sessionId = sid; m.correlationId = corr; m.adminName = "admin";
        m.verifier = pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(salt.data()), 16), 4096,
                                      std::string_view(reinterpret_cast<const char*>(stored.data()), 64),
                                      std::string_view(reinterpret_cast<const char*>(server.data()), 64)};
        m.fakeSaltSeed.fill(5); m.nonce.fill(6);
        if (keyContents.empty()) {
            m.mac.fill(0xEE);
        } else {
            const auto k = rune::crypto::deriveClusterKey(std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(keyContents.data()), keyContents.size()));
            std::vector<uint8_t> in;
            pc::bootstrapMacInput(m, in);
            rune::crypto::DigestBytes mac;
            rune::crypto::hmac(rune::crypto::Digest::Sha512, k->master, in, mac);
            std::memcpy(m.mac.data(), mac.bytes.data(), 64);
        }
        bytes.resize(m.encodedSize());
        bytes.resize(m.encode(bytes.data()));
    }
};

const std::string kA(40, 'a'), kB(40, 'b'), kC(40, 'c');

}  // namespace

TEST(SecurityBootstrapRegression, BootstrapWithoutAValidProofIsRefusedAndLogged) {
    Rig rig;
    rig.gw.setClusterKeys(keysFor(kA));
    const BootFrame f(rig.sid, 7, kC);   // a MAC under ANOTHER cluster's key
    rig.gw.applyDdlFrameForTest(f.bytes.data(), f.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 0) << "never forwarded";
    const Seen* a = rig.answerFor(7);
    ASSERT_NE(a, nullptr) << "answered, never hung";
    EXPECT_EQ(a->tid, static_cast<uint16_t>(pc::TemplateId::DdlResponse));
    EXPECT_EQ(a->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(a->text, "bootstrap proof does not match this cluster's key");
    EXPECT_TRUE(rig.logContains("security: bootstrap refused from"));
    EXPECT_TRUE(rig.logContains("bad cluster-key proof"));
    EXPECT_EQ(rig.gw.bootstrapsRefused(), 1u);
    // A garbage MAC too.
    const BootFrame g(rig.sid, 8, "");
    rig.gw.applyDdlFrameForTest(g.bytes.data(), g.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 0);
    EXPECT_EQ(rig.gw.bootstrapsRefused(), 2u);
}

TEST(SecurityBootstrapRegression, BootstrapUnderTheNextKeyIsAccepted) {
    Rig rig;
    rig.gw.setClusterKeys(keysFor(kA, kB));   // a rolling restart: current A, next B
    const BootFrame underNext(rig.sid, 1, kB);
    rig.gw.applyDdlFrameForTest(underNext.bytes.data(), underNext.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 1) << "a proof under the NEXT key is accepted";
    const BootFrame underCurrent(rig.sid, 2, kA);
    rig.gw.applyDdlFrameForTest(underCurrent.bytes.data(), underCurrent.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 2) << "and under the current one";
    const BootFrame underOther(rig.sid, 3, kC);
    rig.gw.applyDdlFrameForTest(underOther.bytes.data(), underOther.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 2) << "a third key is neither";
}

TEST(SecurityBootstrapRegression, ABrokerWithoutAClusterKeyRefusesEveryBootstrap) {
    Rig rig;   // InProcess / dev: no key
    const BootFrame f(rig.sid, 5, kA);
    rig.gw.applyDdlFrameForTest(f.bytes.data(), f.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 0);
    const Seen* a = rig.answerFor(5);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

TEST(SecurityRouteRegression, SecurityDdlFollowsTheSecurityRouteLikeCatalogDdl) {
    Rig rig;
    const auto frame = [&](int64_t c) { return rig.createRole(c); };
    // LocalLeader (default): forwarded.
    auto f = frame(1);
    rig.gw.applyDdlFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.admin.securityDdls, 1);
    // Remote: Redirect to the security leader's endpoint; the CATALOG route is independent.
    rig.gw.setSecurityRoute(RouteRemote{"aeron:udp?endpoint=h:1234"});
    f = frame(2);
    rig.gw.applyDdlFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.admin.securityDdls, 1);
    ASSERT_NE(rig.answerFor(2), nullptr);
    EXPECT_EQ(rig.answerFor(2)->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(rig.answerFor(2)->text, "aeron:udp?endpoint=h:1234");
    // Unknown: NotLeader.  Halted: Internal naming the log.
    rig.gw.setSecurityRoute(RouteUnknown{});
    f = frame(3);
    rig.gw.applyDdlFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.answerFor(3)->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    rig.gw.setSecurityRoute(RouteHalted{9});
    f = frame(4);
    rig.gw.applyDdlFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.answerFor(4)->code, static_cast<int16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(rig.answerFor(4)->text, "security apply halted at offset 9");
    // The catalog route still says LocalLeader: catalog DDL is unaffected.
    EXPECT_TRUE(std::holds_alternative<RouteLocalLeader>(rig.gw.catalogRoute()));
}

TEST(SecurityRouteRegression, AnOldSessionIsRefusedSecurityDdl) {
    Rig rig(pc::kClientVersionCatalog);   // a v8 session
    const auto f = rig.createRole(1);
    rig.gw.applyDdlFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.admin.securityDdls, 0);
    ASSERT_NE(rig.answerFor(1), nullptr);
    EXPECT_EQ(rig.answerFor(1)->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

TEST(SecuritySubscribeRegression, ASubscribeOnTheSecurityLogIsForwardedToItsReadServer) {
    Rig rig;
    pc::Subscribe s;
    s.sessionId = rig.sid; s.correlationId = 3; s.topicId = pc::kSecurityTopicId; s.partition = 0; s.partitionKnown = 1;
    s.fromOffset = -2;
    std::vector<uint8_t> b(s.encodedSize()); b.resize(s.encode(b.data()));
    rig.gw.applySubscribeForTest(b.data(), b.size());
    ASSERT_EQ(rig.admin.secSubs.size(), 1u);
    EXPECT_EQ(rig.admin.secSubs[0].second, 3);
    EXPECT_TRUE(rig.admin.catSubs.empty()) << "never the catalog's server";
    // A security subscription id's Unsubscribe goes to the security server, a catalog id's to the catalog's.
    const int64_t secId = kCatalogSubscriptionIdBase | kSecuritySubscriptionIdBit | 5;
    const int64_t catId = kCatalogSubscriptionIdBase | 5;
    for (const int64_t id : {secId, catId}) {
        pc::Unsubscribe u;
        u.sessionId = rig.sid; u.subscriptionId = id;
        std::vector<uint8_t> ub(64); ub.resize(u.encode(ub.data()));
        rig.gw.applyUnsubscribeForTest(ub.data(), ub.size());
    }
    ASSERT_EQ(rig.admin.secUnsubs.size(), 1u);
    EXPECT_EQ(rig.admin.secUnsubs[0].second, secId);
    ASSERT_EQ(rig.admin.catUnsubs.size(), 1u);
    EXPECT_EQ(rig.admin.catUnsubs[0].second, catId);
    // partition != 0: InvalidRequest; a pre-v9 session: UnknownTopic.
    s.correlationId = 4; s.partition = 1;
    b.resize(s.encodedSize()); b.resize(s.encode(b.data()));
    rig.gw.applySubscribeForTest(b.data(), b.size());
    ASSERT_NE(rig.answerFor(4), nullptr);
    EXPECT_EQ(rig.answerFor(4)->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

TEST(SecuritySubscribeRegression, AnOldSessionCannotReadTheSecurityLog) {
    Rig rig(pc::kClientVersionCatalog);
    pc::Subscribe s;
    s.sessionId = rig.sid; s.correlationId = 3; s.topicId = pc::kSecurityTopicId; s.partition = 0; s.partitionKnown = 1;
    std::vector<uint8_t> b(s.encodedSize()); b.resize(s.encode(b.data()));
    rig.gw.applySubscribeForTest(b.data(), b.size());
    EXPECT_TRUE(rig.admin.secSubs.empty());
    ASSERT_NE(rig.answerFor(3), nullptr);
    EXPECT_EQ(rig.answerFor(3)->code, static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
}

// ── The pre-authentication gate (controller ruling; D13) ───────────────────────────────────────────────────────────
namespace {
SecurityView emptyView() { return SecurityView{rune::security::SecurityState{}}; }
SecurityView adminView() {
    rune::security::SecurityReplayer r;
    auto events = rune::security::testing::goldenScriptV1();
    auto b = rune::security::encode(events[0]);   // Bootstrap admin
    (void)r.feed(0, *b);
    return SecurityView{r.state()};
}
}  // namespace

TEST(AuthGateRegression, AnAnonymousBootstrapWindowSessionSendingAnythingButBootstrapIsRefusedAndEnded) {
    Rig rig;
    auto view = emptyView();
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    rig.gw.setClusterKeys(keysFor(kA));
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
    const auto f = rig.createRole(11);   // not a bootstrap
    rig.gw.applyClientFrameForTest(f.data(), f.size());
    EXPECT_EQ(rig.admin.securityDdls, 0) << "never reached the admin ring";
    const Seen* a = rig.answerFor(11);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->tid, static_cast<uint16_t>(pc::TemplateId::DdlResponse)) << "in the request's own response type";
    EXPECT_EQ(a->code, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(rig.gw.hasSessionForTest(rig.sid)) << "the session ends (D13)";
    EXPECT_EQ(rig.gw.authGateRefusals(), 1u);
}

TEST(AuthGateRegression, TheBootstrapWindowAdmitsExactlyBootstrapWithAValidMac) {
    Rig rig;
    auto view = emptyView();
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    rig.gw.setClusterKeys(keysFor(kA));
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
    const BootFrame bad(rig.sid, 20, kC);
    rig.gw.applyClientFrameForTest(bad.bytes.data(), bad.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 0);
    ASSERT_NE(rig.answerFor(20), nullptr);
    EXPECT_EQ(rig.answerFor(20)->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "bad MAC: refused by the proof";
    EXPECT_TRUE(rig.gw.hasSessionForTest(rig.sid)) << "a bad proof does not end the window session";
    const BootFrame good(rig.sid, 21, kA);
    rig.gw.applyClientFrameForTest(good.bytes.data(), good.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 1);
}

TEST(AuthGateRegression, AfterBootstrapAnonymousSessionsAreRefusedEvenForBootstrap) {
    Rig rig;
    auto view = adminView();   // a Bootstrap event applied
    ASSERT_TRUE(view.state().hasSecurityAdmin());
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    rig.gw.setClusterKeys(keysFor(kA));
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});   // offered the window BEFORE it closed
    const BootFrame good(rig.sid, 30, kA);
    rig.gw.applyClientFrameForTest(good.bytes.data(), good.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 0) << "the window is closed";
    ASSERT_NE(rig.answerFor(30), nullptr);
    EXPECT_EQ(rig.answerFor(30)->code, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(rig.gw.hasSessionForTest(rig.sid));
}

TEST(AuthGateRegression, SessionLevelFramesAndOpenSessionsAreNeverGated) {
    Rig rig;
    auto view = emptyView();
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
    pc::Keepalive k;
    k.sessionId = rig.sid;
    std::vector<uint8_t> kb(64); kb.resize(k.encode(kb.data()));
    rig.gw.applyClientFrameForTest(kb.data(), kb.size());
    EXPECT_TRUE(rig.gw.hasSessionForTest(rig.sid)) << "a Keepalive keeps an unauthenticated session alive";
    EXPECT_EQ(rig.gw.authGateRefusals(), 0u);
    // An in-process gateway gates nothing.
    Rig open;
    open.gw.setSessionAuthForTest(open.sid, AuthAwaitingFirst{INT64_MAX});   // ignored: authMode_ is InProcess
    const auto f = open.createRole(1);
    open.gw.applyClientFrameForTest(f.data(), f.size());
    EXPECT_EQ(open.admin.securityDdls, 1);
}

// Review C I2 (ruling: D3 recovery stays): the window follows "no ENABLED admin", cached in the view.
TEST(AuthGateRegression, BootstrapWindowFollowsTheRulingRegression) {
    rune::node::test::SecurityEventChain chain;
    auto ev0 = chain.emit(rune::security::BootstrapOps{rune::node::test::testVerifier(0), {}}, "admin", "BOOTSTRAP", "bootstrap");
    SecurityView view{rune::security::SecurityState{}};
    EXPECT_TRUE(view.bootstrapWindowOpen()) << "a fresh cluster";
    ASSERT_TRUE(view.onApplied(0, *rune::security::encode(ev0)).has_value());
    EXPECT_FALSE(view.bootstrapWindowOpen()) << "an enabled admin exists";
    auto dis = chain.emit(rune::security::AlterUserOps{rune::security::DisableUser{}}, "admin", "ALTER USER admin DISABLE;");
    ASSERT_TRUE(view.onApplied(1, *rune::security::encode(dis)).has_value());
    EXPECT_TRUE(view.bootstrapWindowOpen()) << "the last admin was disabled: D3 recovery, still MAC-gated";
    auto en = chain.emit(rune::security::AlterUserOps{rune::security::EnableUser{}}, "admin", "ALTER USER admin ENABLE;");
    ASSERT_TRUE(view.onApplied(2, *rune::security::encode(en)).has_value());
    EXPECT_FALSE(view.bootstrapWindowOpen());
    view.onHalted(rune::security::ApplyError{});
    EXPECT_FALSE(view.bootstrapWindowOpen()) << "a halted mirror fails closed";
    // The gate follows the cached value for a new session.
    Rig rig;
    SecurityView open{rune::security::SecurityState{}};
    rig.gw.setAuthMode(GatewayAuthMode::Required, &open);
    rig.gw.setClusterKeys(keysFor(kA));
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
    const BootFrame good(rig.sid, 40, kA);
    rig.gw.applyClientFrameForTest(good.bytes.data(), good.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 1);
}

// Review C I3: Produce skips the pre-dispatch gate (no extra header decode / lookup per frame) and is checked inside
// handleProduce on the session it already looked up.
TEST(AuthGateRegression, ProduceFromAGatedSessionIsRefusedInItsOwnResponseAndEndedRegression) {
    auto produceFrame = [](int64_t sid, int64_t corr) {
        rune::protocol::RecordBatchBuilder rb(0, 1000);
        rb.append("k", "v", 1000);
        const auto batch = rb.build();
        pc::Produce m;
        m.sessionId = sid; m.correlationId = corr; m.topicId = 1; m.partition = 0; m.partitionKnown = 1; m.acks = 1;
        m.producerId = -1; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> b(m.encodedSize() + 8); b.resize(m.encode(b.data()));
        return b;
    };
    auto code = [](const Seen* a) { return a ? a->code : int16_t{-1}; };
    {
        Rig rig;
        auto view = adminView();
        rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
        rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
        const auto f = produceFrame(rig.sid, 61);
        rig.gw.applyClientFrameForTest(f.data(), f.size());
        const Seen* a = rig.answerFor(61);
        ASSERT_NE(a, nullptr);
        EXPECT_EQ(a->tid, static_cast<uint16_t>(pc::TemplateId::ProduceResponse)) << "its own response type";
        EXPECT_EQ(code(a), static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
        EXPECT_FALSE(rig.gw.hasSessionForTest(rig.sid));
        EXPECT_EQ(rig.gw.authGateRefusals(), 1u);
    }
    {   // a one-time-password session: PasswordChangeRequired, and the session stays
        Rig rig;
        auto view = adminView();
        rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
        rig.gw.setSessionAuthForTest(rig.sid, AuthPasswordChangeRequired{1, "admin"});
        const auto f = produceFrame(rig.sid, 62);
        rig.gw.applyClientFrameForTest(f.data(), f.size());
        EXPECT_EQ(code(rig.answerFor(62)), static_cast<int16_t>(pc::ErrorCode::PasswordChangeRequired));
        EXPECT_TRUE(rig.gw.hasSessionForTest(rig.sid));
    }
}

// X013: AuthAnonymous is the state of nobody who logged in.  On an authenticating (Required) broker it holds no grants:
// every request that needs one is denied.  RED: restore the allow-all arm for AuthAnonymous in permsOf_ (or skip the
// check for it in handleProduce) - the produce is no longer answered NotAuthorized.
TEST(AuthGateRegression, AnAnonymousSessionOnAnAuthenticatingBrokerIsDeniedEverythingRegression) {
    Rig rig;
    auto view = adminView();
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    rig.gw.setSessionAuthForTest(rig.sid, AuthAnonymous{});
    rune::protocol::RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    const auto batch = rb.build();
    pc::Produce m;
    m.sessionId = rig.sid; m.correlationId = 71; m.topicId = 1; m.partition = 0; m.partitionKnown = 1; m.acks = 1;
    m.producerId = -1; m.baseSequence = -1;
    m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> f(m.encodedSize() + 8); f.resize(m.encode(f.data()));
    rig.gw.applyClientFrameForTest(f.data(), f.size());
    const Seen* a = rig.answerFor(71);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->tid, static_cast<uint16_t>(pc::TemplateId::ProduceResponse));
    EXPECT_EQ(a->code, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    // The same session on a dev broker is allow-all (the answer is anything but 45).
    Rig dev;
    dev.gw.setSessionAuthForTest(dev.sid, AuthAnonymous{});
    m.sessionId = dev.sid; m.correlationId = 72;
    f.assign(m.encodedSize() + 8, 0); f.resize(m.encode(f.data()));
    dev.gw.applyClientFrameForTest(f.data(), f.size());
    if (const Seen* d = dev.answerFor(72)) EXPECT_NE(d->code, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
}

// X103: bad bootstrap proofs come from unauthenticated sessions.  Counted every time, logged the first time and every
// 1024th; the rest only bump the suppression counter.  RED: log every refusal (drop the due() gate).
TEST(AuthLoginRegression, UnauthenticatedLogLinesAreRateLimitedRegression) {
    Rig rig;
    rig.gw.setClusterKeys(keysFor(kA));
    constexpr int kFlood = 2500;
    for (int i = 0; i < kFlood; ++i) {
        const BootFrame f(rig.sid, 1000 + i, kC);   // a MAC under ANOTHER cluster's key
        rig.gw.applyDdlFrameForTest(f.bytes.data(), f.bytes.size());
    }
    EXPECT_EQ(rig.gw.bootstrapsRefused(), static_cast<uint64_t>(kFlood));
    EXPECT_EQ(rig.gw.unauthLogSuppressed(), static_cast<uint64_t>(kFlood - 3));
    int lines = 0;
    for (const std::string& l : rig.logSink->last_formatted()) if (l.find("bootstrap refused") != std::string::npos) ++lines;
    EXPECT_EQ(lines, 3) << "first, 1025th and 2049th";
}

// X103 (review I2): a malformed first login leg is driven by an unauthenticated session.  Counted every time, logged the
// first time and every 1024th.  RED: drop the due(6) gate - every frame logs.
TEST(AuthLoginRegression, MalformedFirstLegLogLinesAreRateLimitedRegression) {
    Rig rig;
    auto view = adminView();
    rig.gw.setAuthMode(GatewayAuthMode::Required, &view);
    constexpr int kFlood = 1100;
    for (int i = 0; i < kFlood; ++i) {
        rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
        pc::AuthClientFirst cf;
        cf.sessionId = rig.sid; cf.correlationId = 500 + i; cf.mechanism = 0;   // not SCRAM-SHA-512: undecodable as a first leg
        std::vector<uint8_t> b(cf.encodedSize() + 8); b.resize(cf.encode(b.data()));
        rig.gw.applyClientFrameForTest(b.data(), b.size());
        if (!rig.gw.hasSessionForTest(rig.sid)) rig.sid = rig.gw.injectTestSession(0, pc::kClientVersionAuth);
    }
    int lines = 0;
    for (const std::string& l : rig.logSink->last_formatted()) if (l.find("reason=malformed") != std::string::npos) ++lines;
    EXPECT_EQ(lines, 2) << "first and 1025th only";
    EXPECT_EQ(rig.gw.unauthLogSuppressed(), static_cast<uint64_t>(kFlood - 2));
}

// Adversarial review of PR #459 (issue #15): a wiped / new broker catching up has an EMPTY replica - its view says
// "never bootstrapped" - but the cluster may have been bootstrapped long ago.  A valid bootstrap proof (the key is the
// cluster's) sent through the pre-auth gate must not reach the admin thread while the broker is not current, and a
// fresh connect is never told Anonymous; once current, the same frame is forwarded.
TEST(SecurityBootstrapRegression, AValidBootstrapOnACatchingUpBrokerIsNeverForwardedAdversarialRegression) {
    Rig rig;
    SecurityView open{rune::security::SecurityState{}};
    rig.gw.setAuthMode(GatewayAuthMode::Required, &open);
    rig.gw.setClusterKeys(keysFor(kA));
    rig.gw.setSecurityServing(false);
    rig.gw.setSessionAuthForTest(rig.sid, AuthAwaitingFirst{INT64_MAX});
    EXPECT_EQ(rig.gw.connectAuthStateForTest(), pc::AuthState::MustAuthenticate);
    for (int64_t corr : {50, 51}) {   // via the pre-auth gate and via the DDL route
        const BootFrame good(rig.sid, corr, kA);
        if (corr == 50) rig.gw.applyClientFrameForTest(good.bytes.data(), good.bytes.size());
        else rig.gw.applyDdlFrameForTest(good.bytes.data(), good.bytes.size());
        EXPECT_EQ(rig.admin.bootstraps, 0) << corr << ": never forwarded while not current";
        const Seen* a = rig.answerFor(corr);
        ASSERT_NE(a, nullptr) << corr << ": answered, never hung";
        EXPECT_EQ(a->code, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable)) << corr;
    }
    rig.gw.setSecurityServing(true);
    const BootFrame good(rig.sid, 52, kA);
    rig.gw.applyClientFrameForTest(good.bytes.data(), good.bytes.size());
    EXPECT_EQ(rig.admin.bootstraps, 1) << "current again: forwarded";
}

// PR #611 review, adversarial pass (issue #232, sibling entry point): a refused bootstrap is driven by an
// UNAUTHENTICATED session and logs where it came from - the session's client-supplied egress channel.  A `\n` in that
// channel must never forge a log line.
TEST(SecurityBootstrapRegression, ABootstrapFromAChannelWithControlBytesIsLoggedEscapedAdversarialRegression) {
    Rig rig;
    rig.gw.setClusterKeys(keysFor(kA));
    const int64_t sid = rig.gw.injectTestSessionOnChannel("aeron:udp?endpoint=h:1\nauth: login ok user=admin");
    const BootFrame f(sid, 7, kC);
    rig.gw.applyDdlFrameForTest(f.bytes.data(), f.bytes.size());
    ASSERT_EQ(rig.gw.bootstrapsRefused(), 1u);
    bool sawLine = false;
    for (std::string l : rig.logSink->last_formatted()) {
        while (!l.empty() && (l.back() == '\n' || l.back() == '\r')) l.pop_back();
        EXPECT_EQ(l.find('\n'), std::string::npos) << "a raw newline in a log line: " << l;
        if (l.find("bootstrap refused from aeron:udp?endpoint=h:1\\x0aauth:\\x20login") != std::string::npos) sawLine = true;
    }
    EXPECT_TRUE(sawLine) << "the channel is logged escaped";
}
