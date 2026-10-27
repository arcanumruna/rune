// NamespaceLoginTest.cpp — namespaces Task 9 (spec §2, §11; D12) against ONE real BrokerNode (authentication ENABLED,
// real Aeron UDP, raw frames through AuthWireClient): the session's namespace comes from the LOGIN.
//   * `ns/user` logs into namespace `ns`; a bare `user` (and `default/user`) into `default` (nsId 0).
//   * An unknown or malformed namespace is an unknown user: the same two fake salts, the same iterations, the same
//     final 44 byte for byte - the first leg never says which part of the name was wrong.
//   * The same local name in two namespaces is two users with two passwords.
//   * Anonymous read (RUNE_ANONYMOUS_READ, OFF by default): off = a request before login is 44 and the session ends;
//     on = the session is `default` with Describe only (metadata / schema), anything else 45 and the session stays.
// The session's namespace is observed through ListNamespaces (82): a session that is not a cluster admin sees only its
// own namespace (D15), so the one entry it gets back IS the session's nsId.
//
// Interaction matrix (login name × namespace state × password):
//   | login name      | namespace         | password        | expected                                         | test
//   |-----------------|-------------------|-----------------|--------------------------------------------------|------
//   | a/alice         | a (1) exists      | a's             | ok, session ns 1                                 | Qualified
//   | a/alice         | a (1) exists      | default alice's | 44 (a different user)                            | SameNameDifferentNs
//   | alice           | default           | default's       | ok, session ns 0                                 | BareLoginIsDefault / SameName…
//   | alice           | default           | a's             | 44                                               | SameNameDifferentNs
//   | default/alice   | default           | default's       | ok, session ns 0 (= bare)                        | DefaultQualifiedEqualsBare
//   | b/alice         | b (2), no alice   | a's             | 44, unknown-user shape                           | SameNameDifferentNs
//   | nosuch/alice    | none              | a's             | 44, unknown-user shape                           | UnknownNsLikeWrongPassword
//   | a/nosuch        | a exists          | any             | 44, unknown-user shape                           | UnknownNsLikeWrongPassword
//   | a//x            | malformed         | any             | 44, unknown-user shape                           | UnknownNsLikeWrongPassword
//   | a/alice         | a exists          | wrong           | 44 (the reference shape)                         | UnknownNsLikeWrongPassword
//   | (no login)      | anon read off     | -               | 44, session ends                                 | AnonymousOffByDefault
//   | (no login)      | anon read on      | -               | metadata / schema of `default` ok; produce and   | AnonymousOnIsDefaultNs
//   |                 |                   |                 | ListNamespaces 45, session stays                 |
#include "support/AuthRig.h"

#include <rune/protocol/client/NamespaceMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kPwA       = "alice in namespace a";
constexpr const char* kPwDefault = "alice in default here";
constexpr uint16_t    kAuthFailed = static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed);

bool sameBodyDifferentHeader(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
    return a.size() == b.size() && a.size() > h && std::equal(a.begin() + h, a.end(), b.begin() + h);
}

/// A broker with a cluster admin logged in at client version 14 (namespace DDL needs it).  A session lives on the
/// client that opened it (AuthBroker::wire() is a FRESH client each call): `aw` carries the admin's session, `uw` every
/// user session a test opens (and their ListNamespaces).
struct NsBroker {
    AuthBroker      b;
    AuthWireClient* aw = nullptr;
    AuthWireClient* uw = nullptr;
    int64_t         admSid = 0;
    uint64_t        ridLo = 0;
    explicit NsBroker(const std::string& tag, AuthBroker::Options o = {}) : b(tag, std::move(o)) {
        b.bootstrapAdmin();
        aw = &b.wire();
        const auto l = aw->connectAndLogin(b.channel(), "admin", AuthBroker::kAdminPassword, pc::kClientVersionNamespaces);
        if (!l.ok()) throw std::logic_error("NsBroker: the v14 admin login failed");
        admSid = l.sessionId;
        uw = &b.wire();
    }
    /// CREATE NAMESPACE `name` as the cluster admin; returns its nsId.
    uint32_t createNs(const std::string& name) {
        AuthWireClient& w = *aw;
        pc::NamespaceDdlRequest m;
        m.sessionId = admSid;
        m.correlationId = w.raw().nextCorrelationId();
        m.ext.clientName = "ns-login-test";
        m.ext.requestId = rune::protocol::RequestId{0xA11CE, ++ridLo};
        m.kind = pc::NamespaceDdlKind::Create;
        m.name = name;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        if (!w.raw().send(b.channel(), AuthWireClient::kClientStream, buf.data(), buf.size()))
            throw std::logic_error("NsBroker: send CREATE NAMESPACE");
        const auto any = w.raw().awaitAnyDdlAnswer(m.correlationId, std::chrono::steady_clock::now() + 20s);
        const auto a = w.raw().awaitDdl(m.correlationId, std::chrono::steady_clock::now() + 1s);
        if (!a || a->errorCode != 0)
            throw std::logic_error("NsBroker: CREATE NAMESPACE " + name + " failed: " +
                                   (any ? std::to_string(any->responseTemplate) + "/" + std::to_string(any->code) + " " +
                                              any->message
                                        : std::string("no answer")));
        return static_cast<uint32_t>(a->objectId);
    }
};

/// ListNamespaces on `sid`: what the session may see (only its own namespace unless a cluster admin).
std::optional<pc::ListNamespacesResponse> listNs(NsBroker& nb, int64_t sid) {
    AuthWireClient& w = *nb.uw;
    w.raw().captureRawAnswers(true);
    pc::ListNamespaces m;
    m.sessionId = sid;
    m.correlationId = w.raw().nextCorrelationId();
    m.afterNsId = 0;
    m.max = 100;
    std::vector<uint8_t> buf(m.encodedSize());
    buf.resize(m.encode(buf.data()));
    if (!w.raw().send(nb.b.channel(), AuthWireClient::kClientStream, buf.data(), buf.size())) return std::nullopt;
    const auto raw = w.raw().awaitRaw(m.correlationId, std::chrono::steady_clock::now() + 20s);
    if (!raw) return std::nullopt;
    return pc::ListNamespacesResponse::decode(raw->data(), raw->size());
}

/// The session's namespace as the broker sees it: the single entry its ListNamespaces returns.
std::optional<std::pair<uint32_t, std::string>> sessionNs(NsBroker& nb, int64_t sid) {
    const auto r = listNs(nb, sid);
    if (!r || r->namespaces.size() != 1) return std::nullopt;
    return std::pair<uint32_t, std::string>{r->namespaces[0].nsId, r->namespaces[0].name};
}

AuthWireClient::Login loginV14(NsBroker& nb, const std::string& user, const std::string& pw) {
    return nb.uw->connectAndLogin(nb.b.channel(), user, pw, pc::kClientVersionNamespaces);
}

}  // namespace

TEST(NamespaceLoginRegression, QualifiedLoginRegression) {
    NsBroker nb("nsq");
    ASSERT_EQ(nb.createNs("a"), 1u);
    nb.b.createUser("a/alice", kPwA);
    const auto l = loginV14(nb, "a/alice", kPwA);
    ASSERT_TRUE(l.ok()) << "a/alice logs in with a's password";
    EXPECT_TRUE(l.serverSignatureValid());
    const auto ns = sessionNs(nb, l.sessionId);
    ASSERT_TRUE(ns.has_value()) << "a non-admin session sees exactly one namespace: its own";
    EXPECT_EQ(ns->first, 1u) << "the session's namespace is the login's";
    EXPECT_EQ(ns->second, "a");
}

TEST(NamespaceLoginRegression, BareLoginIsDefaultRegression) {
    NsBroker nb("nsbare");
    ASSERT_EQ(nb.createNs("a"), 1u);
    nb.b.createUser("alice", kPwDefault);
    const auto l = loginV14(nb, "alice", kPwDefault);
    ASSERT_TRUE(l.ok());
    const auto ns = sessionNs(nb, l.sessionId);
    ASSERT_TRUE(ns.has_value());
    EXPECT_EQ(ns->first, 0u) << "a bare name is namespace `default` (D12)";
    EXPECT_EQ(ns->second, "default");
}

TEST(NamespaceLoginRegression, DefaultQualifiedEqualsBareRegression) {
    NsBroker nb("nsdefq");
    ASSERT_EQ(nb.createNs("a"), 1u);
    nb.b.createUser("alice", kPwDefault);
    const auto q = loginV14(nb, "default/alice", kPwDefault);
    ASSERT_TRUE(q.ok()) << "default/alice is the bare alice";
    EXPECT_TRUE(q.serverSignatureValid());
    const auto ns = sessionNs(nb, q.sessionId);
    ASSERT_TRUE(ns.has_value());
    EXPECT_EQ(ns->first, 0u);
    // The same user both ways: the bare login offers the very same salts.
    const auto bare = loginV14(nb, "alice", kPwDefault);
    ASSERT_TRUE(bare.ok());
    ASSERT_TRUE(q.first && bare.first);
    EXPECT_EQ(q.first->salts[0].salt, bare.first->salts[0].salt) << "one user, one verifier";
}

// Review Focus: the first leg never reveals whether the namespace, the user or the password was wrong.
TEST(NamespaceLoginRegression, UnknownNsLikeWrongPasswordRegression) {
    NsBroker nb("nsunk");
    ASSERT_EQ(nb.createNs("a"), 1u);
    nb.b.createUser("a/alice", kPwA);
    AuthWireClient& w = *nb.uw;

    struct Case { const char* what; std::string user; std::string pw; };
    // The reference first (a real user, the wrong password); every name a distinct throttle key, 4 failures per host.
    const std::vector<Case> cases = {{"wrong password", "a/alice", "not the password"},
                                     {"unknown namespace", "nosuch/alice", kPwA},
                                     {"unknown user in a known namespace", "a/nosuch", kPwA},
                                     {"malformed name", "a//x", kPwA}};
    std::vector<AuthWireClient::Login> out;
    for (const Case& c : cases) {
        const int64_t sid = w.connect(nb.b.channel(), pc::kClientVersionNamespaces);
        ASSERT_GT(sid, 0) << c.what;
        out.push_back(w.login(nb.b.channel(), sid, c.user, {c.pw, c.pw}));
        const auto& l = out.back();
        const auto& ref = out[0];
        ASSERT_TRUE(l.first.has_value()) << c.what << ": answered with salts, never refused before them";
        EXPECT_EQ(l.first->count, 2) << c.what;
        for (int i = 0; i < 2; ++i) {
            EXPECT_EQ(l.first->salts[i].salt.size(), 16u) << c.what << " slot " << i;
            EXPECT_EQ(l.first->salts[i].iterations, ref.first->salts[i].iterations) << c.what << " slot " << i;
        }
        EXPECT_EQ(l.first->combinedNonce.size(), ref.first->combinedNonce.size()) << c.what;
        EXPECT_EQ(l.firstBytes.size(), ref.firstBytes.size()) << c.what << ": one frame shape";
        ASSERT_TRUE(l.fin.has_value()) << c.what;
        EXPECT_EQ(l.fin->errorCode, kAuthFailed) << c.what;
        EXPECT_EQ(l.fin->flags, ref.fin->flags) << c.what;
        EXPECT_EQ(l.fin->retryAfterMs, 0u) << c.what;
        EXPECT_EQ(l.fin->detail, "authentication failed") << c.what;
        EXPECT_TRUE(l.fin->serverSignature.empty()) << c.what;
        EXPECT_TRUE(sameBodyDifferentHeader(l.finalBytes, ref.finalBytes))
            << c.what << ": the AuthServerFinal is byte-identical except the header ids";
    }
    // The control: the right password still logs in (nothing above locked the account).
    const auto ok = loginV14(nb, "a/alice", kPwA);
    EXPECT_TRUE(ok.ok());
    EXPECT_EQ(ok.firstBytes.size(), out[0].firstBytes.size()) << "a real user's first leg has the same shape too";
}

TEST(NamespaceLoginRegression, SameNameDifferentNsDifferentPasswordRegression) {
    NsBroker nb("nssame");
    ASSERT_EQ(nb.createNs("a"), 1u);
    ASSERT_EQ(nb.createNs("b"), 2u);
    nb.b.createUser("a/alice", kPwA);
    nb.b.createUser("alice", kPwDefault);

    const auto inA = loginV14(nb, "a/alice", kPwA);
    ASSERT_TRUE(inA.ok());
    const auto nsA = sessionNs(nb, inA.sessionId);
    ASSERT_TRUE(nsA.has_value());
    EXPECT_EQ(nsA->first, 1u);

    const auto inDefault = loginV14(nb, "alice", kPwDefault);
    ASSERT_TRUE(inDefault.ok());
    const auto nsD = sessionNs(nb, inDefault.sessionId);
    ASSERT_TRUE(nsD.has_value());
    EXPECT_EQ(nsD->first, 0u);
    ASSERT_TRUE(inA.first && inDefault.first);
    EXPECT_NE(inA.first->salts[0].salt, inDefault.first->salts[0].salt) << "two users, two verifiers";

    // Crossed passwords fail: each alice has its own.
    EXPECT_EQ(loginV14(nb, "a/alice", kPwDefault).errorCode(), kAuthFailed) << "default alice's password in a";
    EXPECT_EQ(loginV14(nb, "alice", kPwA).errorCode(), kAuthFailed) << "a's alice's password in default";
    // b has no alice at all.
    EXPECT_EQ(loginV14(nb, "b/alice", kPwA).errorCode(), kAuthFailed) << "no alice in b";
}

TEST(NamespaceLoginRegression, AnonymousOffByDefaultRegression) {
    NsBroker nb("nsanonoff");   // the DEFAULT config: RUNE_ANONYMOUS_READ unset
    AuthWireClient& w = nb.b.wire();
    const int64_t sid = w.connect(nb.b.channel(), pc::kClientVersionNamespaces);
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(w.lastConnect().has_value());
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate);
    const int64_t corr = w.raw().nextCorrelationId();
    ASSERT_TRUE(w.raw().produce(nb.b.channel(), AuthWireClient::kClientStream, sid, corr, /*topicId=*/1, 0, 1, makeBatch(1)));
    const auto r = w.raw().awaitProduceResponse(corr);
    ASSERT_TRUE(r.has_value()) << "answered, never hung";
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
    const auto ping = w.raw().pingError(nb.b.channel(), AuthWireClient::kClientStream, sid,
                                        std::chrono::steady_clock::now() + 1s);
    EXPECT_FALSE(ping.has_value()) << "the session ended (D13)";
    EXPECT_FALSE(nb.b.node().gatewayHasSessionForTest(sid));

    // A first request that goes through the auth gate (not the Produce fast path): also 44 and the end - never the
    // anonymous reader (mutation: an anonymous conversion that ignores RUNE_ANONYMOUS_READ turns this red).
    const int64_t sid2 = w.connect(nb.b.channel(), pc::kClientVersionNamespaces);
    ASSERT_GT(sid2, 0);
    const int64_t sc = w.raw().sendGetTopicSchema(nb.b.channel(), AuthWireClient::kClientStream, sid2, "orders");
    const auto schema = w.raw().awaitSchema(sc, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(schema.has_value()) << "answered, never hung";
    EXPECT_EQ(schema->errorCode, static_cast<int16_t>(pc::ErrorCode::AuthenticationFailed));
    EXPECT_FALSE(nb.b.node().gatewayHasSessionForTest(sid2)) << "the session ended (D13)";
}

TEST(NamespaceLoginRegression, AnonymousOnIsDefaultNsRegression) {
    AuthBroker::Options o;
    o.configure = [](NodeConfig& c) { c.anonymousRead = true; };
    NsBroker nb("nsanonon", std::move(o));
    AuthWireClient& w = *nb.uw;
    // A topic in `default`, defined by a builder (CREATE on the cluster; the admin's ADMIN bit is security DDL only).
    nb.b.createUser("builder", kPwDefault, {grants::cluster(grants::SO::Create)});
    const auto bl = loginV14(nb, "builder", kPwDefault);
    ASSERT_TRUE(bl.ok());
    const int64_t dc = w.raw().sendDefineTopic(nb.b.channel(), AuthWireClient::kClientStream, bl.sessionId, "orders", 1,
                                               authRigCharBlob(), 0, {}, 0, authRigTopicConfig());
    const auto def = w.raw().awaitDefine(dc, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(def.has_value());
    ASSERT_EQ(def->errorCode, 0);
    const int32_t topicId = def->topicId;

    const int64_t sid = w.connect(nb.b.channel(), pc::kClientVersionNamespaces);
    ASSERT_GT(sid, 0);
    // Describe on `default`: metadata lists the topic, its schema resolves.
    const auto md = w.raw().metadata(nb.b.channel(), AuthWireClient::kClientStream, sid);
    EXPECT_TRUE(std::any_of(md.begin(), md.end(), [&](const auto& p) { return p.topicId == topicId; }))
        << "an anonymous session sees `default`'s topics";
    const int64_t sc = w.raw().sendGetTopicSchema(nb.b.channel(), AuthWireClient::kClientStream, sid, "orders");
    const auto schema = w.raw().awaitSchema(sc, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(schema.has_value());
    EXPECT_EQ(schema->errorCode, 0);
    EXPECT_EQ(schema->topicId, topicId);

    // Anything else: NotAuthorized, and the session stays.
    const int64_t pcorr = w.raw().nextCorrelationId();
    ASSERT_TRUE(w.raw().produce(nb.b.channel(), AuthWireClient::kClientStream, sid, pcorr, topicId, 0, 1, makeBatch(1)));
    const auto pr = w.raw().awaitProduceResponse(pcorr);
    ASSERT_TRUE(pr.has_value());
    EXPECT_EQ(pr->errorCode, static_cast<int16_t>(kNotAuthorized)) << "anonymous read never writes";
    EXPECT_TRUE(nb.b.node().gatewayHasSessionForTest(sid)) << "45, not 44: the session stays";

    w.raw().captureRawAnswers(true);
    pc::ListNamespaces ln;
    ln.sessionId = sid;
    ln.correlationId = w.raw().nextCorrelationId();
    std::vector<uint8_t> buf(ln.encodedSize());
    buf.resize(ln.encode(buf.data()));
    ASSERT_TRUE(w.raw().send(nb.b.channel(), AuthWireClient::kClientStream, buf.data(), buf.size()));
    (void)w.raw().awaitRaw(ln.correlationId, std::chrono::steady_clock::now() + 20s);
    const auto& errs = w.raw().errors();
    const auto it = std::find_if(errs.begin(), errs.end(), [&](const auto& e) { return e.correlationId == ln.correlationId; });
    ASSERT_NE(it, errs.end()) << "ListNamespaces answered by an ErrorResponse";
    EXPECT_EQ(it->code, static_cast<int16_t>(kNotAuthorized));

    // A session whose FIRST request is a Produce (the gate's fast path) becomes the anonymous reader too: 45, it stays.
    const int64_t sid3 = w.connect(nb.b.channel(), pc::kClientVersionNamespaces);
    ASSERT_GT(sid3, 0);
    const int64_t p3 = w.raw().nextCorrelationId();
    ASSERT_TRUE(w.raw().produce(nb.b.channel(), AuthWireClient::kClientStream, sid3, p3, topicId, 0, 1, makeBatch(1)));
    const auto pr3 = w.raw().awaitProduceResponse(p3);
    ASSERT_TRUE(pr3.has_value());
    EXPECT_EQ(pr3->errorCode, static_cast<int16_t>(kNotAuthorized)) << "the first request's kind never changes the outcome";
    EXPECT_TRUE(nb.b.node().gatewayHasSessionForTest(sid3));

    // Still a live session: metadata again.
    const auto md2 = w.raw().metadata(nb.b.channel(), AuthWireClient::kClientStream, sid);
    EXPECT_TRUE(std::any_of(md2.begin(), md2.end(), [&](const auto& p) { return p.topicId == topicId; }));
    EXPECT_TRUE(nb.b.node().gatewayHasSessionForTest(sid));
}
