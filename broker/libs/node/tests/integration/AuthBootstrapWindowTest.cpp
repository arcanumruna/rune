// AuthBootstrapWindowTest.cpp — authentication Task 10 (controller ruling, D4 / D11 / D13): how a session of a fresh,
// authenticating cluster sends SecurityBootstrap.  While NO ENABLED user holds ADMIN ON CLUSTER (a fresh cluster) a
// session that connects (it can send no credentials) is told authState = Anonymous and may send ONLY SecurityBootstrap,
// whose cluster-key MAC is verified.  Every other request is answered AuthenticationFailed and the session ends.  Once
// an admin exists the window is closed for every session; it REOPENS (D3 recovery, review C I2 ruling) if the last
// enabled admin is disabled or dropped - still gated by the cluster-key MAC.  One real BrokerNode, RUNE_AUTH enabled.
#include "support/ScramTestClient.h"
#include "support/SecurityBrokerCluster.h"
#include "support/SecurityEventChain.h"

#include <rune/crypto/Kdf.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>

#include <fstream>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
using namespace rune::node::test::security_cluster;
namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

namespace {

const std::string kKeyFile = std::string(48, 'K') + "\n";
const std::string kOtherKey = std::string(48, 'Z') + "\n";

struct Verifier {
    std::vector<uint8_t> salt;
    std::array<uint8_t, 64> stored{}, server{};
    pc::VerifierWire view() const {
        return pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(salt.data()), salt.size()), 4096,
                                std::string_view(reinterpret_cast<const char*>(stored.data()), 64),
                                std::string_view(reinterpret_cast<const char*>(server.data()), 64)};
    }
};

/// A node with authentication ENABLED and a cluster key file; waits until it leads `__security`.
struct AuthNode {
    TmpDir dir{"authwin"};
    std::filesystem::path keyFile;
    Ports ports = Ports::discover(1);
    std::unique_ptr<BrokerNode> node;
    std::unique_ptr<TmpDir> clientDir{std::make_unique<TmpDir>("authwin_client")};

    explicit AuthNode(std::function<void(NodeConfig&)> tweak = {}) {
        keyFile = dir.path.string() + ".key";
        { std::ofstream f(keyFile, std::ios::binary); f << kKeyFile; }
        std::filesystem::permissions(keyFile, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
        NodeConfig cfg = makeConfig(1, dir.path, ports, 1, Mode::Shard);
        cfg.authMode = AuthMode::Enabled;
        cfg.clientTransport = ClientTransport::PlaintextForTest;   // issue #10: plaintext client sessions, test only
        cfg.clusterKeyFile = keyFile.string();
        if (tweak) tweak(cfg);
        node = std::make_unique<BrokerNode>(cfg);
        node->start();
        EXPECT_TRUE(node->awaitLeader(10'000ms));
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        while (securityOf(*node).role != rune::replication::Role::Leader && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(20ms);
    }
    ~AuthNode() { node->stop(); std::error_code ec; std::filesystem::remove(keyFile, ec); }
    [[nodiscard]] std::string channel() const {
        return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[0]);
    }
};

int64_t connect(ShardTestClient& cl, const std::string& ch) {
    for (const auto dl = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < dl;) {
        const int64_t s = cl.connect(ch, kClientStream);
        if (s > 0) return s;
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

/// A SecurityBootstrap frame with a MAC under `keyFileContents` (or a garbage MAC when empty).
std::vector<uint8_t> bootstrapFrame(int64_t sid, int64_t corr, const std::string& keyFileContents, uint8_t nonceSeed) {
    Verifier v;
    v.salt.assign(16, 0x11);
    v.stored.fill(0x22);
    v.server.fill(0x33);
    pc::SecurityBootstrap m;
    m.sessionId = sid;
    m.correlationId = corr;
    m.adminName = "admin";
    m.verifier = v.view();
    for (std::size_t i = 0; i < m.fakeSaltSeed.size(); ++i) m.fakeSaltSeed[i] = static_cast<uint8_t>(0x40 + i);
    for (std::size_t i = 0; i < m.nonce.size(); ++i) m.nonce[i] = static_cast<uint8_t>(nonceSeed + i);
    if (keyFileContents.empty()) {
        m.mac.fill(0x99);
    } else {
        const auto key = rune::crypto::deriveClusterKey(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(keyFileContents.data()), keyFileContents.size()));
        std::vector<uint8_t> input;
        pc::bootstrapMacInput(m, input);
        rune::crypto::DigestBytes mac;
        EXPECT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, key->master, input, mac));
        std::memcpy(m.mac.data(), mac.bytes.data(), m.mac.size());
    }
    std::vector<uint8_t> buf(m.encodedSize());
    buf.resize(m.encode(buf.data()));
    return buf;
}

std::vector<uint8_t> createRoleFrame(int64_t sid, int64_t corr) {
    pc::SecurityDdl m;
    m.sessionId = sid; m.correlationId = corr;
    m.ext.clientName = "t"; m.ext.requestId = {1, static_cast<uint64_t>(corr)};
    m.kind = pc::SecurityOpKind::CreateRole; m.name = "r";
    std::vector<uint8_t> buf(m.encodedSize());
    buf.resize(m.encode(buf.data()));
    return buf;
}

}  // namespace

TEST(AuthBootstrapWindowRegression, ConnectInTheBootstrapWindowIsToldAnonymousAndAfterBootstrapMustAuthenticate) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    const int64_t s1 = connect(cl, n.channel());
    ASSERT_GT(s1, 0);
    ASSERT_TRUE(cl.lastConnectResponse().has_value());
    EXPECT_EQ(cl.lastConnectResponse()->authMode, pc::AuthMode::Required);
    EXPECT_EQ(cl.lastConnectResponse()->authState, pc::AuthState::Anonymous) << "no Bootstrap event yet: the window";

    // Bootstrap with the right proof.
    const int64_t corr = cl.nextCorrelationId();
    const auto f = bootstrapFrame(s1, corr, kKeyFile, 1);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
    const auto ok = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, 0) << ok->detail;
    EXPECT_TRUE(n.node->securityViewForTest().hasSecurityAdmin) << "answered OK only once THIS broker enforces it";

    // The window is closed for EVERY session: a new connect must authenticate ...
    const int64_t s2 = connect(cl, n.channel());
    ASSERT_GT(s2, 0);
    EXPECT_EQ(cl.lastConnectResponse()->authState, pc::AuthState::MustAuthenticate)
        << "while an enabled admin exists an anonymous session is not offered the window";
    // ... and the session that was offered the window can send nothing more (a second bootstrap included).
    const int64_t corr2 = cl.nextCorrelationId();
    const auto again = bootstrapFrame(s1, corr2, kKeyFile, 9);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, again.data(), again.size()));
    const auto refused = cl.awaitDdl(corr2, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed))
        << "refused by the gate (window closed), not by the admin thread's 'already bootstrapped'";
}

TEST(AuthBootstrapWindowRegression, AnAnonymousSessionSendingAnythingButBootstrapIsRefusedAndEnded) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    const int64_t sid = connect(cl, n.channel());
    ASSERT_GT(sid, 0);
    const int64_t corr = cl.nextCorrelationId();
    const auto f = createRoleFrame(sid, corr);   // a security DDL is NOT a bootstrap
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
    const auto r = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << "in the request's own response type";
    // The session ended (D13): the broker no longer knows it.
    EXPECT_FALSE(n.node->gatewayHasSessionForTest(sid)) << "the session was ended";
    EXPECT_EQ(n.node->authGateRefusalsForTest(), 1u);
    EXPECT_EQ(n.node->securityViewForTest().appliedOffset, -1) << "nothing reached __security";
}

TEST(AuthBootstrapWindowRegression, ABootstrapWithABadMacIsRefusedAndAppendsNothing) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    const int64_t sid = connect(cl, n.channel());
    ASSERT_GT(sid, 0);
    for (const std::string& key : {std::string{}, kOtherKey}) {   // garbage MAC; a MAC under another cluster's key
        const int64_t corr = cl.nextCorrelationId();
        const auto f = bootstrapFrame(sid, corr, key, 3);
        ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
        const auto r = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
        EXPECT_EQ(r->detail, "bootstrap proof does not match this cluster's key");
    }
    EXPECT_EQ(n.node->securityViewForTest().appliedOffset, -1);
    EXPECT_FALSE(n.node->securityViewForTest().hasSecurityAdmin);
    // The session is still in the window: a correct proof now works.
    const int64_t corr = cl.nextCorrelationId();
    const auto f = bootstrapFrame(sid, corr, kKeyFile, 4);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
    const auto ok = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, 0) << ok->detail;
}

// Review C I2 (ruling: D3 recovery stays): disabling the LAST admin reopens the window (the cluster-key holder can
// re-bootstrap a locked-out cluster); a bad MAC is still refused.
TEST(AuthBootstrapWindowRegression, TheWindowReopensWhenTheLastAdminIsDisabledStillMacGatedRegression) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    int64_t s1 = connect(cl, n.channel());
    ASSERT_GT(s1, 0);
    const int64_t corr = cl.nextCorrelationId();
    const auto f = bootstrapFrame(s1, corr, kKeyFile, 1);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
    ASSERT_EQ(cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s)->errorCode, 0);
    const int64_t s2 = connect(cl, n.channel());
    ASSERT_GT(s2, 0);
    ASSERT_EQ(cl.lastConnectResponse()->authState, pc::AuthState::MustAuthenticate) << "closed while an admin is enabled";
    // Append `ALTER USER admin DISABLE` (the same bootstrap history rebuilt with the library, as the leader builds it).
    rune::node::test::SecurityEventChain chain;
    rune::security::BootstrapOps b;
    b.verifier.salt.assign(16, 0x11);
    b.verifier.iterations = 4096;
    b.verifier.storedKey.fill(0x22);
    b.verifier.serverKey.fill(0x33);
    for (std::size_t i = 0; i < b.fakeSaltSeed.size(); ++i) b.fakeSaltSeed[i] = static_cast<uint8_t>(0x40 + i);
    (void)chain.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    const auto disable = chain.emit(rune::security::AlterUserOps{rune::security::DisableUser{}}, "admin", "ALTER USER admin DISABLE;");
    auto bytes = rune::security::encode(disable);
    ASSERT_TRUE(bytes.has_value());
    ASSERT_GE(n.node->appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(*bytes)), 0);
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (n.node->securityViewForTest().hasSecurityAdmin && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(20ms);
    ASSERT_FALSE(n.node->securityViewForTest().hasSecurityAdmin);
    const int64_t s3 = connect(cl, n.channel());
    ASSERT_GT(s3, 0);
    // Issue #121: the window is open again, but users exist - a connect is told MustAuthenticate (they log in), and
    // SecurityBootstrap stays allowed before the login while the window is open.
    EXPECT_EQ(cl.lastConnectResponse()->authState, pc::AuthState::MustAuthenticate)
        << "a reopened window still asks for a login: the other users exist";
    // Still MAC-gated: a bad MAC is refused, the right one accepted.
    const int64_t c2 = cl.nextCorrelationId();
    const auto bad = bootstrapFrame(s3, c2, kOtherKey, 5);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, bad.data(), bad.size()));
    EXPECT_EQ(cl.awaitDdl(c2, std::chrono::steady_clock::now() + 20s)->errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
}

namespace {
/// Bootstrap `admin` on `n`, then append `CREATE USER ws1` (a real verifier of kTestPasswordA) and `ALTER USER admin
/// DISABLE` (the bootstrap history rebuilt with the library, as the leader builds it): the window is reopened while a
/// credentialed non-admin user exists.
void reopenTheWindowWithAUser(AuthNode& n, ShardTestClient& cl) {
    const int64_t s1 = connect(cl, n.channel());
    ASSERT_GT(s1, 0);
    const int64_t corr = cl.nextCorrelationId();
    const auto f = bootstrapFrame(s1, corr, kKeyFile, 1);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, f.data(), f.size()));
    ASSERT_EQ(cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s)->errorCode, 0);
    rune::node::test::SecurityEventChain chain;
    rune::security::BootstrapOps b;
    b.verifier.salt.assign(16, 0x11);
    b.verifier.iterations = 4096;
    b.verifier.storedKey.fill(0x22);
    b.verifier.serverKey.fill(0x33);
    for (std::size_t i = 0; i < b.fakeSaltSeed.size(); ++i) b.fakeSaltSeed[i] = static_cast<uint8_t>(0x40 + i);
    (void)chain.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    for (const auto& e : {chain.emit(rune::security::CreateUserOps{scramVerifierOf(kTestPasswordA, 1)}, "ws1",
                                     "CREATE USER ws1 WITH PASSWORD '<redacted>';"),
                          chain.emit(rune::security::AlterUserOps{rune::security::DisableUser{}}, "admin",
                                     "ALTER USER admin DISABLE;")}) {
        auto bytes = rune::security::encode(e);
        ASSERT_TRUE(bytes.has_value());
        ASSERT_GE(n.node->appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(*bytes)), 0);
    }
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (n.node->securityViewForTest().hasSecurityAdmin && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(20ms);
    ASSERT_FALSE(n.node->securityViewForTest().hasSecurityAdmin) << "the window is reopened";
}

/// The SCRAM legs as raw frames on `cl` (the real client's shape: one password, every slot); nullopt = not answered.
std::optional<pc::AuthServerFinal> scramLogin(ShardTestClient& cl, const AuthNode& n, int64_t sid, const std::string& user,
                                              const std::string& pw) {
    pc::AuthClientFirst cf;
    const std::string nonce = rune::crypto::scram::makeNonce();
    cf.sessionId = sid; cf.correlationId = cl.nextCorrelationId(); cf.username = user; cf.clientNonce = nonce;
    std::vector<uint8_t> fb(cf.encodedSize());
    fb.resize(cf.encode(fb.data()));
    const auto dl = std::chrono::steady_clock::now() + 20s;
    if (!cl.send(n.channel(), kClientStream, fb.data(), fb.size())) return std::nullopt;
    const auto first = cl.awaitAuthFrame(cf.correlationId, dl);
    if (!first || first->templateId != static_cast<uint16_t>(pc::TemplateId::AuthServerFirst)) return std::nullopt;
    const auto sf = pc::AuthServerFirst::decode(first->bytes.data(), first->bytes.size());
    if (!sf) return std::nullopt;
    const auto ans = scramAnswer(user, nonce, *sf, {pw, pw}, sid, cf.correlationId);
    if (!ans) return std::nullopt;
    std::vector<uint8_t> nb(ans->final.encodedSize());
    nb.resize(ans->final.encode(nb.data()));
    if (!cl.send(n.channel(), kClientStream, nb.data(), nb.size())) return std::nullopt;
    const auto fin = cl.awaitAuthFinal(cf.correlationId, dl);
    if (!fin) return std::nullopt;
    return pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size());
}
}  // namespace

// #136: while the window is reopened (the last admin disabled), an existing NON-admin user still logs in with its
// password - the reopened window only adds SecurityBootstrap, it never takes the login away (issue #121).
TEST(AuthBootstrapWindowRegression, ACredentialedUserLogsInWhileTheWindowIsReopenedRegression) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    reopenTheWindowWithAUser(n, cl);
    if (HasFatalFailure()) return;
    const int64_t sid = connect(cl, n.channel());
    ASSERT_GT(sid, 0);
    EXPECT_EQ(cl.lastConnectResponse()->authState, pc::AuthState::MustAuthenticate);
    const auto ok = scramLogin(cl, n, sid, "ws1", kTestPasswordA);
    ASSERT_TRUE(ok.has_value()) << "the login was not answered";
    EXPECT_EQ(ok->errorCode, 0) << "the existing user logs in while the window is reopened";
    EXPECT_EQ(ok->serverSignature.size(), 64u);
    EXPECT_TRUE(n.node->gatewayHasSessionForTest(sid)) << "and its session stays";
    const int64_t sid2 = connect(cl, n.channel());
    ASSERT_GT(sid2, 0);
    const auto bad = scramLogin(cl, n, sid2, "ws1", "not the password");
    ASSERT_TRUE(bad.has_value());
    EXPECT_EQ(bad->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << "still a real check";
}

// Adversarial review of PR #583 (#136, escalation through the reopened window): the reopened window gives a session
// nothing but SecurityBootstrap.  A non-admin user logged in while it is open is refused a security DDL (45, its session
// kept), and a session that never logged in is refused it as unauthenticated (44) and ended - nothing is applied.
TEST(AuthBootstrapWindowAdversarialRegression, TheReopenedWindowGrantsNoAdminDdlAdversarialRegression) {
    AuthNode n;
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    reopenTheWindowWithAUser(n, cl);
    if (HasFatalFailure()) return;
    const int64_t applied = n.node->securityViewForTest().appliedOffset;
    const int64_t user = connect(cl, n.channel());
    ASSERT_GT(user, 0);
    const auto ok = scramLogin(cl, n, user, "ws1", kTestPasswordA);
    ASSERT_TRUE(ok.has_value());
    ASSERT_EQ(ok->errorCode, 0);
    const int64_t c1 = cl.nextCorrelationId();
    const auto ddl = createRoleFrame(user, c1);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, ddl.data(), ddl.size()));
    const auto refused = cl.awaitDdl(c1, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(pc::ErrorCode::NotAuthorized)) << refused->detail;
    EXPECT_TRUE(n.node->gatewayHasSessionForTest(user)) << "a refusal, not an end";

    const int64_t anon = connect(cl, n.channel());
    ASSERT_GT(anon, 0);
    const int64_t c2 = cl.nextCorrelationId();
    const auto ddl2 = createRoleFrame(anon, c2);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, ddl2.data(), ddl2.size()));
    const auto gated = cl.awaitDdl(c2, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(gated.has_value());
    EXPECT_EQ(gated->errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed)) << gated->detail;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (n.node->gatewayHasSessionForTest(anon) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(5ms);
    EXPECT_FALSE(n.node->gatewayHasSessionForTest(anon)) << "the unauthenticated session was ended";
    EXPECT_EQ(n.node->securityViewForTest().appliedOffset, applied) << "nothing was applied";
}

// Review C I4: the cluster key is loaded ONCE (start()); replacing the file afterwards (a Kubernetes secret update)
// changes nothing - the validated key is the key in use.
TEST(NodeConfigRegression, TheValidatedClusterKeyIsTheOneInUseRegression) {
    AuthNode n{[](NodeConfig& c) {
        // rotated on disk between the validation and the gateway being built (the old code read the file again there)
        c.testAfterAuthValidation = [path = c.clusterKeyFile] { std::ofstream f(path, std::ios::binary | std::ios::trunc); f << kOtherKey; };
    }};
    ShardTestClient cl(n.clientDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    const int64_t sid = connect(cl, n.channel());
    ASSERT_GT(sid, 0);
    const int64_t c1 = cl.nextCorrelationId();
    const auto underNew = bootstrapFrame(sid, c1, kOtherKey, 1);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, underNew.data(), underNew.size()));
    const auto refused = cl.awaitDdl(c1, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest)) << "the file's NEW content is not the key in use";
    EXPECT_EQ(refused->detail, "bootstrap proof does not match this cluster's key") << "and never the silent 'authentication is not enabled'";
    const int64_t c2 = cl.nextCorrelationId();
    const auto underStarted = bootstrapFrame(sid, c2, kKeyFile, 2);
    ASSERT_TRUE(cl.send(n.channel(), kClientStream, underStarted.data(), underStarted.size()));
    const auto ok = cl.awaitDdl(c2, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, 0) << ok->detail;
}
