// AuthBootstrapWindowTest.cpp — authentication Task 10 (controller ruling, D4 / D11 / D13): how a session of a fresh,
// authenticating cluster sends SecurityBootstrap.  While `__security` holds no Bootstrap event a session that connects
// (it can send no credentials) is told authState = Anonymous and may send ONLY SecurityBootstrap, whose cluster-key
// MAC is verified.  Every other request is answered AuthenticationFailed and the session ends.  Once a Bootstrap event
// is applied no session is ever offered the window again.  One real BrokerNode, RUNE_AUTH enabled, real Aeron.
#include "support/SecurityBrokerCluster.h"

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

    AuthNode() {
        keyFile = dir.path.string() + ".key";
        { std::ofstream f(keyFile, std::ios::binary); f << kKeyFile; }
        std::filesystem::permissions(keyFile, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
        NodeConfig cfg = makeConfig(1, dir.path, ports, 1, Mode::Shard);
        cfg.authMode = AuthMode::Enabled;
        cfg.clusterKeyFile = keyFile.string();
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
        << "once a Bootstrap event is applied an anonymous session is never offered again";
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
