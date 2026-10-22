// ClientLoginBrokerRegressionTest.cpp - authentication Task 11: the REAL client (rune::client::Client, Thread runner)
// logging in to a REAL authenticating BrokerNode (AuthBroker) over Aeron UDP: the right password, a wrong one, no
// credentials, an old client (the version gate, driven by the real SessionMachine), and the 5 ms login gate.
#include "support/AuthBroker.h"

#include <rune/client/Client.h>
#include <rune/client/session/SessionMachine.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace sm = rune::client::session;
using namespace std::chrono_literals;

namespace {

constexpr const char* kPwA = "correct horse battery";

std::unique_ptr<rc::Client> makeClient(AuthBroker& b, const std::string& user, const std::string& password) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = password;
    c.offerCrypto     = true;                      // issue #14: credentials only over TLS
    c.tlsTrustPem     = AuthBroker::tlsCaPem();   // the rig's test CA (the certificate names 127.0.0.1)
    return std::make_unique<rc::Client>(c);
}

std::string connectError(rc::Client& c) {
    try {
        c.connectAndWait(20s);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return {};
}

struct WireIo final : sm::SessionIo {   // a SessionMachine whose frames really go to the broker
    AuthWireClient& w; const std::string& channel;
    WireIo(AuthWireClient& wc, const std::string& ch) : w(wc), channel(ch) {}
    bool sendFrame(const uint8_t* buf, std::size_t n) override { return w.raw().send(channel, AuthWireClient::kClientStream, buf, n); }
    bool retarget(const std::string&) noexcept override { return true; }
};

}  // namespace

TEST(ClientLoginBrokerRegression, ConnectsWithUsernameAndPassword) {
    AuthBroker b("clientok");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    auto client = makeClient(b, "ws1", kPwA);
    EXPECT_EQ(client->authStatus().kind, rc::AuthStatus::Kind::Anonymous) << "before any login";
    ASSERT_EQ(connectError(*client), "");
    const rc::AuthStatus a = client->authStatus();
    EXPECT_EQ(a.kind, rc::AuthStatus::Kind::Authenticated);
    EXPECT_EQ(a.user, "ws1");
    EXPECT_EQ(a.brokerMode, pc::AuthMode::Required);
    EXPECT_TRUE(b.awaitCounter(kLoginsOkCounterTypeId, 2)) << "the bootstrap admin's login and this one";
    client->closeImmediately();
}

TEST(ClientLoginBrokerRegression, WrongPasswordFailsConnectAuthenticationFailed) {
    AuthBroker b("clientbad");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    auto client = makeClient(b, "ws1", "not the right password");
    const std::string err = connectError(*client);
    EXPECT_NE(err.find("AuthenticationFailed"), std::string::npos) << err;
    EXPECT_NE(err.find("44"), std::string::npos) << err;
    EXPECT_EQ(client->authStatus().kind, rc::AuthStatus::Kind::Anonymous);
    // An unknown user fails with the very same text.
    auto unknown = makeClient(b, "nobody", kPwA);
    EXPECT_EQ(connectError(*unknown), err);
    EXPECT_TRUE(b.awaitCounter(kLoginsFailedCounterTypeId, 2));
}

TEST(ClientLoginBrokerRegression, NoCredentialsFailsAuthenticationFailed) {
    AuthBroker b("clientnone");
    b.bootstrapAdmin();
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    rc::Client client(c);   // no username, no password
    // Without credentials the client never short-circuits: it connects, and the first request is refused by the broker's
    // gate (AuthenticationFailed, in the request's own response type) - never silently served.
    try {
        client.connectAndWait(20s);
        const auto r = client.defineTopicAndWait("t", 1, "", false, {}, 20s);
        EXPECT_EQ(r.errorCode, static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed));
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("AuthenticationFailed"), std::string::npos) << e.what();
    }
    client.closeImmediately();
}

TEST(ClientLoginBrokerRegression, OldClientVersionIsRefusedExplicitly) {
    AuthBroker b("clientold");
    b.bootstrapAdmin();
    AuthWireClient& w = b.wire();
    // The REAL SessionMachine of a v8 client, its ConnectRequest really sent to the broker.
    WireIo io(w, b.channel());
    sm::SessionConfig cfg;
    cfg.clientVersion  = pc::kClientVersionCatalog;   // 8
    cfg.egressChannel  = w.raw().egressChannel();
    cfg.egressStreamId = ShardTestClient::kEgressStream;
    cfg.reconnectForeverAfterLive = true;   // the Client's mode: a non-retriable answer is a terminal Rejected
    sm::SessionMachine m(io, cfg);
    w.raw().forgetConnectResponse();
    m.connect(0);
    const auto resp = w.raw().awaitConnectResponse(std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
    m.onConnectResponse(resp->code, resp->leaderMemberId, resp->assignedSessionId, resp->correlationId, 1);
    EXPECT_EQ(m.state(), sm::SessionState::Rejected) << "terminal: the client never retries what cannot succeed";
    EXPECT_EQ(m.rejectCode(), static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
}

// The plan's login gate (§12): a full SCRAM login <= 5 ms.  The cost is the exchange itself - two round trips and the
// HMACs - because PBKDF2 (4096 iterations) is paid by the CLIENT once per verifier and cached (ScramCache), and the
// server never runs it.  Measured on a connected-transport session: AuthClientFirst sent -> AuthServerFinal received.
TEST(AuthGatesRegression, FullScramLoginMedianUnder5msRegression) {
    AuthBroker b("gate5ms");
    b.bootstrapAdmin();
    b.createUser("ws1", kPwA);
    AuthWireClient& w = b.wire();
    w.cacheSaltedPasswords(true);
    {   // the first login warms the cache (the one PBKDF2) and the link
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        ASSERT_TRUE(w.login(b.channel(), sid, "ws1", {kPwA, kPwA}).ok());
    }
    std::vector<double> ms;
    for (int i = 0; i < 50; ++i) {
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        const auto t0 = std::chrono::steady_clock::now();
        const auto l = w.login(b.channel(), sid, "ws1", {kPwA, kPwA});
        const auto t1 = std::chrono::steady_clock::now();
        ASSERT_TRUE(l.ok()) << i;
        ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    }
    std::sort(ms.begin(), ms.end());
    const double p50 = ms[ms.size() / 2], p99 = ms[ms.size() * 99 / 100];
    std::cout << "[ login ] 50 full SCRAM logins over loopback UDP: p50 = " << p50 << " ms, p99 = " << p99 << " ms\n";
    ::testing::Test::RecordProperty("login_p50_ms", std::to_string(p50));
    ::testing::Test::RecordProperty("login_p99_ms", std::to_string(p99));
    EXPECT_LE(p50, 5.0) << "median login must stay within the plan's 5 ms gate";
}
