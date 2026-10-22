// Authentication Task 7 fix-on-sight: ClientConfig::authToken was documented as "validated by the broker's
// existing auth path" — no such path existed.  It is removed; the wire field stays reserved and is sent empty.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <type_traits>
#include <unistd.h>

namespace rc = rune::client;

template <class T> concept HasAuthToken = requires(T t) { t.authToken; };
static_assert(!HasAuthToken<rc::ClientConfig>, "ClientConfig::authToken must not exist");
static_assert(!HasAuthToken<rune::client::session::SessionConfig>, "SessionConfig::authToken must not exist");

namespace {
std::string pem(const char* name) {
    std::ifstream f(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}
}  // namespace

TEST(ClientConnectRegression, NoAuthTokenIsEverSentRegression) {
    static int ctr = 0;
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = 3740; c.egressChannel = "aeron:ipc"; c.egressStreamId = 3741;
    c.dataDir = "/tmp/client_connect_reg_" + std::to_string(::getpid()) + "_" + std::to_string(ctr++);
    c.runnerMode  = rc::RunnerMode::Invoker;
    c.offerCrypto = true;                 // the crypto ConnectRequest is the one that carried the field
    c.tlsTrustPem = pem("ca.pem");        // issue #115: an ipc channel names no host - the CA is pinned
    c.username = "ws1"; c.password = "correct horse battery";   // credentials are never put in the token either
    rc::Client client(c);
    test_support::InvokerGateway gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
                                    c.egressStreamId);
    gw.tlsCertPem = pem("server.pem");
    gw.tlsKeyPem  = pem("server.key");
    test_support::TestCq cq(client);
    for (int i = 0; i < 200000 && !gw.ready(); ++i) { client.doWork(); gw.pump(); }
    ASSERT_TRUE(gw.ready());
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
    ASSERT_TRUE(f.accepted());
    for (int i = 0; i < 200000 && gw.connectAuthTokens.empty(); ++i) { client.doWork(); gw.pump(); }
    ASSERT_FALSE(gw.connectAuthTokens.empty()) << "the ConnectRequest reached the broker";
    for (const auto& t : gw.connectAuthTokens) EXPECT_TRUE(t.empty()) << "ConnectRequest.authToken must be empty";
    EXPECT_GE(gw.lastConnectClientVersion, 9);
}
