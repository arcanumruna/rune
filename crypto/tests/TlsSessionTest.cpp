#include <gtest/gtest.h>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include "rune/crypto/TlsSession.h"
using namespace rune::crypto;

static std::string slurp(const char* p) {
  std::ifstream f(p, std::ios::binary);
  std::ostringstream ss;
  ss << f.rdbuf();
  return ss.str();
}

// Pump the two sessions until both report done() or either fails.
// Note: the brief's pump() pseudocode compares done() (bool) to HsState::Failed, which
// doesn't typecheck against the actual interface (done() is bool; HsState comes back from
// readInbound/writeOutbound). This captures those return values directly instead.
static bool pump(TlsSession& cli, TlsSession& srv) {
  uint8_t buf[16384];
  HsState cs = HsState::NeedInput, ss = HsState::NeedInput;
  for (int i = 0; i < 20 && !(cli.done() && srv.done()); ++i) {
    size_t n = cli.writeOutbound(buf, sizeof buf);
    if (n) ss = srv.readInbound(buf, n);
    n = srv.writeOutbound(buf, sizeof buf);
    if (n) cs = cli.readInbound(buf, n);
    if (cs == HsState::Failed || ss == HsState::Failed) return false;
  }
  return cli.done() && srv.done();
}

TEST(Tls, HandshakeSucceedsAndKeysAgree) {
  auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli = TlsSession::makeClient(slurp("testdata/ca.pem").c_str(), "localhost");
  ASSERT_TRUE(srv.valid());
  ASSERT_TRUE(cli.valid());
  cli.readInbound(nullptr, 0);  // kick client to emit ClientHello
  ASSERT_TRUE(pump(cli, srv));
  auto ck = cli.exportKeys(Cipher::Aes256Gcm, 42);
  auto sk = srv.exportKeys(Cipher::Aes256Gcm, 42);
  EXPECT_EQ(0, memcmp(ck.c2b.bytes, sk.c2b.bytes, 32));  // both sides derive identical directional keys
  EXPECT_EQ(0, memcmp(ck.b2c.bytes, sk.b2c.bytes, 32));
  EXPECT_EQ(0, memcmp(ck.c2bPrefix, sk.c2bPrefix, 4));
  EXPECT_EQ(0, memcmp(ck.b2cPrefix, sk.b2cPrefix, 4));
  EXPECT_NE(0, memcmp(ck.c2b.bytes, ck.b2c.bytes, 32));  // the two directions must not collide
}

TEST(Tls, WrongHostnameRejected) {
  auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli = TlsSession::makeClient(slurp("testdata/ca.pem").c_str(), "wrong.example");  // not the cert SAN
  cli.readInbound(nullptr, 0);
  EXPECT_FALSE(pump(cli, srv));  // verification failure aborts
}

TEST(Tls, UntrustedCertRejected) {
  auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli = TlsSession::makeClient("", "localhost");  // empty trust: server cert is untrusted
  cli.readInbound(nullptr, 0);
  EXPECT_FALSE(pump(cli, srv));
}
