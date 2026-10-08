// libs/gateway/tests/GatewayCryptoTest.cpp
//
// client<->broker TLS/AEAD (Task 7) — gateway SERVER-ROLE handshake.
//
// Standalone, Aeron-free (this host has no Aeron — task-7-interfaces.md's
// BUILD/TEST SPLIT): drives a REAL server-role rune::crypto::TlsSession (the
// broker's own handshake engine — the exact sequence of calls
// ClientGateway::handleConnect/handleHandshakeContinue_ make, replicated here
// without any Aeron/ClientGateway/SessionTable involvement, since those
// headers transitively pull in <Aeron.h>) against a real client-role
// TlsSession, using rune-crypto's own test certs (tests/testdata/{server,ca}.pem
// + server.key).
//
// `rune::gateway::CryptoState` (libs/gateway/include/rune/gateway/
// SessionTable.h) cannot be instantiated here directly for the same reason —
// `GatewayCryptoState` below is a field-for-field mirror of it (Rule 3: each
// state holds exactly what it needs), not a new/different design; it exists
// purely so this file has zero Aeron dependency.
//
// What this proves:
//   1. HandshakeReachesDoneAndKeysAgree — a full in-process handshake
//      (client TlsSession <-> "gateway" server TlsSession) reaches done() on
//      BOTH sides; exportKeys() with the same sessionId + role-correct
//      direction labels on both sides yields identical keys.
//   2. DirectionMappingSealOpenRoundTrips — the DIRECTION MAPPING
//      task-7-interfaces.md calls out as the #1 correctness trap: the
//      broker's CryptoState.recv OPENS client->broker traffic (c2b/
//      c2bPrefix), CryptoState.send SEALS broker->client traffic (b2c/
//      b2cPrefix) — the MIRROR of the client's own send<-c2b/recv<-b2c. A
//      client-sealed frame opens on the broker's `recv`, and a broker-sealed
//      frame opens on the client's `recv`, in both directions.
//   3. BadClientHelloFailsHandshakeNonRetriably — malformed/invalid opening
//      handshake bytes (handleConnect's "a bad client token" case — anything
//      the TLS engine rejects outright, not a real TLS 1.3 ClientHello
//      record) make the server-role TlsSession's readInbound() return
//      HsState::Failed on the FIRST call: exactly the condition handleConnect
//      checks to set PendingConnect::cryptoFailed -> pollPendingConnects
//      answers ConnectResponse{HandshakeFailed}, no session ever created.
//   4. ExportKeysNoneSentinelNeverInstalled — the fail-closed contract
//      task-7-interfaces.md requires: Cipher::None is the sentinel that MUST
//      be checked (and is, in handleHandshakeContinue_) before any AeadCipher
//      is built from exported keys — never install all-zero keys.

#include <gtest/gtest.h>

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>

namespace cr = rune::crypto;

namespace {

std::string slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

std::string testdataPath(const char* name) {
#ifdef RUNE_CRYPTO_TESTDATA_DIR
    return std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name;
#else
    return std::string("testdata/") + name;
#endif
}

// Field-for-field mirror of rune::gateway::CryptoState (SessionTable.h) — see
// the file header comment for why this file can't include that header.
struct GatewayCryptoState {
    bool                          encrypted  = false;
    cr::Cipher                    cipher{};
    uint16_t                      epoch      = 0;
    uint64_t                      sendSeq    = 0;
    uint64_t                      recvSeq    = 0;
    uint64_t                      recvWindow = 0;
    std::optional<cr::AeadCipher> send, recv;
};

// client<->broker TLS/AEAD (Task 7): the TOGGLE under test for the direction
// mapping — set to true to reproduce the ★ bug task-7-interfaces.md warns
// about (recv<-b2c/send<-c2b, the MIRROR of the correct mapping below) and
// confirm DirectionMappingSealOpenRoundTrips catches it (RED evidence; see
// task-7-report.md).  Always false in the committed file.
constexpr bool kInjectDirectionMappingBug = false;

// Drive a full in-process TLS 1.3 handshake between a real server-role `srv`
// (built exactly as ClientGateway::handleConnect builds one, via
// TlsSession::makeServer) and a real client-role `cli`.  Each round fully
// drains one side's pending outbound bytes into the other before swapping
// direction; stops once both report done(), or once a round makes no
// progress (a real ping-pong bug fails the test instead of hanging it).
bool driveServerHandshake(cr::TlsSession& srv, cr::TlsSession& cli) {
    cli.readInbound(nullptr, 0);  // kicks off ClientHello generation (client's first call)
    uint8_t chunk[16384];

    for (int round = 0; round < 10; ++round) {
        bool progressed = false;

        for (;;) {  // client -> server
            const std::size_t n = cli.writeOutbound(chunk, sizeof chunk);
            if (n == 0) break;
            progressed = true;
            if (srv.readInbound(chunk, n) == cr::HsState::Failed) return false;
        }
        for (;;) {  // server -> client
            const std::size_t n = srv.writeOutbound(chunk, sizeof chunk);
            if (n == 0) break;
            progressed = true;
            if (cli.readInbound(chunk, n) == cr::HsState::Failed) return false;
        }

        if (srv.done() && cli.done()) return true;
        if (!progressed) break;
    }
    return srv.done() && cli.done();
}

}  // namespace

TEST(GatewayCrypto, HandshakeReachesDoneAndKeysAgree) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    ASSERT_FALSE(serverPem.empty());
    ASSERT_FALSE(serverKey.empty());
    ASSERT_FALSE(caPem.empty());

    // "The gateway" — server role, exactly ClientGateway::handleConnect's
    // TlsSession::makeServer(tlsCertPem_.c_str(), tlsKeyPem_.c_str()) call.
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    // A real client, trusting the test CA and checking the server's SAN.
    cr::TlsSession cli = cr::TlsSession::makeClient(caPem.c_str(), "localhost");
    ASSERT_TRUE(cli.valid());

    ASSERT_TRUE(driveServerHandshake(srv, cli)) << "handshake did not reach done() on both sides";

    constexpr int64_t kSessionId = 42;
    constexpr cr::Cipher kCipher = cr::Cipher::Aes256Gcm;  // the client's cipherList[0]

    const cr::SessionKeys srvKeys = srv.exportKeys(kCipher, kSessionId);
    const cr::SessionKeys cliKeys = cli.exportKeys(kCipher, kSessionId);
    ASSERT_NE(srvKeys.cipher, cr::Cipher::None);
    ASSERT_NE(cliKeys.cipher, cr::Cipher::None);
    EXPECT_EQ(srvKeys.cipher, cliKeys.cipher);
    EXPECT_EQ(0, std::memcmp(srvKeys.c2b.bytes, cliKeys.c2b.bytes, sizeof srvKeys.c2b.bytes));
    EXPECT_EQ(0, std::memcmp(srvKeys.b2c.bytes, cliKeys.b2c.bytes, sizeof srvKeys.b2c.bytes));
    EXPECT_EQ(0, std::memcmp(srvKeys.c2bPrefix, cliKeys.c2bPrefix, sizeof srvKeys.c2bPrefix));
    EXPECT_EQ(0, std::memcmp(srvKeys.b2cPrefix, cliKeys.b2cPrefix, sizeof srvKeys.b2cPrefix));
}

TEST(GatewayCrypto, DirectionMappingSealOpenRoundTrips) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));

    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    cr::TlsSession cli = cr::TlsSession::makeClient(caPem.c_str(), "localhost");
    ASSERT_TRUE(srv.valid());
    ASSERT_TRUE(cli.valid());
    ASSERT_TRUE(driveServerHandshake(srv, cli));

    constexpr int64_t    kSessionId = 7;
    constexpr cr::Cipher kCipher    = cr::Cipher::Aes256Gcm;
    const cr::SessionKeys srvKeys = srv.exportKeys(kCipher, kSessionId);
    const cr::SessionKeys cliKeys = cli.exportKeys(kCipher, kSessionId);
    ASSERT_NE(srvKeys.cipher, cr::Cipher::None);
    ASSERT_NE(cliKeys.cipher, cr::Cipher::None);

    // The GATEWAY's CryptoState — exactly handleHandshakeContinue_'s
    // construction (task-7-interfaces.md's ★ DIRECTION MAPPING):
    //   recv <- c2b/c2bPrefix  (OPENs client->broker ingress)
    //   send <- b2c/b2cPrefix  (SEALs broker->client egress)
    GatewayCryptoState gw;
    gw.cipher = srvKeys.cipher;
    if constexpr (kInjectDirectionMappingBug) {
        // RED-evidence toggle (see the constant's doc comment): mirror-swapped,
        // reproducing the #1 trap task-7-interfaces.md warns about.
        gw.recv.emplace(srvKeys.cipher, srvKeys.b2c, srvKeys.b2cPrefix);
        gw.send.emplace(srvKeys.cipher, srvKeys.c2b, srvKeys.c2bPrefix);
    } else {
        gw.recv.emplace(srvKeys.cipher, srvKeys.c2b, srvKeys.c2bPrefix);
        gw.send.emplace(srvKeys.cipher, srvKeys.b2c, srvKeys.b2cPrefix);
    }
    gw.encrypted = true;

    // The CLIENT's CryptoState — rune::client::session::CryptoState's own
    // construction (SessionMachine.h finishHandshake_): send<-c2b, recv<-b2c.
    cr::AeadCipher clientSend(cliKeys.cipher, cliKeys.c2b, cliKeys.c2bPrefix);  // client -> broker
    cr::AeadCipher clientRecv(cliKeys.cipher, cliKeys.b2c, cliKeys.b2cPrefix);  // broker -> client

    const uint8_t aad[4] = {9, 9, 9, 9};

    // client -> broker: client seals with its `send` (c2b); the broker OPENS
    // with `recv` (c2b) — same key, same direction label, must round-trip.
    {
        const uint8_t plain[] = "hello gateway";
        uint8_t       ct[sizeof(plain) + cr::kTagSize];
        const std::size_t n = clientSend.seal(1, aad, sizeof aad, plain, sizeof plain, ct);
        ASSERT_GT(n, 0u);
        uint8_t recovered[sizeof(plain)];
        const long m = gw.recv->open(1, aad, sizeof aad, ct, n, recovered);
        ASSERT_EQ(m, static_cast<long>(sizeof plain));
        EXPECT_EQ(0, std::memcmp(plain, recovered, sizeof plain));
    }

    // broker -> client: the broker seals with its `send` (b2c); the client
    // OPENS with its `recv` (b2c) — same key, same direction label.
    {
        const uint8_t plain[] = "hello client";
        uint8_t       ct[sizeof(plain) + cr::kTagSize];
        const std::size_t n = gw.send->seal(1, aad, sizeof aad, plain, sizeof plain, ct);
        ASSERT_GT(n, 0u);
        uint8_t recovered[sizeof(plain)];
        const long m = clientRecv.open(1, aad, sizeof aad, ct, n, recovered);
        ASSERT_EQ(m, static_cast<long>(sizeof plain));
        EXPECT_EQ(0, std::memcmp(plain, recovered, sizeof plain));
    }
}

TEST(GatewayCrypto, BadClientHelloFailsHandshakeNonRetriably) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());

    // handleConnect's FIRST call: srv.readInbound(req.handshakeBytes, ...) —
    // a garbage blob is not a valid TLS 1.3 ClientHello record (the catch-all
    // "bad client token" case: a malformed opening handshake, not a real flight).
    const uint8_t garbage[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03};
    const cr::HsState hs = srv.readInbound(garbage, sizeof garbage);
    EXPECT_EQ(hs, cr::HsState::Failed);
    // Mirrors handleConnect exactly: this is the condition that sets
    // PendingConnect::cryptoFailed -> ConnectResponse{HandshakeFailed}, no session.
    EXPECT_FALSE(srv.done());
}

TEST(GatewayCrypto, ExportKeysNoneSentinelNeverInstalled) {
    // A valid() TlsSession that was never driven to done() must fail the
    // exportKeys() sentinel check -- the ONE guard handleHandshakeContinue_
    // relies on to never install an all-zero AeadCipher.
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    ASSERT_FALSE(srv.done());

    const cr::SessionKeys keys = srv.exportKeys(cr::Cipher::Aes256Gcm, 42);
    ASSERT_EQ(keys.cipher, cr::Cipher::None);  // the documented fail-closed sentinel

    // handleHandshakeContinue_'s guard, reproduced: only build AeadCiphers
    // when the sentinel says it's safe.
    GatewayCryptoState crypto;
    if (keys.cipher != cr::Cipher::None) {
        crypto.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
        crypto.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
        crypto.encrypted = true;
    }
    EXPECT_FALSE(crypto.encrypted);
    EXPECT_FALSE(crypto.recv.has_value());
    EXPECT_FALSE(crypto.send.has_value());
}
