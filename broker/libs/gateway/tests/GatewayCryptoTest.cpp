// libs/gateway/tests/GatewayCryptoTest.cpp
//
// client<->broker TLS/AEAD (Task 7 handshake + Task 8 data-plane AEAD) —
// gateway SERVER-ROLE crypto, end to end.
//
// Standalone, Aeron-free (this host has no Aeron — task-7/8-interfaces.md's
// BUILD/TEST SPLIT): drives a REAL server-role rune::crypto::TlsSession (the
// broker's own handshake engine — the exact sequence of calls
// ClientGateway::handleConnect/handleHandshakeContinue_ make, replicated here
// without any Aeron/ClientGateway/SessionTable involvement, since those
// headers transitively pull in <Aeron.h>) against a real client-role
// TlsSession, using rune-crypto's own test certs (tests/testdata/{server,ca}.pem
// + server.key).
//
// Task 8 extracted `rune::gateway::CryptoState` into its OWN aeron-free
// header (rune/gateway/CryptoState.h) precisely so the data-plane codec
// (rune/gateway/CryptoFrame.h — openIngressFrame/sealResponseFrame, the
// functions ClientGateway::onClientFrame_/sendTo actually call in
// production) can be used here directly, with zero Aeron dependency. The
// `GatewayCryptoState` alias below is therefore the REAL production type,
// not a hand-rolled mirror (Task 7's version of this file used a field-for-
// field copy because CryptoState lived inside SessionTable.h, which pulls in
// <Aeron.h> — no longer true after the Task 8 extraction).
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
//
// Task 8 data-plane cases (exercise the REAL rune::gateway::codec functions):
//   5. ClientSealedIngressOpensOnGateway — a client-sealed Produce-shaped
//      frame opens via openIngressFrame() with the correct seq/window state.
//   6. GatewaySealedResponseOpensOnClient — sealResponseFrame()'s wire output
//      opens under the client's own b2c AeadCipher.
//   7. TamperedCiphertextDropped — a flipped ciphertext byte fails auth.
//   8. TamperedHeaderAadDropped — a flipped clear-header byte fails auth (the
//      header is part of the AAD).
//   9. ReplayedSeqDropped — resending the exact same wire frame is rejected
//      the second time.
//  10. TooOldSeqDropped — a seq more than 64 behind the current high-water
//      mark is rejected.
//  11. WindowNotPoisonedByForgedFrame — a forged frame (wrong key, claims a
//      fresh seq) fails auth and must NOT advance recvSeq/recvWindow; the
//      SAME seq sent correctly afterwards must still succeed.

#include <gtest/gtest.h>

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>
#include <rune/gateway/CryptoFrame.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace cr  = rune::crypto;
namespace rpc = rune::protocol::client;

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

// The REAL production type (Task 8 extracted it to its own aeron-free
// header) — see the file header comment.
using GatewayCryptoState = rune::gateway::CryptoState;

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

// ===========================================================================
// Task 8 — gateway data-plane AEAD (rune::gateway::codec::openIngressFrame /
// sealResponseFrame, the REAL functions ClientGateway::onClientFrame_/sendTo
// call in production).
// ===========================================================================

namespace {

// Build a plaintext [24B MessageHeader][body] frame exactly the shape every
// protocol::client message encodes (e.g. Produce/ProduceResponse).
std::vector<uint8_t> buildPlainFrame(uint16_t templateId, int64_t sessionId,
                                      const std::string& body) {
    std::vector<uint8_t> frame(rpc::MessageHeader::ENCODED_SIZE + body.size());
    rpc::MessageHeader hdr;
    hdr.templateId = templateId;
    hdr.sessionId  = sessionId;
    hdr.encode(frame.data());
    std::memcpy(frame.data() + rpc::MessageHeader::ENCODED_SIZE, body.data(), body.size());
    return frame;
}

// Seal `plain` the way any real peer (client or gateway) does: clear header
// copied verbatim, clear CryptoSubHeader{seq} encoded right after it, body
// sealed under `sealer` with AAD = the 36 clear bytes. This is the WIRE-level
// counterpart to rune::gateway::codec::sealResponseFrame — used here to
// fabricate CLIENT-sealed ingress frames (sealResponseFrame itself is only
// ever used broker-side) and to independently cross-check sealResponseFrame's
// own output shape in GatewaySealedResponseOpensOnClient.
std::vector<uint8_t> sealFrameAsPeer(cr::AeadCipher& sealer, uint64_t seq,
                                      const std::vector<uint8_t>& plain) {
    std::vector<uint8_t> out(plain.size() + rpc::kCryptoOverhead);
    std::memcpy(out.data(), plain.data(), rpc::MessageHeader::ENCODED_SIZE);
    rpc::CryptoSubHeader sub;
    sub.version = 1;
    sub.seq     = seq;
    sub.encode(out.data() + rpc::MessageHeader::ENCODED_SIZE);
    const std::size_t bodyLen = plain.size() - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t n = sealer.seal(seq, out.data(), rpc::kCryptoHeaderBytes,
                                       plain.data() + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
                                       out.data() + rpc::kCryptoHeaderBytes);
    EXPECT_GT(n, 0u) << "test setup: seal must not itself fail";
    out.resize(rpc::kCryptoHeaderBytes + n);
    return out;
}

// Drives a full handshake and returns the gateway-side CryptoState (recv<-c2b,
// send<-b2c) plus the two client-side AeadCiphers a real client would hold
// (clientSend keyed c2b, clientRecv keyed b2c) — the shared fixture every
// Task 8 data-plane test below starts from.
struct DataPlaneFixture {
    GatewayCryptoState gw;
    std::optional<cr::AeadCipher> clientSend;  // client -> broker (c2b)
    std::optional<cr::AeadCipher> clientRecv;  // broker -> client (b2c)
};

DataPlaneFixture makeDataPlaneFixture(int64_t sessionId) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));

    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    cr::TlsSession cli = cr::TlsSession::makeClient(caPem.c_str(), "localhost");
    EXPECT_TRUE(srv.valid());
    EXPECT_TRUE(cli.valid());
    EXPECT_TRUE(driveServerHandshake(srv, cli));

    constexpr cr::Cipher kCipher = cr::Cipher::Aes256Gcm;
    const cr::SessionKeys srvKeys = srv.exportKeys(kCipher, sessionId);
    const cr::SessionKeys cliKeys = cli.exportKeys(kCipher, sessionId);
    EXPECT_NE(srvKeys.cipher, cr::Cipher::None);
    EXPECT_NE(cliKeys.cipher, cr::Cipher::None);

    DataPlaneFixture f;
    f.gw.cipher = srvKeys.cipher;
    f.gw.recv.emplace(srvKeys.cipher, srvKeys.c2b, srvKeys.c2bPrefix);  // OPENs client->broker
    f.gw.send.emplace(srvKeys.cipher, srvKeys.b2c, srvKeys.b2cPrefix);  // SEALs broker->client
    f.gw.encrypted = true;
    f.clientSend.emplace(cliKeys.cipher, cliKeys.c2b, cliKeys.c2bPrefix);
    f.clientRecv.emplace(cliKeys.cipher, cliKeys.b2c, cliKeys.b2cPrefix);
    return f;
}

}  // namespace

TEST(GatewayCryptoDataPlane, ClientSealedIngressOpensOnGateway) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/7);

    const auto plain = buildPlainFrame(/*templateId=*/16 /*Produce*/, 7, "produce-body");
    const auto wire   = sealFrameAsPeer(*f.clientSend, /*seq=*/1, plain);

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());

    ASSERT_EQ(n, plain.size());
    EXPECT_EQ(0, std::memcmp(scratch.data(), plain.data(), plain.size()));
    EXPECT_EQ(f.gw.recvSeq, 1u);
    EXPECT_NE(f.gw.recvWindow & 1u, 0u);
}

TEST(GatewayCryptoDataPlane, GatewaySealedResponseOpensOnClient) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/8);

    const auto plain = buildPlainFrame(/*templateId=*/17 /*ProduceResponse*/, 8, "response-body");
    std::vector<uint8_t> sealScratch(plain.size() + rpc::kCryptoOverhead);
    const std::size_t sealedLen = rune::gateway::codec::sealResponseFrame(
        f.gw, plain.data(), plain.size(), sealScratch.data(), sealScratch.size());
    ASSERT_GT(sealedLen, 0u);
    EXPECT_EQ(f.gw.sendSeq, 1u);

    // Open it the way a real client would: decode the subheader for `seq`,
    // then AeadCipher::open under the client's own b2c key, AAD = the clear
    // 36 bytes exactly as sealResponseFrame wrote them.
    const auto subOpt = rpc::CryptoSubHeader::decode(
        sealScratch.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealedLen - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(subOpt.has_value());
    EXPECT_EQ(subOpt->seq, 1u);

    std::vector<uint8_t> recovered(sealedLen);
    const long m = f.clientRecv->open(
        subOpt->seq, sealScratch.data(), rpc::kCryptoHeaderBytes,
        sealScratch.data() + rpc::kCryptoHeaderBytes, sealedLen - rpc::kCryptoHeaderBytes,
        recovered.data());
    ASSERT_EQ(m, static_cast<long>(plain.size() - rpc::MessageHeader::ENCODED_SIZE));
    EXPECT_EQ(0, std::memcmp(recovered.data(), plain.data() + rpc::MessageHeader::ENCODED_SIZE,
                             static_cast<std::size_t>(m)));
}

TEST(GatewayCryptoDataPlane, TamperedCiphertextDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/9);
    const auto plain = buildPlainFrame(16, 9, "produce-body");
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    // Flip one ciphertext byte (just past the clear 36-byte AAD region).
    wire[rpc::kCryptoHeaderBytes] ^= 0xFF;

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u);
    // Auth failed before commit — window untouched.
    EXPECT_EQ(f.gw.recvSeq, 0u);
    EXPECT_EQ(f.gw.recvWindow, 0u);
}

TEST(GatewayCryptoDataPlane, TamperedHeaderAadDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/10);
    const auto plain = buildPlainFrame(16, 10, "produce-body");
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    // Flip a byte inside the clear MessageHeader (sessionId field) — it's
    // part of the AAD, so this must also fail auth even though the
    // ciphertext/tag themselves are untouched.
    wire[8] ^= 0xFF;

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u);
    EXPECT_EQ(f.gw.recvSeq, 0u);
    EXPECT_EQ(f.gw.recvWindow, 0u);
}

TEST(GatewayCryptoDataPlane, ReplayedSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/11);
    const auto plain = buildPlainFrame(16, 11, "produce-body");
    const auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n1 = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    ASSERT_EQ(n1, plain.size());
    EXPECT_EQ(f.gw.recvSeq, 1u);

    // Resend the EXACT same wire frame — the replay-window check rejects it
    // read-only, before even attempting to open().
    const std::size_t n2 = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n2, 0u);
    // State unchanged by the rejected replay.
    EXPECT_EQ(f.gw.recvSeq, 1u);
}

TEST(GatewayCryptoDataPlane, TooOldSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/12);
    const auto plain = buildPlainFrame(16, 12, "produce-body");

    // Advance the window to seq=100 first.
    const auto wireAhead = sealFrameAsPeer(*f.clientSend, 100, plain);
    std::vector<uint8_t> scratch(wireAhead.size());
    const std::size_t nAhead = rune::gateway::codec::openIngressFrame(
        f.gw, wireAhead.data(), wireAhead.size(), scratch.data(), scratch.size());
    ASSERT_EQ(nAhead, plain.size());
    ASSERT_EQ(f.gw.recvSeq, 100u);

    // seq=1 is now 99 behind the high-water mark (>= 64) — too old, rejected.
    const auto wireOld = sealFrameAsPeer(*f.clientSend, 1, plain);
    scratch.assign(wireOld.size(), 0);
    const std::size_t nOld = rune::gateway::codec::openIngressFrame(
        f.gw, wireOld.data(), wireOld.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nOld, 0u);
    EXPECT_EQ(f.gw.recvSeq, 100u);  // unchanged
}

TEST(GatewayCryptoDataPlane, WindowNotPoisonedByForgedFrame) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/13);
    const auto plain = buildPlainFrame(16, 13, "produce-body");

    // Forge: seal with a DIFFERENT (wrong) key claiming seq=5 — simulates an
    // attacker without the real c2b key guessing/replaying a seq number.
    // AeadCipher has no public prefix accessor (by design — it's private
    // key-schedule state), so any 4-byte nonce prefix works here: the test
    // only needs the wrong key material to make open() fail auth.
    const uint8_t fakePrefix[cr::kNoncePrefix] = {1, 2, 3, 4};
    cr::AeadCipher wrongKeyCipher(f.gw.cipher,
                                  cr::AeadKey{} /* all-zero, != real c2b */,
                                  fakePrefix);
    const auto forged = sealFrameAsPeer(wrongKeyCipher, /*seq=*/5, plain);

    std::vector<uint8_t> scratch(forged.size());
    const std::size_t nForged = rune::gateway::codec::openIngressFrame(
        f.gw, forged.data(), forged.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nForged, 0u);  // auth failure
    // Window must NOT have been poisoned to seq=5 by the forged frame.
    EXPECT_EQ(f.gw.recvSeq, 0u);
    EXPECT_EQ(f.gw.recvWindow, 0u);

    // The SAME seq=5, sealed correctly, must still be accepted afterwards —
    // proof the forged attempt left no trace in the window state.
    const auto legit = sealFrameAsPeer(*f.clientSend, /*seq=*/5, plain);
    scratch.assign(legit.size(), 0);
    const std::size_t nLegit = rune::gateway::codec::openIngressFrame(
        f.gw, legit.data(), legit.size(), scratch.data(), scratch.size());
    ASSERT_EQ(nLegit, plain.size());
    EXPECT_EQ(f.gw.recvSeq, 5u);
}

// ---------------------------------------------------------------------------
// RED-evidence toggle (task-8-interfaces.md: "Show RED first ... move the
// window-commit before auth, or skip the handshake-leg exemption"). This is
// a LOCAL, deliberately-buggy copy of openIngressFrame's logic — commits the
// replay window BEFORE checking the AEAD auth result, instead of after — to
// prove WindowNotPoisonedByForgedFrame actually catches that class of bug.
// Always `false` in the committed file; see task-8-report.md for the RED run.
// ---------------------------------------------------------------------------
namespace {
constexpr bool kInjectCommitBeforeAuthBug = false;

// Reuses the REAL replayWindowCheck/Commit (rune::gateway::codec::
// detail_crypto) so this isolates EXACTLY the ordering bug the brief names —
// the check is identical to the production function; only the COMMIT is
// moved to before open() instead of after.
std::size_t openIngressFrameBuggy(GatewayCryptoState& crypto, const uint8_t* wire,
                                   std::size_t wireLen, uint8_t* scratch,
                                   std::size_t scratchCap) noexcept {
    namespace dc = rune::gateway::codec::detail_crypto;
    constexpr std::size_t kMinWire = rpc::kCryptoHeaderBytes + rune::crypto::kTagSize;
    if (wireLen < kMinWire || !crypto.recv.has_value()) return 0;
    const auto subOpt = rpc::CryptoSubHeader::decode(
        wire + rpc::MessageHeader::ENCODED_SIZE, wireLen - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return 0;
    const uint64_t seq = subOpt->seq;

    if (!dc::replayWindowCheck(crypto.recvSeq, crypto.recvWindow, seq)) return 0;

    // BUG: commit the window before authenticating — a forged seq that
    // passes the (pre-auth) check now poisons the window even though the
    // frame is about to be rejected for failing the AEAD tag.
    dc::replayWindowCommit(crypto.recvSeq, crypto.recvWindow, seq);

    const std::size_t ctLen    = wireLen - rpc::kCryptoHeaderBytes;
    const std::size_t plainCap = ctLen - rune::crypto::kTagSize;
    const std::size_t outLen   = rpc::MessageHeader::ENCODED_SIZE + plainCap;
    if (scratchCap < outLen) return 0;
    const long plainLen = crypto.recv->open(seq, wire, rpc::kCryptoHeaderBytes,
                                             wire + rpc::kCryptoHeaderBytes, ctLen,
                                             scratch + rpc::MessageHeader::ENCODED_SIZE);
    if (plainLen < 0) return 0;
    std::memcpy(scratch, wire, rpc::MessageHeader::ENCODED_SIZE);
    return rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(plainLen);
}
}  // namespace

TEST(GatewayCryptoDataPlane, WindowNotPoisonedByForgedFrame_RedToggle) {
    if (!kInjectCommitBeforeAuthBug) {
        GTEST_SKIP() << "RED-evidence toggle off in the committed file (see task-8-report.md)";
    }
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/14);
    const auto plain = buildPlainFrame(16, 14, "produce-body");

    const uint8_t fakePrefix[cr::kNoncePrefix] = {1, 2, 3, 4};
    cr::AeadCipher wrongKeyCipher(f.gw.cipher, cr::AeadKey{}, fakePrefix);
    const auto forged = sealFrameAsPeer(wrongKeyCipher, /*seq=*/5, plain);

    std::vector<uint8_t> scratch(forged.size());
    const std::size_t nForged = openIngressFrameBuggy(
        f.gw, forged.data(), forged.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nForged, 0u);

    // With the bug, the window was already poisoned to seq=5 — the
    // legitimate seq=5 frame is now wrongly rejected as a replay.
    const auto legit = sealFrameAsPeer(*f.clientSend, /*seq=*/5, plain);
    scratch.assign(legit.size(), 0);
    const std::size_t nLegit = openIngressFrameBuggy(
        f.gw, legit.data(), legit.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nLegit, plain.size()) << "expected to FAIL under the injected bug (RED evidence)";
}
