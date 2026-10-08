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
//
// Task 8b cases (rune::node::sealRecordsFrame, RecordsSeal.h — the function
// ClientGateway::pushRecords_ calls for an ENCRYPTED session's Records push;
// aeron-free by construction, so it IS unit-testable standalone, unlike
// sendRecordsFrame itself which pulls in <ExclusivePublication.h>):
//  12. RecordsPushSealOpensOnClient_Small — a single-fragment-sized Records
//      frame, sealed exactly the way pushRecords_ will, opens under the
//      client's b2c AeadCipher and round-trips the ORIGINAL Records fields
//      (not just raw bytes) through Records::decode.
//  13. RecordsPushSealOpensOnClient_Large — a batch far larger than one MTU
//      (the plaintext path would have taken the zero-copy vectored offer)
//      proves the seal-level contiguous-assembly logic at that size.
//  14. RecordsPushSealTamperedCiphertextDropped — a flipped ciphertext byte
//      fails auth; the client must never accept a tampered Records push.
//  15. RecordsPushSealMisconfiguredCtxFailsClosed — a RecordsSealCtx with a
//      null `send` (the "encrypted but key not installed" case) makes
//      sealRecordsFrame fail closed (return 0), never falling back to
//      transmitting the frame in the clear.
//  16. RecordsPushPlaintextBug_RedToggle — RED-evidence toggle: reproduces
//      the ORIGINAL Task-8b bug (Records pushed in the clear on an
//      encrypted session) and shows the client-side open attempt on those
//      unsealed bytes fails/garbles — exactly why consumer fetch was
//      entirely broken on an encrypted session before this fix.
//
// Task 19 cases (per-worker egress crypto addendum, 2026-10-08 — a SHARD
// WORKER seals its OWN Records push with its OWN deriveSenderKey()-derived
// key, RecordsSealCtx::senderId):
//  17. WorkerSealedRecordsOpenUnderDerivedPerSenderKey — a worker's sealed
//      frame (senderId=w+1) opens under the SAME key the client
//      independently re-derives from its own b2c; the subheader carries
//      the authenticated senderId.
//  18. TwoWorkersProduceDistinctKeysNeitherOpensUnderTheOther — two workers'
//      derived keys differ; each worker's frame opens ONLY under its own
//      key, never the other's.
//  19. WorkerSealedRecordsWrongSenderIdTamperDropped — forging the
//      authenticated senderId byte fails auth (never silently redirected).
//  20. WorkerDeriveSenderKeyZeroEqualsGatewayB2c — senderId==0 is defined
//      to equal the gateway's own (unchanged) b2c key.
//
// Task 22 cases (shard worker DECRYPTS produce / per-worker ingress,
// 2026-10-08 — rune::node::openProduceFrame, RecordsOpen.h, the ingress
// counterpart to Task 19's egress seal; ingress has a SINGLE sender, the
// client, so ONE c2b key shared by every worker — no per-sender split):
//  21. WorkerIngressOpensClientSealedProduceAndDecodes — a client-sealed
//      Produce frame opens on a worker-owned RecordsOpenCtx and the
//      reconstructed plaintext decodes correctly via the REAL
//      rune::protocol::client::Produce::decode.
//  22. WorkerIngressTamperedCiphertextDropped / 23. TamperedHeaderAadDropped
//      — tamper anywhere in the AAD or ciphertext fails auth, dropped.
//  24. WorkerIngressReplayedSeqDropped — the exact same wire frame replayed
//      is rejected the second time.
//  25. WorkerIngressTooOldSeqDropped — a seq more than 64 behind the
//      worker's own high-water mark is rejected.
//  26. WorkerIngressWindowNotPoisonedByForgedFrame (+ RedToggle) — a forged
//      (wrong-key) frame fails auth and must not poison the window against
//      a later, legitimately-sealed frame using the same seq.
//  27. WorkerIngressNoKeyYetFailsClosed — no c2b key installed yet (pre-key/
//      unknown session) fails closed, never a plaintext fallback.
//  28. WorkerIngressOwnWindowIndependentOfGatewayWindow — the worker's own
//      replay window tolerates forward gaps left by gateway-bound frames
//      sharing the client's single c2b seq stream; neither window is
//      touched by the other's traffic.
//
// Task 23 cases (shard worker SEALS produce acks / SessionAckSink, mirrors
// Task 19's flushAcks_ seal branch exactly — same sealRecordsFrame, same
// worker per-sender b2c cipher, THE SAME sendSeq counter Records uses):
//  29. WorkerAckSealedProduceResponseOpensUnderWorkerSenderKey — a
//      ProduceResponse sealed the way flushAcks_'s encrypted branch does
//      (encodeFor into a scratch, then sealRecordsFrame under the worker's
//      derived per-sender key) opens under the client's independently
//      re-derived sender-(w+1) key and decodes to the right ProduceResponse;
//      tamper/replay dropped.
//  30. WorkerAckAndRecordsShareOneMonotonicSendSeq — ★★ the nonce-interleave
//      proof: seal a Records frame then an ack under the SAME AeadCipher +
//      the SAME shared sendSeq counter; both open and their subheader seqs
//      are consecutive/distinct (N, N+1) — never a collision.
//  31. WorkerAckAndRecordsSeparateCounterCollides_RedToggle — flips to two
//      INDEPENDENT counters (the bug Task 23 forbids) and shows the
//      resulting seq collision (both frames claim seq=1 under one key) —
//      RED evidence for why the ack sink must never own its own sendSeq.

#include <gtest/gtest.h>

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>
#include <rune/gateway/CryptoFrame.h>
#include <rune/node/RecordsOpen.h>
#include <rune/node/RecordsSeal.h>
#include <rune/node/SessionAnnounceKeyTail.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/DataMessages.h>
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
    // Task 19: retain the RAW b2c key/prefix exactly as
    // handleHandshakeContinue_ now does, so tests can derive per-worker
    // sender keys from the SAME fixture (mirrors the real gateway code).
    f.gw.b2c = srvKeys.b2c;
    std::memcpy(f.gw.b2cPrefix, srvKeys.b2cPrefix, 4);
    // Task 22: retain the RAW c2b key/prefix exactly as
    // handleHandshakeContinue_ now ALSO does, so tests can ship the SAME
    // ingress key to a simulated worker (mirrors the real gateway code).
    f.gw.c2b = srvKeys.c2b;
    std::memcpy(f.gw.c2bPrefix, srvKeys.c2bPrefix, 4);
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

// ===========================================================================
// Task 8b — seal the BROKER's Records push (consumer-fetch) path.
//
// rune::node::sealRecordsFrame (RecordsSeal.h) is the raw-crypto-params seal
// step rune::node::sendRecordsFrame (RecordsPush.h) calls for an ENCRYPTED
// session's Records push — see RecordsPush.h's file header comment for the
// full encrypted-path flow (assemble contiguous plaintext -> seal -> publish
// a single contiguous sealed buffer). sendRecordsFrame itself cannot be
// linked here (it pulls in <ExclusivePublication.h>, unavailable on this
// aeron-free host — the aeron-linked tryClaim/offer wiring is box-deferred),
// but sealRecordsFrame has ZERO Aeron dependency by construction, so the
// seal/open round-trip — the actual bug fix (Records sent in the clear on
// an encrypted session, dropped by the client) — is fully testable here.
// ===========================================================================

namespace {

namespace pn = rune::node;

// Build a Records frame exactly as ClientGateway::pushSubscriptions()
// builds one (sessionId/correlationId/subscriptionId/topicId/partition/
// baseOffset + a zero-copy-in-production, here owned, batch span).
rpc::Records makeRecordsFrame(int64_t sessionId, int64_t correlationId, int64_t subscriptionId,
                               int32_t topicId, int32_t partition, int64_t baseOffset,
                               const std::vector<uint8_t>& batch) {
    rpc::Records rec;
    rec.sessionId      = sessionId;
    rec.correlationId  = correlationId;
    rec.subscriptionId = subscriptionId;
    rec.topicId        = topicId;
    rec.partition       = partition;
    rec.baseOffset      = baseOffset;
    rec.recordBatch     = std::span<const uint8_t>(batch.data(), batch.size());
    return rec;
}

// Seal `rec` exactly the way pushRecords_/sendRecordsFrame's ENCRYPTED
// branch does: rec.encode() into a contiguous plaintext buffer, then
// rune::node::sealRecordsFrame() into a sealed buffer.  Returns the sealed
// bytes (empty on seal failure).
std::vector<uint8_t> sealRecordsAsPushWould(const rpc::Records& rec, cr::AeadCipher& send,
                                            uint64_t& sendSeq, cr::Cipher cipher, uint16_t epoch,
                                            uint8_t senderId = 0) {
    const std::size_t frameLen = rec.encodedSize();
    std::vector<uint8_t> plain(frameLen);
    rec.encode(plain.data());

    pn::RecordsSealCtx ctx;
    ctx.send     = &send;
    ctx.sendSeq  = &sendSeq;
    ctx.cipher   = static_cast<uint8_t>(cipher);
    ctx.epoch    = epoch;
    ctx.senderId = senderId;   // Task 19: 0 == gateway (byte-identical default)

    std::vector<uint8_t> sealed(frameLen + rpc::kCryptoOverhead);
    const std::size_t n = pn::sealRecordsFrame(ctx, plain.data(), frameLen, sealed.data(), sealed.size());
    sealed.resize(n);  // n==0 -> empty, signalling failure to the caller
    return sealed;
}

// Open `sealed` the way the CLIENT's dispatchEgressFrame does for every
// non-handshake egress frame: decode the CryptoSubHeader at [24..35], AEAD
// open under the client's own b2c AeadCipher (AAD = the 36 clear bytes),
// then Records::decode the reconstructed [24B header][plaintext body].
// Returns nullopt on ANY failure (truncation, decode failure, auth
// failure) — exactly "the client drops it" from the bug report.
//
// `storage` is an OUT param: Records::decode returns a ZERO-COPY view into
// the buffer it was given (recordBatch is a span, not an owned vector), so
// the reconstructed plaintext must outlive the returned Records — it is
// caller-owned (kept alive by the test for as long as it inspects the
// result), never a function-local temporary.
std::optional<rpc::Records> openRecordsAsClientWould(const std::vector<uint8_t>& sealed,
                                                      cr::AeadCipher& recv,
                                                      std::vector<uint8_t>& storage) {
    if (sealed.size() < rpc::kCryptoHeaderBytes + cr::kTagSize) return std::nullopt;
    const auto subOpt = rpc::CryptoSubHeader::decode(
        sealed.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealed.size() - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return std::nullopt;

    const std::size_t ctLen = sealed.size() - rpc::kCryptoHeaderBytes;
    storage.assign(rpc::MessageHeader::ENCODED_SIZE + ctLen, 0);  // >= plaintext cap
    const long m = recv.open(subOpt->seq, sealed.data(), rpc::kCryptoHeaderBytes,
                              sealed.data() + rpc::kCryptoHeaderBytes, ctLen,
                              storage.data() + rpc::MessageHeader::ENCODED_SIZE);
    if (m < 0) return std::nullopt;  // auth failure

    std::memcpy(storage.data(), sealed.data(), rpc::MessageHeader::ENCODED_SIZE);
    storage.resize(rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(m));
    return rpc::Records::decode(storage.data(), storage.size());
}

}  // namespace

TEST(GatewayCryptoDataPlane, RecordsPushSealOpensOnClient_Small) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/20);

    const std::vector<uint8_t> batch(64, 0xAB);  // single-fragment-sized
    const rpc::Records rec = makeRecordsFrame(20, 99, 5, 3, 1, 1000, batch);

    const auto sealed = sealRecordsAsPushWould(rec, *f.gw.send, f.gw.sendSeq, f.gw.cipher, f.gw.epoch);
    ASSERT_FALSE(sealed.empty()) << "seal must not fail in this test setup";
    EXPECT_EQ(f.gw.sendSeq, 1u);

    std::vector<uint8_t> storage;
    const auto opened = openRecordsAsClientWould(sealed, *f.clientRecv, storage);
    ASSERT_TRUE(opened.has_value()) << "a sealed Records frame must decrypt on the client";
    EXPECT_EQ(opened->sessionId, rec.sessionId);
    EXPECT_EQ(opened->correlationId, rec.correlationId);
    EXPECT_EQ(opened->subscriptionId, rec.subscriptionId);
    EXPECT_EQ(opened->topicId, rec.topicId);
    EXPECT_EQ(opened->partition, rec.partition);
    EXPECT_EQ(opened->baseOffset, rec.baseOffset);
    ASSERT_EQ(opened->recordBatch.size(), batch.size());
    EXPECT_EQ(0, std::memcmp(opened->recordBatch.data(), batch.data(), batch.size()));
}

TEST(GatewayCryptoDataPlane, RecordsPushSealOpensOnClient_Large) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/21);

    // Far larger than any realistic maxPayloadLength (~1376-8192 B MTU) —
    // the plaintext path would have taken the zero-copy vectored offer;
    // this proves the seal-level contiguous-assembly logic survives at that
    // size (the actual tryClaim/single-offer choice inside sendRecordsFrame
    // is box-deferred — Aeron-linked — but the seal step is identical
    // regardless of which publish call follows it).
    std::vector<uint8_t> batch(256 * 1024);
    for (std::size_t i = 0; i < batch.size(); ++i) batch[i] = static_cast<uint8_t>(i & 0xFF);
    const rpc::Records rec = makeRecordsFrame(21, 100, 6, 4, 2, 2000, batch);

    const auto sealed = sealRecordsAsPushWould(rec, *f.gw.send, f.gw.sendSeq, f.gw.cipher, f.gw.epoch);
    ASSERT_FALSE(sealed.empty());
    EXPECT_EQ(sealed.size(), rec.encodedSize() + rpc::kCryptoOverhead);

    std::vector<uint8_t> storage;
    const auto opened = openRecordsAsClientWould(sealed, *f.clientRecv, storage);
    ASSERT_TRUE(opened.has_value());
    ASSERT_EQ(opened->recordBatch.size(), batch.size());
    EXPECT_EQ(0, std::memcmp(opened->recordBatch.data(), batch.data(), batch.size()));
}

TEST(GatewayCryptoDataPlane, RecordsPushSealTamperedCiphertextDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/22);
    const std::vector<uint8_t> batch(128, 0x11);
    const rpc::Records rec = makeRecordsFrame(22, 1, 1, 1, 0, 0, batch);

    auto sealed = sealRecordsAsPushWould(rec, *f.gw.send, f.gw.sendSeq, f.gw.cipher, f.gw.epoch);
    ASSERT_FALSE(sealed.empty());
    sealed[rpc::kCryptoHeaderBytes] ^= 0xFF;  // flip one ciphertext byte

    std::vector<uint8_t> storage;
    const auto opened = openRecordsAsClientWould(sealed, *f.clientRecv, storage);
    EXPECT_FALSE(opened.has_value()) << "a tampered Records push must never decrypt";
}

TEST(GatewayCryptoDataPlane, RecordsPushSealMisconfiguredCtxFailsClosed) {
    // "encrypted but key not installed" — session.crypto.encrypted==true,
    // session.crypto.send has no value: pushRecords_ still builds a
    // RecordsSealCtx (gated on `encrypted` alone, mirroring sendTo), but
    // with a null `send`. sealRecordsFrame must fail closed, never
    // transmitting the frame in the clear.
    const std::vector<uint8_t> batch(32, 0x22);
    const rpc::Records rec = makeRecordsFrame(23, 1, 1, 1, 0, 0, batch);
    const std::size_t frameLen = rec.encodedSize();
    std::vector<uint8_t> plain(frameLen);
    rec.encode(plain.data());

    uint64_t sendSeq = 0;
    pn::RecordsSealCtx ctx;
    ctx.send    = nullptr;  // key not installed
    ctx.sendSeq = &sendSeq;

    std::vector<uint8_t> sealed(frameLen + rpc::kCryptoOverhead);
    const std::size_t n = pn::sealRecordsFrame(ctx, plain.data(), frameLen, sealed.data(), sealed.size());
    EXPECT_EQ(n, 0u);
    EXPECT_EQ(sendSeq, 0u) << "a failed seal must not even consume a sequence number";
}

// ---------------------------------------------------------------------------
// Task 19 (per-worker egress crypto addendum, 2026-10-08): a SHARD WORKER
// seals its OWN Records push with its OWN per-sender key
// (deriveSenderKey(b2c, workerId+1)), stamping senderId into the
// CryptoSubHeader (RecordsSealCtx::senderId). These cases mirror
// RecordsPushSealOpensOnClient_Small/Large above, but with a worker's
// derived key + senderId instead of the gateway's own b2c/senderId=0, and
// the CLIENT re-deriving the SAME per-sender key locally (exactly per the
// addendum's design — no key crosses the wire) to pick its recv cipher.
// ---------------------------------------------------------------------------

TEST(GatewayCryptoDataPlane, WorkerSealedRecordsOpenUnderDerivedPerSenderKey) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/30);

    constexpr uint8_t kWorkerId = 2;      // worker 2 -> senderId 3
    constexpr uint8_t kSenderId = kWorkerId + 1;

    // Broker side: the gateway derives this worker's key at SessionAnnounce
    // time from the session's raw b2c (SessionAnnouncer::announce).
    cr::AeadKey workerKey{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, workerKey));
    cr::AeadCipher workerSend(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    uint64_t workerSendSeq = 0;   // this worker's OWN counter — starts fresh, independent of the gateway's

    const std::vector<uint8_t> batch(96, 0x5A);
    const rpc::Records rec = makeRecordsFrame(30, 7, 2, 9, 1, 500, batch);
    const auto sealed = sealRecordsAsPushWould(rec, workerSend, workerSendSeq, f.gw.cipher,
                                                /*epoch=*/0, kSenderId);
    ASSERT_FALSE(sealed.empty());
    EXPECT_EQ(workerSendSeq, 1u);

    // The subheader carries the authenticated senderId (inside the AAD).
    const auto sub = rpc::CryptoSubHeader::decode(sealed.data() + rpc::MessageHeader::ENCODED_SIZE,
                                                   sealed.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(sub.has_value());
    EXPECT_EQ(sub->senderId, kSenderId);

    // Client side: re-derives the SAME per-sender key from ITS OWN b2c (the
    // same exporter secret, never a key sent over the wire) and picks it by
    // the clear senderId exactly as the addendum's client design describes.
    // (makeDataPlaneFixture already proved client b2c == gateway b2c via
    // HandshakeReachesDoneAndKeysAgree; re-deriving from f.gw.b2c here
    // stands in for "the client's own independent derivation".)
    cr::AeadKey derivedFromGatewayB2c{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, derivedFromGatewayB2c));
    EXPECT_EQ(0, std::memcmp(workerKey.bytes, derivedFromGatewayB2c.bytes, sizeof(workerKey.bytes)))
        << "deriveSenderKey must be a deterministic pure function of (b2c, senderId)";
    cr::AeadCipher clientRecvForWorker(f.gw.cipher, workerKey, f.gw.b2cPrefix);

    std::vector<uint8_t> storage;
    const auto opened = openRecordsAsClientWould(sealed, clientRecvForWorker, storage);
    ASSERT_TRUE(opened.has_value()) << "a worker-sealed Records frame must open under the "
                                        "client's independently-re-derived per-sender key";
    EXPECT_EQ(opened->sessionId, rec.sessionId);
    ASSERT_EQ(opened->recordBatch.size(), batch.size());
    EXPECT_EQ(0, std::memcmp(opened->recordBatch.data(), batch.data(), batch.size()));
}

TEST(GatewayCryptoDataPlane, TwoWorkersProduceDistinctKeysNeitherOpensUnderTheOther) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/31);

    cr::AeadKey key1{}, key2{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, /*senderId=*/1, key1));   // worker 0
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, /*senderId=*/2, key2));   // worker 1
    EXPECT_NE(0, std::memcmp(key1.bytes, key2.bytes, sizeof(key1.bytes)))
        << "two distinct senderIds must derive two distinct keys — the whole point: independent "
           "nonce spaces, no cross-sender AES-GCM nonce reuse";

    cr::AeadCipher send1(f.gw.cipher, key1, f.gw.b2cPrefix);
    cr::AeadCipher send2(f.gw.cipher, key2, f.gw.b2cPrefix);
    uint64_t seq1 = 0, seq2 = 0;

    const std::vector<uint8_t> batch(40, 0x7E);
    const rpc::Records rec1 = makeRecordsFrame(31, 1, 1, 1, 0, 0, batch);
    const rpc::Records rec2 = makeRecordsFrame(31, 1, 2, 1, 0, 0, batch);
    const auto sealed1 = sealRecordsAsPushWould(rec1, send1, seq1, f.gw.cipher, 0, /*senderId=*/1);
    const auto sealed2 = sealRecordsAsPushWould(rec2, send2, seq2, f.gw.cipher, 0, /*senderId=*/2);
    ASSERT_FALSE(sealed1.empty());
    ASSERT_FALSE(sealed2.empty());

    cr::AeadCipher recv1(f.gw.cipher, key1, f.gw.b2cPrefix);
    cr::AeadCipher recv2(f.gw.cipher, key2, f.gw.b2cPrefix);

    std::vector<uint8_t> storage;
    EXPECT_TRUE(openRecordsAsClientWould(sealed1, recv1, storage).has_value())
        << "worker 0's frame must open under worker 0's own derived key";
    EXPECT_TRUE(openRecordsAsClientWould(sealed2, recv2, storage).has_value())
        << "worker 1's frame must open under worker 1's own derived key";

    // Cross-sender: worker 0's frame must NOT open under worker 1's key, and
    // vice versa (even though both frames carry a correctly-authenticated
    // CryptoSubHeader — they are simply sealed under a different key).
    EXPECT_FALSE(openRecordsAsClientWould(sealed1, recv2, storage).has_value())
        << "worker 0's frame must never open under worker 1's key";
    EXPECT_FALSE(openRecordsAsClientWould(sealed2, recv1, storage).has_value())
        << "worker 1's frame must never open under worker 0's key";
}

TEST(GatewayCryptoDataPlane, WorkerSealedRecordsWrongSenderIdTamperDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/32);
    cr::AeadKey key{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, /*senderId=*/5, key));
    cr::AeadCipher send(f.gw.cipher, key, f.gw.b2cPrefix);
    uint64_t seq = 0;

    const std::vector<uint8_t> batch(16, 0x33);
    const rpc::Records rec = makeRecordsFrame(32, 1, 1, 1, 0, 0, batch);
    auto sealed = sealRecordsAsPushWould(rec, send, seq, f.gw.cipher, 0, /*senderId=*/5);
    ASSERT_FALSE(sealed.empty());

    // Flip the senderId byte in the (authenticated) subheader — senderId is
    // inside the AAD, so this must fail auth on open, never silently pick a
    // different key and "succeed" (the addendum's "no redirection vector"
    // security note).
    sealed[rpc::MessageHeader::ENCODED_SIZE + 3 /* high byte of the epoch/senderId u16 */] ^= 0xFF;

    cr::AeadCipher recv(f.gw.cipher, key, f.gw.b2cPrefix);
    std::vector<uint8_t> storage;
    EXPECT_FALSE(openRecordsAsClientWould(sealed, recv, storage).has_value())
        << "a forged senderId byte (inside the AAD) must fail authentication, never be silently "
           "accepted under the original key";
}

TEST(GatewayCryptoDataPlane, WorkerDeriveSenderKeyZeroEqualsGatewayB2c) {
    // deriveSenderKey's documented contract: senderId==0 => out == b2c
    // UNCHANGED (the gateway's own id) — never a change to the
    // already-shipped gateway/client seal/open path.
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/33);
    cr::AeadKey out{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, /*senderId=*/0, out));
    EXPECT_EQ(0, std::memcmp(out.bytes, f.gw.b2c.bytes, sizeof(out.bytes)));
}

// ===========================================================================
// Task 22 (shard worker decrypts produce / per-worker ingress, 2026-10-08):
// rune::node::openProduceFrame (RecordsOpen.h) is the raw-crypto-params OPEN
// step a shard worker's handleWorkerIngressFrame_ calls BEFORE
// rune::protocol::client::Produce::decode, for a produce frame the client
// sealed under the session's c2b cipher (the SAME key the gateway uses to
// open the client's control frames — ingress has a single sender, no
// per-worker derivation, unlike the b2c egress key). Mirrors
// rune::gateway::codec::openIngressFrame's cases (5/7/8/9/10/11 above) one
// for one, but against a worker-owned RecordsOpenCtx instead of the
// gateway's own CryptoState, AND proves the reconstructed plaintext
// actually decodes via the real rune::protocol::client::Produce::decode —
// the "feed the EXISTING decode/produce path unchanged" requirement.
// ===========================================================================

namespace {

// Build + encode a real Produce message exactly the shape a client would
// send (full [24B MessageHeader][body] frame via Produce::encode) — used as
// the `plain` input to sealFrameAsPeer so the sealed wire frame is a
// genuine Produce frame, not just header-shaped bytes.
std::vector<uint8_t> encodeProduceFrame(int64_t sessionId, int64_t correlationId,
                                        int32_t topicId, int32_t partition,
                                        int64_t producerId, int16_t producerEpoch,
                                        int32_t baseSequence, uint8_t acks,
                                        const std::vector<uint8_t>& batch) {
    rpc::Produce p;
    p.sessionId      = sessionId;
    p.correlationId  = correlationId;
    p.topicId        = topicId;
    p.partition      = partition;
    p.partitionKnown = 1;
    p.acks           = acks;
    p.producerId     = producerId;
    p.producerEpoch  = producerEpoch;
    p.baseSequence   = baseSequence;
    p.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> out(p.encodedSize());
    p.encode(out.data());
    return out;
}

// A worker's own per-session ingress crypto state, built the way
// executeSessionAnnounceWire_ builds WorkerSessionCrypto::recv/recvSeq/
// recvWindow from the SessionAnnounceCryptoTail's c2b fields — but using
// the fixture's raw c2b bytes directly (standing in for "the gateway
// shipped this worker the real c2b key").  A SEPARATE recvSeq/recvWindow
// instance per test/worker — never shared with the gateway's own
// `f.gw.recvSeq`/`recvWindow` (the "own replay window" invariant).
struct WorkerIngressFixture {
    std::optional<cr::AeadCipher> recv;
    uint64_t recvSeq    = 0;
    uint64_t recvWindow = 0;

    pn::RecordsOpenCtx ctx() {
        pn::RecordsOpenCtx c;
        c.recv       = recv.has_value() ? &*recv : nullptr;
        c.recvSeq    = &recvSeq;
        c.recvWindow = &recvWindow;
        return c;
    }
};

WorkerIngressFixture makeWorkerIngressFixture(const DataPlaneFixture& f) {
    WorkerIngressFixture w;
    w.recv.emplace(f.gw.cipher, f.gw.c2b, f.gw.c2bPrefix);
    return w;
}

}  // namespace

TEST(GatewayCryptoDataPlane, WorkerIngressOpensClientSealedProduceAndDecodes) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/40);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);

    const std::vector<uint8_t> batch(48, 0x9C);
    const auto plain = encodeProduceFrame(/*sessionId=*/40, /*correlationId=*/5,
                                           /*topicId=*/11, /*partition=*/2,
                                           /*producerId=*/-1, /*producerEpoch=*/0,
                                           /*baseSequence=*/-1, /*acks=*/1, batch);
    const auto wire = sealFrameAsPeer(*f.clientSend, /*seq=*/1, plain);

    pn::RecordsOpenCtx ctx = w.ctx();
    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    ASSERT_EQ(n, plain.size());
    EXPECT_EQ(w.recvSeq, 1u);
    EXPECT_NE(w.recvWindow & 1u, 0u);

    // Feed the reconstructed [24B header][plaintext body] into the EXACT
    // decode function handleWorkerIngressFrame_ calls — proving it decodes
    // to the right Produce, not just that the bytes round-trip.
    const auto msgOpt = rpc::Produce::decode(scratch.data(), n);
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId, 40);
    EXPECT_EQ(msgOpt->correlationId, 5);
    EXPECT_EQ(msgOpt->topicId, 11);
    EXPECT_EQ(msgOpt->partition, 2);
    EXPECT_EQ(msgOpt->acks, 1);
    ASSERT_EQ(msgOpt->recordBatch.size(), batch.size());
    EXPECT_EQ(0, std::memcmp(msgOpt->recordBatch.data(), batch.data(), batch.size()));
}

TEST(GatewayCryptoDataPlane, WorkerIngressTamperedCiphertextDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/41);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);

    const auto plain = encodeProduceFrame(41, 1, 1, 0, -1, 0, -1, 0, {0x01, 0x02});
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);
    wire[rpc::kCryptoHeaderBytes] ^= 0xFF;  // flip one ciphertext byte

    pn::RecordsOpenCtx ctx = w.ctx();
    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a tampered produce frame must never decode as plaintext";
    EXPECT_EQ(w.recvSeq, 0u) << "auth failed before commit — window untouched";
    EXPECT_EQ(w.recvWindow, 0u);
}

TEST(GatewayCryptoDataPlane, WorkerIngressTamperedHeaderAadDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/42);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);

    const auto plain = encodeProduceFrame(42, 1, 1, 0, -1, 0, -1, 0, {0x03});
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);
    wire[0] ^= 0xFF;  // flip a clear MessageHeader byte — it is part of the AAD

    pn::RecordsOpenCtx ctx = w.ctx();
    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a tampered clear header must fail auth, never be accepted";
}

TEST(GatewayCryptoDataPlane, WorkerIngressReplayedSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/43);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);

    const auto plain = encodeProduceFrame(43, 1, 1, 0, -1, 0, -1, 0, {0x04});
    const auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    pn::RecordsOpenCtx ctx = w.ctx();
    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n1 = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    ASSERT_EQ(n1, plain.size());

    // Replay the EXACT same wire frame — must be rejected the second time.
    const std::size_t n2 = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n2, 0u) << "a replayed seq must be dropped";
}

TEST(GatewayCryptoDataPlane, WorkerIngressTooOldSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/44);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);
    pn::RecordsOpenCtx ctx = w.ctx();

    // Advance the window far ahead (seq=100), then try seq=1 — more than 64
    // behind the high-water mark.
    const auto plainAhead = encodeProduceFrame(44, 1, 1, 0, -1, 0, -1, 0, {0x05});
    const auto wireAhead  = sealFrameAsPeer(*f.clientSend, 100, plainAhead);
    std::vector<uint8_t> scratch(wireAhead.size());
    ASSERT_GT(pn::openProduceFrame(ctx, wireAhead.data(), wireAhead.size(), scratch.data(), scratch.size()), 0u);
    ASSERT_EQ(w.recvSeq, 100u);

    const auto plainOld = encodeProduceFrame(44, 2, 1, 0, -1, 0, -1, 0, {0x06});
    const auto wireOld  = sealFrameAsPeer(*f.clientSend, 1, plainOld);
    scratch.assign(wireOld.size(), 0);
    const std::size_t n = pn::openProduceFrame(ctx, wireOld.data(), wireOld.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a seq more than 64 behind the high-water mark must be dropped as too old";
}

TEST(GatewayCryptoDataPlane, WorkerIngressWindowNotPoisonedByForgedFrame) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/45);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);
    pn::RecordsOpenCtx ctx = w.ctx();

    // A forged frame (sealed under the WRONG key) claiming seq=5 must fail
    // auth and NOT poison the window — a later, legitimately-sealed seq=5
    // frame must still be accepted.
    cr::AeadCipher wrongKeySend(f.gw.cipher, f.gw.b2c /* wrong key: b2c, not c2b */, f.gw.c2bPrefix);
    const auto forgedPlain = encodeProduceFrame(45, 1, 1, 0, -1, 0, -1, 0, {0x07});
    const auto forgedWire  = sealFrameAsPeer(wrongKeySend, 5, forgedPlain);
    std::vector<uint8_t> scratch(forgedWire.size());
    const std::size_t nForged = pn::openProduceFrame(ctx, forgedWire.data(), forgedWire.size(),
                                                       scratch.data(), scratch.size());
    EXPECT_EQ(nForged, 0u);
    EXPECT_EQ(w.recvSeq, 0u) << "a forged frame must never advance the window";

    const auto legitPlain = encodeProduceFrame(45, 2, 1, 0, -1, 0, -1, 0, {0x08});
    const auto legitWire  = sealFrameAsPeer(*f.clientSend, 5, legitPlain);
    scratch.assign(legitWire.size(), 0);
    const std::size_t nLegit = pn::openProduceFrame(ctx, legitWire.data(), legitWire.size(),
                                                      scratch.data(), scratch.size());
    EXPECT_EQ(nLegit, legitPlain.size())
        << "the legitimate seq=5 frame must still succeed — the window must not have been "
           "poisoned by the forged frame that never authenticated";
}

TEST(GatewayCryptoDataPlane, WorkerIngressNoKeyYetFailsClosed) {
    // Pre-key / unknown-session case: the worker has a sessionCrypto_ entry
    // (sessionEncrypted known from the first announce wave) but no real c2b
    // key installed yet (recv == nullopt). openProduceFrame must fail
    // closed — NEVER fall back to decoding the ciphertext as plaintext.
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/46);
    const auto plain = encodeProduceFrame(46, 1, 1, 0, -1, 0, -1, 0, {0x09});
    const auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    uint64_t recvSeq = 0, recvWindow = 0;
    pn::RecordsOpenCtx ctx;
    ctx.recv       = nullptr;   // no key installed yet
    ctx.recvSeq    = &recvSeq;
    ctx.recvWindow = &recvWindow;

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = pn::openProduceFrame(ctx, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "no key installed yet must fail closed, never decode as plaintext";
    EXPECT_EQ(recvSeq, 0u) << "a failed open must not even consume a sequence number";
}

TEST(GatewayCryptoDataPlane, WorkerIngressOwnWindowIndependentOfGatewayWindow) {
    // Nonce/replay note: the client seals EVERY client->broker frame under
    // ONE c2b cipher + a single monotonic sendSeq, so produce (worker-bound)
    // and control (gateway-bound) frames never collide in seq space — but
    // each receiver keeps its OWN replay window and sees only its subset of
    // the stream (forward gaps where the other's frames were). Simulate:
    // seq=1 goes to the gateway (opened on f.gw), seq=2 goes to the worker
    // (opened on w) — the worker's window must accept seq=2 even though it
    // never saw seq=1 at all (a forward gap, never shared with the gateway's
    // own window).
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/47);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);

    const auto controlPlain = encodeProduceFrame(47, 1, 1, 0, -1, 0, -1, 0, {0x0A});
    const auto controlWire  = sealFrameAsPeer(*f.clientSend, 1, controlPlain);
    std::vector<uint8_t> gwScratch(controlWire.size());
    const std::size_t nGw = rune::gateway::codec::openIngressFrame(
        f.gw, controlWire.data(), controlWire.size(), gwScratch.data(), gwScratch.size());
    ASSERT_GT(nGw, 0u);
    EXPECT_EQ(f.gw.recvSeq, 1u);
    EXPECT_EQ(w.recvSeq, 0u) << "the worker's window must be untouched by gateway-bound traffic";

    const auto producePlain = encodeProduceFrame(47, 2, 1, 0, -1, 0, -1, 0, {0x0B});
    const auto produceWire  = sealFrameAsPeer(*f.clientSend, 2, producePlain);
    pn::RecordsOpenCtx ctx = w.ctx();
    std::vector<uint8_t> wScratch(produceWire.size());
    const std::size_t nW = pn::openProduceFrame(ctx, produceWire.data(), produceWire.size(),
                                                 wScratch.data(), wScratch.size());
    EXPECT_GT(nW, 0u) << "the worker must accept seq=2 despite never having seen seq=1 "
                          "(a forward gap where the gateway-bound frame was) — never a shared window";
    EXPECT_EQ(w.recvSeq, 2u);
    EXPECT_EQ(f.gw.recvSeq, 1u) << "the gateway's own window must be untouched by worker-bound traffic";
}

// ---------------------------------------------------------------------------
// RED-evidence toggle for openProduceFrame (RecordsOpen.h): a LOCAL,
// deliberately-buggy copy that commits the replay window BEFORE checking
// the AEAD auth result, instead of after — mirrors
// WindowNotPoisonedByForgedFrame_RedToggle above but for the worker-ingress
// open step. Always `false` in the committed file; see task-22-report.md
// for the RED run (flip to `true` locally to reproduce).
// ---------------------------------------------------------------------------
namespace {
constexpr bool kInjectWorkerIngressCommitBeforeAuthBug = false;

// Reuses the REAL replayWindowCheck/Commit (rune::node::detail_open) so this
// isolates EXACTLY the ordering bug — the check is identical to the
// production function; only the COMMIT is moved to before open() instead of
// after.
std::size_t openProduceFrameBuggy(const pn::RecordsOpenCtx& ctx, const uint8_t* wire,
                                   std::size_t wireLen, uint8_t* scratch,
                                   std::size_t scratchCap) noexcept {
    namespace dc = rune::node::detail_open;
    if (!ctx.recv || !ctx.recvSeq || !ctx.recvWindow) return 0;
    constexpr std::size_t kMinWire = rpc::kCryptoHeaderBytes + rune::crypto::kTagSize;
    if (wireLen < kMinWire) return 0;
    const auto subOpt = rpc::CryptoSubHeader::decode(
        wire + rpc::MessageHeader::ENCODED_SIZE, wireLen - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return 0;
    const uint64_t seq = subOpt->seq;

    if (!dc::replayWindowCheck(*ctx.recvSeq, *ctx.recvWindow, seq)) return 0;

    // BUG: commit the window before authenticating — a forged seq that
    // passes the (pre-auth) check now poisons the window even though the
    // frame is about to be rejected for failing the AEAD tag.
    dc::replayWindowCommit(*ctx.recvSeq, *ctx.recvWindow, seq);

    const std::size_t ctLen    = wireLen - rpc::kCryptoHeaderBytes;
    const std::size_t plainCap = ctLen - rune::crypto::kTagSize;
    const std::size_t outLen   = rpc::MessageHeader::ENCODED_SIZE + plainCap;
    if (scratchCap < outLen) return 0;
    const long plainLen = ctx.recv->open(seq, wire, rpc::kCryptoHeaderBytes,
                                          wire + rpc::kCryptoHeaderBytes, ctLen,
                                          scratch + rpc::MessageHeader::ENCODED_SIZE);
    if (plainLen < 0) return 0;
    std::memcpy(scratch, wire, rpc::MessageHeader::ENCODED_SIZE);
    return rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(plainLen);
}
}  // namespace

TEST(GatewayCryptoDataPlane, WorkerIngressWindowNotPoisonedByForgedFrame_RedToggle) {
    if (!kInjectWorkerIngressCommitBeforeAuthBug) {
        GTEST_SKIP() << "RED-evidence toggle off in the committed file (see task-22-report.md)";
    }
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/48);
    WorkerIngressFixture w = makeWorkerIngressFixture(f);
    pn::RecordsOpenCtx ctx = w.ctx();

    const auto plain = encodeProduceFrame(48, 1, 1, 0, -1, 0, -1, 0, {0x0C});
    const uint8_t fakePrefix[cr::kNoncePrefix] = {1, 2, 3, 4};
    cr::AeadCipher wrongKeyCipher(f.gw.cipher, cr::AeadKey{}, fakePrefix);
    const auto forged = sealFrameAsPeer(wrongKeyCipher, /*seq=*/5, plain);

    std::vector<uint8_t> scratch(forged.size());
    const std::size_t nForged = openProduceFrameBuggy(
        ctx, forged.data(), forged.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nForged, 0u);

    // With the bug, the window was already poisoned to seq=5 — the
    // legitimate seq=5 frame is now wrongly rejected as a replay.
    const auto legit = sealFrameAsPeer(*f.clientSend, /*seq=*/5, plain);
    scratch.assign(legit.size(), 0);
    const std::size_t nLegit = openProduceFrameBuggy(
        ctx, legit.data(), legit.size(), scratch.data(), scratch.size());
    EXPECT_EQ(nLegit, plain.size()) << "expected to FAIL under the injected bug (RED evidence)";
}

// ---------------------------------------------------------------------------
// Task 19 + Task 22: SessionAnnounceKeyTail.h — the
// [cipher|b2cPrefix|b2cKey|c2bPrefix|c2bKey|channel] byte layout Worker.h's
// enqueueOp/executeSessionAnnounceWire_ ride on the SessionAnnounce op's
// EXISTING variable tail (no new SBE field — see that header's file
// comment). Task 22 extended the tail to ALSO carry the session's c2b
// ingress key/prefix alongside Task 19's b2c egress key/prefix — these
// round-trip tests now cover both. Pure byte-format functions, zero
// Aeron/crypto dependency: unit-tested standalone here, same TU as the rest
// of this file's aeron-free crypto coverage.
// ---------------------------------------------------------------------------

TEST(SessionAnnounceKeyTail, EncodeDecodeRoundTrips) {
    uint8_t b2cPrefix[4] = {0x11, 0x22, 0x33, 0x44};
    uint8_t b2cKey[32];
    for (int i = 0; i < 32; ++i) b2cKey[static_cast<std::size_t>(i)] = static_cast<uint8_t>(i);
    // Task 22: the tail now ALSO carries the session's c2b ingress key +
    // prefix — distinct bytes from b2c, so a round trip cannot accidentally
    // pass by aliasing the two fields.
    uint8_t c2bPrefix[4] = {0x55, 0x66, 0x77, 0x88};
    uint8_t c2bKey[32];
    for (int i = 0; i < 32; ++i) c2bKey[static_cast<std::size_t>(i)] = static_cast<uint8_t>(31 - i);
    const std::string channel = "aeron:udp?endpoint=127.0.0.1:9999";

    std::vector<uint8_t> buf(rune::node::kSessionAnnounceCryptoHeaderBytes + channel.size());
    const std::size_t n = rune::node::encodeSessionAnnounceCryptoTail(
        buf.data(), buf.size(), /*cipher=*/static_cast<uint8_t>(cr::Cipher::Aes256Gcm),
        b2cPrefix, b2cKey, c2bPrefix, c2bKey, channel.data(), channel.size());
    ASSERT_EQ(n, buf.size());

    const auto decoded = rune::node::decodeSessionAnnounceCryptoTail(
        reinterpret_cast<const char*>(buf.data()), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->cipher, static_cast<uint8_t>(cr::Cipher::Aes256Gcm));
    EXPECT_EQ(0, std::memcmp(decoded->b2cPrefix, b2cPrefix, 4));
    EXPECT_EQ(0, std::memcmp(decoded->b2cKey, b2cKey, 32));
    EXPECT_EQ(0, std::memcmp(decoded->c2bPrefix, c2bPrefix, 4));
    EXPECT_EQ(0, std::memcmp(decoded->c2bKey, c2bKey, 32));
    ASSERT_EQ(decoded->channel.size(), channel.size());
    EXPECT_EQ(decoded->channel, channel);
}

TEST(SessionAnnounceKeyTail, EncodeDecodeRoundTripsWithEmptyChannel) {
    uint8_t b2cPrefix[4] = {0, 0, 0, 0};
    uint8_t b2cKey[32] = {};
    uint8_t c2bPrefix[4] = {0, 0, 0, 0};
    uint8_t c2bKey[32] = {};
    std::vector<uint8_t> buf(rune::node::kSessionAnnounceCryptoHeaderBytes);
    const std::size_t n = rune::node::encodeSessionAnnounceCryptoTail(
        buf.data(), buf.size(), 1, b2cPrefix, b2cKey, c2bPrefix, c2bKey, nullptr, 0);
    ASSERT_EQ(n, rune::node::kSessionAnnounceCryptoHeaderBytes);
    const auto decoded = rune::node::decodeSessionAnnounceCryptoTail(
        reinterpret_cast<const char*>(buf.data()), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->channel.empty());
}

TEST(SessionAnnounceKeyTail, EncodeFailsClosedWhenBufferTooSmall) {
    uint8_t b2cPrefix[4] = {}, b2cKey[32] = {}, c2bPrefix[4] = {}, c2bKey[32] = {};
    std::vector<uint8_t> tooSmall(rune::node::kSessionAnnounceCryptoHeaderBytes - 1);
    const std::size_t n = rune::node::encodeSessionAnnounceCryptoTail(
        tooSmall.data(), tooSmall.size(), 1, b2cPrefix, b2cKey, c2bPrefix, c2bKey, "x", 1);
    EXPECT_EQ(n, 0u) << "must never write a truncated crypto header — 0 means 'do not enqueue this'";
}

TEST(SessionAnnounceKeyTail, DecodeFailsClosedOnTruncatedTail) {
    // Shorter than the fixed header: must be nullopt, NEVER parsed as if it
    // were a (garbage) key — a worker must park, never install from this.
    std::vector<uint8_t> truncated(rune::node::kSessionAnnounceCryptoHeaderBytes - 1, 0xAB);
    const auto decoded = rune::node::decodeSessionAnnounceCryptoTail(
        reinterpret_cast<const char*>(truncated.data()), truncated.size());
    EXPECT_FALSE(decoded.has_value());
}

// ---------------------------------------------------------------------------
// Task 23 — shard worker SEALS produce acks (SessionAckSink::flushAcks_).
//
// SessionAckSink.h itself cannot be built on this host (it unconditionally
// includes <Aeron.h>/<ExclusivePublication.h> — see the file header's
// BUILD/TEST SPLIT note), so these tests exercise the exact same production
// primitives flushAcks_'s new encrypted branch calls — ProduceResponse::
// encodeFor + rune::node::sealRecordsFrame — standing in for the sink's own
// claim()/encode lambda the way sealRecordsAsPushWould already stands in for
// sendRecordsFrame's encrypted branch above. Field-for-field match against
// SessionAckSink.h's flushAcks_: encode_(a, ver, plainScratch) then
// sealRecordsFrame(*sealCtx, plainScratch, plainLen, sealedScratch, cap).
// ---------------------------------------------------------------------------
namespace {

rpc::ProduceResponse makeProduceResponse(int64_t sessionId, int64_t correlationId,
                                          int32_t partition, int64_t baseOffset,
                                          int64_t logStartOffset = -1) {
    rpc::ProduceResponse r;
    r.sessionId      = sessionId;
    r.correlationId  = correlationId;
    r.errorCode      = 0;
    r.partition       = partition;
    r.baseOffset      = baseOffset;
    r.logStartOffset  = logStartOffset;
    return r;
}

// Seal `resp` exactly the way SessionAckSink::flushAcks_'s ENCRYPTED branch
// does: encodeFor() into a fixed plaintext scratch, then
// rune::node::sealRecordsFrame() into a sealed buffer. Returns the sealed
// bytes (empty on seal failure — fail-closed, never transmit).
std::vector<uint8_t> sealAckAsFlushWould(const rpc::ProduceResponse& resp, int32_t clientVersion,
                                         cr::AeadCipher& send, uint64_t& sendSeq, cr::Cipher cipher,
                                         uint8_t senderId) {
    const std::size_t plainLen = rpc::ProduceResponse::encodedSizeFor(clientVersion);
    std::vector<uint8_t> plain(plainLen);
    resp.encodeFor(plain.data(), clientVersion);

    pn::RecordsSealCtx ctx;
    ctx.send     = &send;
    ctx.sendSeq  = &sendSeq;
    ctx.cipher   = static_cast<uint8_t>(cipher);
    ctx.senderId = senderId;

    std::vector<uint8_t> sealed(plainLen + rpc::kCryptoOverhead);
    const std::size_t n = pn::sealRecordsFrame(ctx, plain.data(), plainLen, sealed.data(), sealed.size());
    sealed.resize(n);   // n==0 -> empty, signalling seal failure to the caller
    return sealed;
}

// Open `sealed` the way the client opens ANY worker-sender frame (acks and
// Records both open under the same per-sender key, Task 20) and decode it
// as a ProduceResponse. nullopt on ANY failure (truncation, auth failure).
std::optional<rpc::ProduceResponse> openProduceResponseAsClientWould(
    const std::vector<uint8_t>& sealed, cr::AeadCipher& recv, std::vector<uint8_t>& storage) {
    if (sealed.size() < rpc::kCryptoHeaderBytes + cr::kTagSize) return std::nullopt;
    const auto subOpt = rpc::CryptoSubHeader::decode(
        sealed.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealed.size() - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return std::nullopt;

    const std::size_t ctLen = sealed.size() - rpc::kCryptoHeaderBytes;
    storage.assign(rpc::MessageHeader::ENCODED_SIZE + ctLen, 0);
    const long m = recv.open(subOpt->seq, sealed.data(), rpc::kCryptoHeaderBytes,
                              sealed.data() + rpc::kCryptoHeaderBytes, ctLen,
                              storage.data() + rpc::MessageHeader::ENCODED_SIZE);
    if (m < 0) return std::nullopt;

    std::memcpy(storage.data(), sealed.data(), rpc::MessageHeader::ENCODED_SIZE);
    storage.resize(rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(m));
    return rpc::ProduceResponse::decode(storage.data(), storage.size());
}

}  // namespace

TEST(GatewayCryptoDataPlane, WorkerAckSealedProduceResponseOpensUnderWorkerSenderKey) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/40);

    constexpr uint8_t kWorkerId = 3;      // worker 3 -> senderId 4
    constexpr uint8_t kSenderId = kWorkerId + 1;

    cr::AeadKey workerKey{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, workerKey));
    cr::AeadCipher workerSend(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    uint64_t workerSendSeq = 0;

    const rpc::ProduceResponse resp = makeProduceResponse(40, 55, 2, 1000);
    const auto sealed = sealAckAsFlushWould(resp, /*clientVersion=*/2, workerSend, workerSendSeq,
                                            f.gw.cipher, kSenderId);
    ASSERT_FALSE(sealed.empty());
    EXPECT_EQ(workerSendSeq, 1u);

    const auto sub = rpc::CryptoSubHeader::decode(sealed.data() + rpc::MessageHeader::ENCODED_SIZE,
                                                   sealed.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(sub.has_value());
    EXPECT_EQ(sub->senderId, kSenderId);

    cr::AeadCipher clientRecvForWorker(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    std::vector<uint8_t> storage;
    const auto opened = openProduceResponseAsClientWould(sealed, clientRecvForWorker, storage);
    ASSERT_TRUE(opened.has_value()) << "a worker-sealed ack must open under the client's "
                                        "independently-re-derived per-sender key";
    EXPECT_EQ(opened->sessionId, resp.sessionId);
    EXPECT_EQ(opened->correlationId, resp.correlationId);
    EXPECT_EQ(opened->baseOffset, resp.baseOffset);
    EXPECT_EQ(opened->partition, resp.partition);

    // Tamper: flip a ciphertext byte — auth must fail (AeadCipher::open's own
    // tag-verification contract; mirrors WorkerSealedRecordsWrongSenderIdTamperDropped's
    // tamper check for Records). Replay rejection is a property of the
    // CALLER's anti-replay WINDOW (RecordsOpen.h / CryptoState.recvWindow),
    // not of AeadCipher::open itself (a pure decrypt+verify primitive, no
    // internal seq memory) — out of scope for a broker->client ack open
    // (the client side takes no change in this task), same as the existing
    // Records open helper above never asserts replay either.
    std::vector<uint8_t> tampered = sealed;
    tampered.back() ^= 0x01;
    std::vector<uint8_t> storage2;
    cr::AeadCipher clientRecvForWorker2(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    EXPECT_FALSE(openProduceResponseAsClientWould(tampered, clientRecvForWorker2, storage2).has_value());
}

TEST(GatewayCryptoDataPlane, WorkerAckAndRecordsShareOneMonotonicSendSeq) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/41);

    constexpr uint8_t kWorkerId = 1;      // worker 1 -> senderId 2
    constexpr uint8_t kSenderId = kWorkerId + 1;

    cr::AeadKey workerKey{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, workerKey));
    cr::AeadCipher workerSend(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    // ★★ ONE shared counter — exactly Worker.h's WorkerSessionCrypto::sendSeq,
    // which pushOwnedSubscriptions_ (Records) and the ackSink_->flush
    // crypto-lookup (acks, this task) both pass a pointer to THE SAME field.
    uint64_t sharedSendSeq = 0;

    const std::vector<uint8_t> batch(32, 0x11);
    const rpc::Records rec = makeRecordsFrame(41, 1, 1, 1, 0, 0, batch);
    const auto sealedRecords = sealRecordsAsPushWould(rec, workerSend, sharedSendSeq, f.gw.cipher,
                                                       /*epoch=*/0, kSenderId);
    ASSERT_FALSE(sealedRecords.empty());

    const rpc::ProduceResponse resp = makeProduceResponse(41, 2, 0, 500);
    const auto sealedAck = sealAckAsFlushWould(resp, /*clientVersion=*/2, workerSend, sharedSendSeq,
                                               f.gw.cipher, kSenderId);
    ASSERT_FALSE(sealedAck.empty());

    const auto subRecords = rpc::CryptoSubHeader::decode(
        sealedRecords.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealedRecords.size() - rpc::MessageHeader::ENCODED_SIZE);
    const auto subAck = rpc::CryptoSubHeader::decode(
        sealedAck.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealedAck.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(subRecords.has_value());
    ASSERT_TRUE(subAck.has_value());
    EXPECT_EQ(subRecords->seq, 1u);
    EXPECT_EQ(subAck->seq, 2u);
    EXPECT_NE(subRecords->seq, subAck->seq)
        << "Records and the ack must NEVER share a seq under the same key (nonce reuse)";

    // Both open correctly under the client's independently-re-derived key —
    // one key, two frame types, one monotonic seq stream.
    cr::AeadCipher clientRecv1(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    std::vector<uint8_t> storage1;
    const auto openedRecords = openRecordsAsClientWould(sealedRecords, clientRecv1, storage1);
    ASSERT_TRUE(openedRecords.has_value());
    EXPECT_EQ(openedRecords->sessionId, rec.sessionId);

    const long mAck = clientRecv1.open(subAck->seq, sealedAck.data(), rpc::kCryptoHeaderBytes,
                                        sealedAck.data() + rpc::kCryptoHeaderBytes,
                                        sealedAck.size() - rpc::kCryptoHeaderBytes,
                                        storage1.data());   // scratch reuse, only need success/failure here
    EXPECT_GE(mAck, 0) << "the ack must open on the SAME per-sender key right after Records did, "
                           "continuing that key's one seq stream";
}

// ---------------------------------------------------------------------------
// RED-evidence toggle: if the ack sink were given its OWN independent
// sendSeq counter (instead of sharing Worker.h's WorkerSessionCrypto::
// sendSeq with Records, as flushAcks_'s new encrypted branch and the
// ackSink_->flush crypto-lookup in Worker.h both do), a Records frame and an
// ack sealed back-to-back under the same key would claim the SAME seq — an
// AES-GCM nonce collision. Always `false` in the committed file.
// ---------------------------------------------------------------------------
namespace {
constexpr bool kInjectSeparateAckCounterBug = false;
}

TEST(GatewayCryptoDataPlane, WorkerAckAndRecordsSeparateCounterCollides_RedToggle) {
    if (!kInjectSeparateAckCounterBug) {
        GTEST_SKIP() << "RED-evidence toggle off in the committed file (see task-23-report.md)";
    }
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/42);
    constexpr uint8_t kSenderId = 2;
    cr::AeadKey workerKey{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, workerKey));
    cr::AeadCipher workerSend(f.gw.cipher, workerKey, f.gw.b2cPrefix);

    // BUG: two INDEPENDENT counters instead of one shared WorkerSessionCrypto::sendSeq.
    uint64_t recordsSeq = 0;
    uint64_t ackSeqOwnCounter = 0;

    const std::vector<uint8_t> batch(16, 0x22);
    const rpc::Records rec = makeRecordsFrame(42, 1, 1, 1, 0, 0, batch);
    const auto sealedRecords = sealRecordsAsPushWould(rec, workerSend, recordsSeq, f.gw.cipher, 0, kSenderId);
    const rpc::ProduceResponse resp = makeProduceResponse(42, 2, 0, 0);
    const auto sealedAck = sealAckAsFlushWould(resp, 2, workerSend, ackSeqOwnCounter, f.gw.cipher, kSenderId);
    ASSERT_FALSE(sealedRecords.empty());
    ASSERT_FALSE(sealedAck.empty());

    const auto subRecords = rpc::CryptoSubHeader::decode(
        sealedRecords.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealedRecords.size() - rpc::MessageHeader::ENCODED_SIZE);
    const auto subAck = rpc::CryptoSubHeader::decode(
        sealedAck.data() + rpc::MessageHeader::ENCODED_SIZE,
        sealedAck.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(subRecords.has_value());
    ASSERT_TRUE(subAck.has_value());
    // Expected to FAIL under the injected bug (RED evidence): both frames
    // claim seq=1 under the SAME key — a nonce collision.
    EXPECT_NE(subRecords->seq, subAck->seq)
        << "expected to FAIL under the injected bug (RED evidence): a separate ack counter "
           "collides with Records' seq under the one worker sender key";
}

// ---------------------------------------------------------------------------
// RED-evidence toggle: reproduces the ORIGINAL Task-8b bug (Records pushed
// in the clear on an encrypted session — pushRecords_ -> sendRecordsFrame
// wrote directly to the raw publication, bypassing sendTo's Task 8
// sealing). Always `false` in the committed file; see task-8b-report.md for
// the RED run (flip to `true` locally to reproduce).
// ---------------------------------------------------------------------------
namespace {
constexpr bool kInjectPlaintextRecordsPushBug = false;
}

TEST(GatewayCryptoDataPlane, RecordsPushPlaintextBug_RedToggle) {
    if (!kInjectPlaintextRecordsPushBug) {
        GTEST_SKIP() << "RED-evidence toggle off in the committed file (see task-8b-report.md)";
    }
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/24);
    const std::vector<uint8_t> batch(64, 0xCD);
    const rpc::Records rec = makeRecordsFrame(24, 1, 1, 1, 0, 0, batch);

    // BUG: the pre-fix pushRecords_ sent rec.encode()'s PLAIN bytes straight
    // to the publication — no seal at all — even though the session is
    // encrypted. The client's dispatchEgressFrame decrypts EVERY non-
    // handshake egress frame unconditionally, so it tries to open these
    // plain bytes as if they were sealed.
    const std::size_t frameLen = rec.encodedSize();
    std::vector<uint8_t> plainWire(frameLen);
    rec.encode(plainWire.data());

    std::vector<uint8_t> storage;
    const auto opened = openRecordsAsClientWould(plainWire, *f.clientRecv, storage);
    // Expected to FAIL under the injected bug (RED evidence): the client
    // cannot recover the Records frame from unsealed bytes — exactly why
    // consumer fetch was entirely broken on an encrypted session.
    EXPECT_TRUE(opened.has_value())
        << "expected to FAIL under the injected bug (RED evidence): the client "
           "cannot open a plaintext Records push as if it were sealed";
}
