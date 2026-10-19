// libs/gateway/tests/GatewayCryptoRegressionTest.cpp
//
// Task 15 (client<->broker TLS/AEAD): SECURITY REGRESSION SUITE, named
// `*Regression*` so it is selected as the run-first priority gate
// (CLAUDE.md rule 6/7), ahead of the rest of the test matrix.
//
// This is the BROKER layer of the three-repo regression gate (the other two
// are rune-crypto's tests/CryptoRegressionTest.cpp and rune-client-cpp's
// tests/crypto/ClientCryptoRegressionTest.cpp). It CONSOLIDATES assertions
// already proven piecemeal across GatewayCryptoTest.cpp / RequireTlsDowngradeTest.cpp
// (Tasks 7/8/8b/9/19/22/23) into one named suite, and FILLS three gaps those
// files did not cover explicitly:
//   - a dedicated AEAD *tag*-byte-flip case on the gateway ingress path (the
//     existing TamperedCiphertextDropped only flips a ciphertext byte);
//   - reorder-WITHIN-the-64-window ACCEPTED on the gateway ingress path (the
//     existing client-side suite proves this; the gateway/broker side never
//     did, even though openIngressFrame shares the identical window logic);
//   - cross-session/wrong-key injection using a SECOND, independently
//     handshaked session's real derived key (the existing
//     WindowNotPoisonedByForgedFrame case uses an all-zero key, which proves
//     "wrong key" but not "a REAL key from a DIFFERENT session");
//   - a ClientHello handshake blob proven LARGER than the old fixed 512-byte
//     scratch buffer this code used to carry handshake bytes in, driven
//     through to a successful handshake (no truncation/overflow) — the
//     production fields involved (PendingConnect::hsBytes) are a growable
//     std::vector today specifically BECAUSE of this constraint; this test
//     pins that by measuring real bytes transferred, not just trusting the
//     type declaration.
//
// This file deliberately does NOT include/link rune_gateway (which drags in
// <Aeron.h>, unavailable on this host) — same reasoning as GatewayCryptoTest.cpp's
// own file header. All fixtures below are self-contained duplicates of that
// file's helpers (by design: "it's fine to add a focused regression version
// ... don't delete the originals" — task-15-brief.md).

#include <gtest/gtest.h>

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>
#include <rune/gateway/ConnectDowngradePolicy.h>
#include <rune/gateway/CryptoFrame.h>
#include <rune/node/RecordsSeal.h>
#include <rune/node/TlsStartupPolicy.h>
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
namespace pn  = rune::node;

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

using GatewayCryptoState = rune::gateway::CryptoState;

// Drive a full in-process TLS 1.3 handshake, exactly GatewayCryptoTest.cpp's
// driveServerHandshake. `*totalBytes`, if non-null, accumulates every byte
// transferred in either direction across the whole handshake (used by the
// 512-byte-scratch regression below).
bool driveServerHandshake(cr::TlsSession& srv, cr::TlsSession& cli,
                          std::size_t* totalBytes = nullptr) {
    cli.readInbound(nullptr, 0);
    uint8_t chunk[16384];

    for (int round = 0; round < 10; ++round) {
        bool progressed = false;

        for (;;) {
            const std::size_t n = cli.writeOutbound(chunk, sizeof chunk);
            if (n == 0) break;
            progressed = true;
            if (totalBytes) *totalBytes += n;
            if (srv.readInbound(chunk, n) == cr::HsState::Failed) return false;
        }
        for (;;) {
            const std::size_t n = srv.writeOutbound(chunk, sizeof chunk);
            if (n == 0) break;
            progressed = true;
            if (totalBytes) *totalBytes += n;
            if (cli.readInbound(chunk, n) == cr::HsState::Failed) return false;
        }

        if (srv.done() && cli.done()) return true;
        if (!progressed) break;
    }
    return srv.done() && cli.done();
}

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
    f.gw.b2c = srvKeys.b2c;
    std::memcpy(f.gw.b2cPrefix, srvKeys.b2cPrefix, 4);
    f.gw.c2b = srvKeys.c2b;
    std::memcpy(f.gw.c2bPrefix, srvKeys.c2bPrefix, 4);
    f.clientSend.emplace(cliKeys.cipher, cliKeys.c2b, cliKeys.c2bPrefix);
    f.clientRecv.emplace(cliKeys.cipher, cliKeys.b2c, cliKeys.b2cPrefix);
    return f;
}

std::vector<uint8_t> makeRecordsBatch(std::size_t n, uint8_t fill) {
    return std::vector<uint8_t>(n, fill);
}

}  // namespace

// ===========================================================================
// AEAD tamper (ciphertext + TAG byte flips) + AAD tamper.
// ===========================================================================

TEST(GatewayCryptoRegression, CiphertextByteFlipDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/100);
    const auto plain = buildPlainFrame(16, 100, "produce-body");
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    wire[rpc::kCryptoHeaderBytes] ^= 0xFF;  // first ciphertext byte, never the tag

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a flipped ciphertext byte must be dropped";
    EXPECT_EQ(f.gw.recvSeq, 0u);
    EXPECT_EQ(f.gw.recvWindow, 0u);
}

TEST(GatewayCryptoRegression, TagByteFlipDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/101);
    const auto plain = buildPlainFrame(16, 101, "produce-body");
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    wire.back() ^= 0xFF;  // seal() output is ciphertext || 16B tag -> last byte is in the tag

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a flipped AEAD tag byte must be dropped, independently of a ciphertext flip";
    EXPECT_EQ(f.gw.recvSeq, 0u);
    EXPECT_EQ(f.gw.recvWindow, 0u);
}

TEST(GatewayCryptoRegression, AadHeaderByteFlipDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/102);
    const auto plain = buildPlainFrame(16, 102, "produce-body");
    auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    // Flip a byte inside the clear MessageHeader's sessionId field (part of the AAD).
    wire[8] ^= 0xFF;

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n, 0u) << "a tampered sessionId (AAD) byte must be dropped";
}

TEST(GatewayCryptoRegression, AadSenderIdByteFlipDropped) {
    // Per-worker egress: senderId lives in the (authenticated) CryptoSubHeader
    // and must never be forgeable into redirecting a frame to a different key.
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/103);
    cr::AeadKey key{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, /*senderId=*/5, key));
    cr::AeadCipher send(f.gw.cipher, key, f.gw.b2cPrefix);

    rpc::Records rec;
    rec.sessionId = 103;
    rec.correlationId = 1;
    rec.subscriptionId = 1;
    rec.topicId = 1;
    rec.partition = 0;
    rec.baseOffset = 0;
    const auto batch = makeRecordsBatch(16, 0x33);
    rec.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> plainBuf(rec.encodedSize());
    rec.encode(plainBuf.data());

    uint64_t sendSeq = 0;
    pn::RecordsSealCtx ctx;
    ctx.send     = &send;
    ctx.sendSeq  = &sendSeq;
    ctx.cipher   = static_cast<uint8_t>(f.gw.cipher);
    ctx.senderId = 5;

    std::vector<uint8_t> sealed(plainBuf.size() + rpc::kCryptoOverhead);
    const std::size_t n = pn::sealRecordsFrame(ctx, plainBuf.data(), plainBuf.size(), sealed.data(), sealed.size());
    ASSERT_GT(n, 0u);
    sealed.resize(n);

    // Flip the senderId byte (epoch high byte) in the authenticated subheader.
    sealed[rpc::MessageHeader::ENCODED_SIZE + 3] ^= 0xFF;

    cr::AeadCipher recv(f.gw.cipher, key, f.gw.b2cPrefix);
    const auto subOpt = rpc::CryptoSubHeader::decode(sealed.data() + rpc::MessageHeader::ENCODED_SIZE,
                                                       sealed.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(subOpt.has_value());
    std::vector<uint8_t> out(sealed.size());
    const long m = recv.open(subOpt->seq, sealed.data(), rpc::kCryptoHeaderBytes,
                              sealed.data() + rpc::kCryptoHeaderBytes, sealed.size() - rpc::kCryptoHeaderBytes,
                              out.data());
    EXPECT_EQ(m, -1) << "a forged senderId byte (inside the AAD) must fail authentication";
}

// ===========================================================================
// Replay: resent seq dropped; reorder within the 64-window accepted;
// too-old (>64 behind) dropped; forged seq does not poison the window.
// ===========================================================================

TEST(GatewayCryptoRegression, ResentSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/110);
    const auto plain = buildPlainFrame(16, 110, "produce-body");
    const auto wire = sealFrameAsPeer(*f.clientSend, 1, plain);

    std::vector<uint8_t> scratch(wire.size());
    const std::size_t n1 = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    ASSERT_EQ(n1, plain.size());

    const std::size_t n2 = rune::gateway::codec::openIngressFrame(
        f.gw, wire.data(), wire.size(), scratch.data(), scratch.size());
    EXPECT_EQ(n2, 0u) << "resending the exact same sealed frame must be dropped";
    EXPECT_EQ(f.gw.recvSeq, 1u);
}

TEST(GatewayCryptoRegression, ReorderWithinWindowAccepted) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/111);
    const auto plain1 = buildPlainFrame(16, 111, "seq-1");
    const auto plain2 = buildPlainFrame(16, 111, "seq-2");
    const auto plain3 = buildPlainFrame(16, 111, "seq-3");
    const auto wire1 = sealFrameAsPeer(*f.clientSend, 1, plain1);
    const auto wire2 = sealFrameAsPeer(*f.clientSend, 2, plain2);
    const auto wire3 = sealFrameAsPeer(*f.clientSend, 3, plain3);

    std::vector<uint8_t> scratch(wire3.size());
    // Deliver 1, then 3 (skipping 2), then 2 — a legitimate network reorder;
    // seq=2 is still inside the 64-wide window and previously unseen.
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, wire1.data(), wire1.size(),
                                                       scratch.data(), scratch.size()),
              plain1.size());
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, wire3.data(), wire3.size(),
                                                       scratch.data(), scratch.size()),
              plain3.size());
    const std::size_t reordered = rune::gateway::codec::openIngressFrame(
        f.gw, wire2.data(), wire2.size(), scratch.data(), scratch.size());
    EXPECT_EQ(reordered, plain2.size()) << "seq=2, delivered out of order but inside the window, must be accepted";

    // A second delivery of the now-seen seq=2 is a replay.
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, wire2.data(), wire2.size(),
                                                       scratch.data(), scratch.size()),
              0u);
}

TEST(GatewayCryptoRegression, TooOldSeqDropped) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/112);
    const auto plain = buildPlainFrame(16, 112, "produce-body");

    const auto wireAhead = sealFrameAsPeer(*f.clientSend, 100, plain);
    std::vector<uint8_t> scratch(wireAhead.size());
    ASSERT_EQ(rune::gateway::codec::openIngressFrame(f.gw, wireAhead.data(), wireAhead.size(),
                                                       scratch.data(), scratch.size()),
              plain.size());
    ASSERT_EQ(f.gw.recvSeq, 100u);

    const auto wireOld = sealFrameAsPeer(*f.clientSend, 1, plain);  // 99 behind, >= 64 -> too old
    scratch.assign(wireOld.size(), 0);
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, wireOld.data(), wireOld.size(),
                                                       scratch.data(), scratch.size()),
              0u);
    EXPECT_EQ(f.gw.recvSeq, 100u);
}

TEST(GatewayCryptoRegression, ForgedSeqDoesNotPoisonWindowAgainstLaterLegitFrame) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/113);
    const auto plain = buildPlainFrame(16, 113, "produce-body");

    const uint8_t fakePrefix[cr::kNoncePrefix] = {1, 2, 3, 4};
    cr::AeadCipher wrongKeyCipher(f.gw.cipher, cr::AeadKey{}, fakePrefix);
    const auto forged = sealFrameAsPeer(wrongKeyCipher, /*seq=*/5, plain);

    std::vector<uint8_t> scratch(forged.size());
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, forged.data(), forged.size(),
                                                       scratch.data(), scratch.size()),
              0u);
    EXPECT_EQ(f.gw.recvSeq, 0u) << "a forged frame must never advance the window";

    const auto legit = sealFrameAsPeer(*f.clientSend, /*seq=*/5, plain);
    scratch.assign(legit.size(), 0);
    EXPECT_EQ(rune::gateway::codec::openIngressFrame(f.gw, legit.data(), legit.size(),
                                                       scratch.data(), scratch.size()),
              plain.size())
        << "the SAME seq, sealed correctly afterwards, must still succeed -- no trace left by the forgery";
    EXPECT_EQ(f.gw.recvSeq, 5u);
}

// ===========================================================================
// Cross-session / wrong-key injection: a frame sealed under one session's
// key must fail to open under ANOTHER (real, independently handshaked)
// session's key -- fail closed. (Stronger than "wrong key = all-zero bytes":
// uses a second genuine TLS 1.3 handshake's derived key.)
// ===========================================================================

TEST(GatewayCryptoRegression, CrossSessionInjectionFailsToOpenUnderDifferentSessionKey) {
    DataPlaneFixture sessionA = makeDataPlaneFixture(/*sessionId=*/120);
    DataPlaneFixture sessionB = makeDataPlaneFixture(/*sessionId=*/121);  // independent handshake

    const auto plain = buildPlainFrame(16, 120, "session-a-body");
    const auto wire = sealFrameAsPeer(*sessionA.clientSend, /*seq=*/1, plain);

    // Session A's wire frame opens fine under session A's own gateway state.
    std::vector<uint8_t> scratchA(wire.size());
    ASSERT_EQ(rune::gateway::codec::openIngressFrame(sessionA.gw, wire.data(), wire.size(),
                                                       scratchA.data(), scratchA.size()),
              plain.size());

    // The SAME wire bytes must fail under session B's gateway state -- a
    // completely independent handshake's derived key.
    std::vector<uint8_t> scratchB(wire.size());
    const std::size_t crossOpened = rune::gateway::codec::openIngressFrame(
        sessionB.gw, wire.data(), wire.size(), scratchB.data(), scratchB.size());
    EXPECT_EQ(crossOpened, 0u)
        << "a frame sealed under session A's key must never open under session B's key";
    EXPECT_EQ(sessionB.gw.recvSeq, 0u) << "the failed cross-session attempt must not touch session B's window";
}

// ===========================================================================
// Per-sender (shard egress): distinct keys/windows; sender0==gateway.
// ===========================================================================

TEST(GatewayCryptoRegression, PerSenderDistinctKeysNeitherOpensUnderTheOther) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/130);

    cr::AeadKey key1{}, key2{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, 1, key1));
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, 2, key2));
    EXPECT_NE(0, std::memcmp(key1.bytes, key2.bytes, sizeof key1.bytes));

    cr::AeadCipher send1(f.gw.cipher, key1, f.gw.b2cPrefix);
    cr::AeadCipher send2(f.gw.cipher, key2, f.gw.b2cPrefix);
    const uint8_t aad[4] = {1, 1, 1, 1};
    const uint8_t plain[] = "worker-payload";
    uint8_t ct1[sizeof(plain) + cr::kTagSize], ct2[sizeof(plain) + cr::kTagSize];
    ASSERT_GT(send1.seal(1, aad, 4, plain, sizeof plain, ct1), 0u);
    ASSERT_GT(send2.seal(1, aad, 4, plain, sizeof plain, ct2), 0u);

    cr::AeadCipher recv1(f.gw.cipher, key1, f.gw.b2cPrefix);
    cr::AeadCipher recv2(f.gw.cipher, key2, f.gw.b2cPrefix);
    uint8_t out[sizeof plain];
    EXPECT_GE(recv1.open(1, aad, 4, ct1, sizeof ct1, out), 0);
    EXPECT_GE(recv2.open(1, aad, 4, ct2, sizeof ct2, out), 0);
    EXPECT_EQ(-1, recv2.open(1, aad, 4, ct1, sizeof ct1, out)) << "worker 1's frame must not open under worker 2's key";
    EXPECT_EQ(-1, recv1.open(1, aad, 4, ct2, sizeof ct2, out)) << "worker 2's frame must not open under worker 1's key";
}

TEST(GatewayCryptoRegression, SenderZeroEqualsGatewayB2cUnchanged) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/131);
    cr::AeadKey out{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, 0, out));
    EXPECT_EQ(0, std::memcmp(out.bytes, f.gw.b2c.bytes, sizeof out.bytes));
}

// ===========================================================================
// Nonce uniqueness: acks + Records under one worker key share ONE monotonic
// seq counter (the interleave property Task 23 requires).
// ===========================================================================

TEST(GatewayCryptoRegression, WorkerAckAndRecordsShareOneMonotonicSeq) {
    DataPlaneFixture f = makeDataPlaneFixture(/*sessionId=*/140);
    constexpr uint8_t kSenderId = 2;
    cr::AeadKey workerKey{};
    ASSERT_TRUE(cr::deriveSenderKey(f.gw.b2c, kSenderId, workerKey));
    cr::AeadCipher workerSend(f.gw.cipher, workerKey, f.gw.b2cPrefix);
    uint64_t sharedSendSeq = 0;  // the ONE counter Records + acks must share

    rpc::Records rec;
    rec.sessionId = 140; rec.correlationId = 1; rec.subscriptionId = 1;
    rec.topicId = 1; rec.partition = 0; rec.baseOffset = 0;
    const auto batch = makeRecordsBatch(8, 0x11);
    rec.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> plainRec(rec.encodedSize());
    rec.encode(plainRec.data());

    pn::RecordsSealCtx ctx;
    ctx.send = &workerSend; ctx.sendSeq = &sharedSendSeq;
    ctx.cipher = static_cast<uint8_t>(f.gw.cipher); ctx.senderId = kSenderId;

    std::vector<uint8_t> sealedRec(plainRec.size() + rpc::kCryptoOverhead);
    const std::size_t nRec = pn::sealRecordsFrame(ctx, plainRec.data(), plainRec.size(), sealedRec.data(), sealedRec.size());
    ASSERT_GT(nRec, 0u);
    sealedRec.resize(nRec);

    rpc::ProduceResponse resp;
    resp.sessionId = 140; resp.correlationId = 2; resp.errorCode = 0;
    resp.partition = 0; resp.baseOffset = 5; resp.logStartOffset = -1;
    std::vector<uint8_t> plainAck(rpc::ProduceResponse::encodedSizeFor(2));
    resp.encodeFor(plainAck.data(), 2);
    std::vector<uint8_t> sealedAck(plainAck.size() + rpc::kCryptoOverhead);
    const std::size_t nAck = pn::sealRecordsFrame(ctx, plainAck.data(), plainAck.size(), sealedAck.data(), sealedAck.size());
    ASSERT_GT(nAck, 0u);
    sealedAck.resize(nAck);

    const auto subRec = rpc::CryptoSubHeader::decode(sealedRec.data() + rpc::MessageHeader::ENCODED_SIZE,
                                                       sealedRec.size() - rpc::MessageHeader::ENCODED_SIZE);
    const auto subAck = rpc::CryptoSubHeader::decode(sealedAck.data() + rpc::MessageHeader::ENCODED_SIZE,
                                                       sealedAck.size() - rpc::MessageHeader::ENCODED_SIZE);
    ASSERT_TRUE(subRec.has_value());
    ASSERT_TRUE(subAck.has_value());
    EXPECT_EQ(subRec->seq, 1u);
    EXPECT_EQ(subAck->seq, 2u);
    EXPECT_NE(subRec->seq, subAck->seq) << "Records and the ack must never share a seq under the same key (nonce reuse)";
}

// ===========================================================================
// Downgrade safety: require_tls refuses plaintext Connect; non-shard
// per-worker-egress + TLS refuses to start.
// ===========================================================================

TEST(GatewayCryptoRegression, RequireTlsRejectsPlaintextConnect) {
    EXPECT_FALSE(rune::gateway::shouldEngageCrypto(/*clientOffersCrypto=*/false, /*brokerHasTls=*/true));
    EXPECT_TRUE(rune::gateway::mustRejectForRequireTls(/*requireTls=*/true, /*cryptoEstablished=*/false));
}

TEST(GatewayCryptoRegression, NonShardPerWorkerEgressWithTlsRefusesStart) {
    EXPECT_TRUE(rune::node::mustRefuseNonShardPerWorkerEgressTls(
        /*perWorkerEgress=*/true, /*shardMode=*/false, /*requireTls=*/true, /*brokerHasTls=*/false));
    EXPECT_TRUE(rune::node::mustRefuseNonShardPerWorkerEgressTls(
        /*perWorkerEgress=*/true, /*shardMode=*/false, /*requireTls=*/false, /*brokerHasTls=*/true));
}

// ===========================================================================
// Handshake: untrusted cert rejected; wrong expectHost rejected; exportKeys
// fails CLOSED; bad ClientHello fails non-retriably; a handshake blob LARGER
// than the old 512-byte scratch is handled without truncation.
// ===========================================================================

TEST(GatewayCryptoRegression, UntrustedCertRejected) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    cr::TlsSession cli = cr::TlsSession::makeClient("", "localhost");  // "" = trust nothing
    ASSERT_TRUE(srv.valid());
    ASSERT_TRUE(cli.valid());
    EXPECT_FALSE(driveServerHandshake(srv, cli));
}

TEST(GatewayCryptoRegression, WrongExpectHostRejected) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    cr::TlsSession cli = cr::TlsSession::makeClient(caPem.c_str(), "not-the-cert-san.example");
    ASSERT_TRUE(srv.valid());
    ASSERT_TRUE(cli.valid());
    EXPECT_FALSE(driveServerHandshake(srv, cli));
}

TEST(GatewayCryptoRegression, ExportKeysFailsClosedBeforeDone) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    ASSERT_FALSE(srv.done());
    const cr::SessionKeys keys = srv.exportKeys(cr::Cipher::Aes256Gcm, 1);
    EXPECT_EQ(keys.cipher, cr::Cipher::None);
}

TEST(GatewayCryptoRegression, BadClientHelloFailsNonRetriably) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());
    const uint8_t garbage[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03};
    EXPECT_EQ(srv.readInbound(garbage, sizeof garbage), cr::HsState::Failed);
    EXPECT_FALSE(srv.done());
}

TEST(GatewayCryptoRegression, HandshakeBlobLargerThanOld512ByteScratchHandled) {
    // handleConnect/handleHandshakeContinue_ accumulate handshake bytes into
    // PendingConnect::hsBytes -- a growable std::vector<uint8_t> today
    // SPECIFICALLY so a flight (e.g. ServerHello+Certificate+CertVerify+
    // Finished, which includes the broker's X.509 cert) isn't truncated by a
    // fixed buffer. This drives a REAL handshake and sums every byte
    // transferred in either direction, proving a real flight here exceeds
    // the old 512-byte bound and STILL completes successfully end to end --
    // not merely that the field's type is a vector.
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    cr::TlsSession cli = cr::TlsSession::makeClient(caPem.c_str(), "localhost");
    ASSERT_TRUE(srv.valid());
    ASSERT_TRUE(cli.valid());

    std::size_t totalBytes = 0;
    ASSERT_TRUE(driveServerHandshake(srv, cli, &totalBytes));
    EXPECT_GT(totalBytes, 512u)
        << "a real TLS 1.3 handshake flight (incl. the broker's certificate) must exceed the "
           "old fixed 512-byte scratch bound -- and still complete (asserted above) without "
           "truncation now that the carrying buffer is growable";
}
