// tests/crypto/ClientCryptoRegressionTest.cpp
//
// Task 15 (client<->broker TLS/AEAD): SECURITY REGRESSION SUITE, named
// `*Regression*` so it is selected as the run-first priority gate (CLAUDE.md
// rule 6/7), ahead of the rest of the test matrix.
//
// This is the CLIENT layer of the three-repo regression gate (the other two
// are rune-crypto's tests/CryptoRegressionTest.cpp and rune-broker's
// libs/gateway/tests/GatewayCryptoRegressionTest.cpp). It CONSOLIDATES
// assertions already proven piecemeal in ClientCryptoRoundTripTest.cpp
// (Tasks 5/6/6b/9/20) into one named suite, and FILLS four gaps that file
// did not cover explicitly:
//   - a dedicated ciphertext-byte-flip case distinct from the existing
//     tag-byte-flip one (TamperedCiphertextIsDroppedWindowNotPoisoned only
//     flips the LAST byte, which is inside the tag);
//   - too-old-seq (>64 behind) dropped, on the client's egress-open path
//     (the existing suite proves replay + in-window-reorder, but never
//     "too old", even though openEgressFrame shares the identical window
//     logic the gateway/worker-ingress suites already prove this for);
//   - forged-seq-does-not-poison-the-window, on the client's egress-open
//     path (a wrong-key frame claiming a fresh seq must fail auth AND leave
//     the window untouched for a later legitimate frame using that seq);
//   - cross-session/wrong-key injection using a SECOND, independently
//     handshaked session's real derived key (a strictly stronger claim than
//     "wrong key = random bytes");
//   - untrusted-cert rejected at the SessionMachine level (the existing
//     client suite only proves wrong-hostname; untrusted-cert was only ever
//     proven at the rune-crypto TlsSession layer, not through the client's
//     own connect() path);
//   - a handshake blob proven LARGER than the old fixed 512-byte scratch,
//     driven through SessionMachine's own (growable) hsBytes_ to a
//     successful Connected session.
//
// This file duplicates ClientCryptoRoundTripTest.cpp's fixture helpers
// locally (by design — see task-15-brief.md: "it's fine to add a focused
// regression version ... don't delete the originals").

#include <gtest/gtest.h>
#include <openssl/evp.h>

#include <rune/client/codec/Codec.h>
#include "../support/CryptoStateTestAccess.h"
#include <rune/client/codec/CryptoFrame.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/crypto/TlsSession.h>

#include <cstring>
#include <optional>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace cc = rune::client::codec;
namespace sm = rune::client::session;
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

struct FakeIo final : sm::SessionIo {
    std::vector<std::vector<uint8_t>> sends;
    std::string channel;   // issue #115: the session's current endpoint (its host is the certificate name)
    bool sendFrame(const uint8_t* buf, std::size_t len) override {
        sends.emplace_back(buf, buf + len);
        return true;
    }
    bool retarget(const std::string& ep) noexcept override {
        channel = ep.rfind("aeron:", 0) == 0 ? ep : "aeron:udp?endpoint=" + ep;
        return true;
    }
    [[nodiscard]] std::string ingressChannel() const override { return channel; }
    [[nodiscard]] std::size_t sendCount() const { return sends.size(); }
    [[nodiscard]] const std::vector<uint8_t>& last() const { return sends.back(); }
};

// Drive the handshake to completion (or failure). `*totalBytes`, if
// non-null, accumulates every handshakeBytes payload transferred in either
// direction (used by the 512-byte-scratch regression below). Mirrors
// ClientCryptoRoundTripTest.cpp's driveHandshake exactly, plus the counter.
template <class Io>   // FakeIo, or the adversarial review's AgentLikeIo
bool driveHandshake(sm::SessionMachine& m, Io& io, cr::TlsSession& srv,
                     int64_t sessionId, int64_t nowNs, std::size_t* totalBytes = nullptr,
                     int32_t brokerVersion = 0) {   // issue #116: the ConnectResponse.brokerVersion the broker announces
    std::size_t delivered = 0;
    for (int i = 0; i < 20; ++i) {
        if (io.sendCount() <= delivered) break;
        const bool isFirst = (delivered == 0);
        const auto& frame  = io.sends[delivered++];

        std::string_view peerBytes;
        int64_t          corrId = 0;
        uint8_t          chosenCipher = 0;
        if (isFirst) {
            auto req = cc::decodeConnectRequest(frame.data(), frame.size());
            if (!req || req->cipherList.empty() || req->handshakeBytes.empty()) return false;
            peerBytes    = req->handshakeBytes;
            corrId       = req->correlationId;
            chosenCipher = req->cipherList[0];
        } else {
            auto hc = cc::decodeHandshakeContinue(frame.data(), frame.size());
            if (!hc) return false;
            peerBytes = hc->handshakeBytes;
            corrId    = hc->correlationId;
        }
        if (totalBytes) *totalBytes += peerBytes.size();

        if (srv.readInbound(reinterpret_cast<const uint8_t*>(peerBytes.data()), peerBytes.size()) ==
            cr::HsState::Failed) {
            return false;
        }

        uint8_t out[16384];
        const std::size_t n = srv.writeOutbound(out, sizeof out);
        if (totalBytes) *totalBytes += n;
        const std::string_view outView(reinterpret_cast<const char*>(out), n);

        if (isFirst) {
            m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, /*assignedSessionId=*/sessionId,
                                 corrId, nowNs, chosenCipher, outView,
                                 rune::protocol::client::AuthMode::InProcess,
                                 rune::protocol::client::AuthState::Anonymous, brokerVersion);
        } else if (m.state() == sm::SessionState::Handshaking) {
            m.onHandshakeContinueResponse(/*code=*/0, outView, corrId, nowNs);
        }
    }
    return m.state() == sm::SessionState::Connected && srv.done();
}

std::vector<uint8_t> buildPlainProduceFrame(int64_t sessionId, int64_t correlationId,
                                             std::string_view body) {
    namespace rpc = rune::protocol::client;
    std::vector<uint8_t> buf(rpc::MessageHeader::ENCODED_SIZE + body.size());
    rpc::MessageHeader h;
    h.blockLength   = static_cast<uint16_t>(body.size());
    h.templateId    = static_cast<uint16_t>(rpc::TemplateId::Produce);
    h.sessionId     = sessionId;
    h.correlationId = correlationId;
    h.encode(buf.data());
    std::memcpy(buf.data() + rpc::MessageHeader::ENCODED_SIZE, body.data(), body.size());
    return buf;
}

sm::CryptoState brokerSideCryptoStateFor(cr::TlsSession& srv, const sm::SessionMachine& m,
                                          int64_t sessionId) {
    const auto keys = srv.exportKeys(m.cryptoState().cipher, sessionId);
    sm::CryptoState c;
    c.encrypted = true;
    c.cipher    = keys.cipher;
    c.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
    c.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
    return c;
}

// Shared setup: drive a real TLS handshake to Connected+encrypted (or, for
// the untrusted-cert regression case, to a deliberate failure).
struct FrameFixture {
    std::string    serverPem, serverKey, caPem;
    cr::TlsSession  srv;
    FakeIo          io;
    sm::SessionMachine m;
    static constexpr int64_t kSessionId = 42;

    // `useDefaultTrust=false` means "use `trustPem` verbatim, even if empty"
    // (TlsSession::makeClient's own contract: "" = explicitly trust nothing)
    // -- distinguishing that from "no override given" (the common case,
    // which loads the real test CA) requires a flag, since an empty string
    // is itself a meaningful, distinct value here.
    explicit FrameFixture(std::string trustPem = {}, bool useDefaultTrust = true)
        : serverPem(slurp(testdataPath("server.pem")))
        , serverKey(slurp(testdataPath("server.key")))
        , caPem(useDefaultTrust ? slurp(testdataPath("ca.pem")) : std::move(trustPem))
        , srv(cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str()))
        , m(io, [this] {
              sm::SessionConfig c;
              c.egressChannel  = "aeron:udp?endpoint=client:40456";
              c.offerCrypto    = true;
              c.tlsTrustPem    = caPem;
              c.tlsExpectHost  = "localhost";
              return c;
          }())
    {}

    bool established(int64_t nowNs, int32_t brokerVersion = 0) {
        m.connect(nowNs);
        return driveHandshake(m, io, srv, kSessionId, nowNs + 1, nullptr, brokerVersion) &&
               m.cryptoState().encrypted;
    }
};

}  // namespace

// ===========================================================================
// AEAD tamper: ciphertext byte flip AND tag byte flip -> dropped.
// ===========================================================================

TEST(ClientCryptoRegression, CiphertextByteFlipDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 800, "ciphertext-tamper");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);

    // Flip the FIRST ciphertext byte (right after the 36-byte clear AAD region, never the tag).
    sealed[rune::protocol::client::kCryptoHeaderBytes] ^= 0xFF;

    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened), 0u)
        << "a flipped ciphertext byte must be dropped";
}

TEST(ClientCryptoRegression, TagByteFlipDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 801, "tag-tamper");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);
    sealed[sealedLen - 1] ^= 0x01;  // the last byte is always inside the 16B tag

    const uint64_t recvSeqBefore = brokerSide.recvSeq;
    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened), 0u)
        << "a flipped tag byte must be dropped";
    EXPECT_EQ(brokerSide.recvSeq, recvSeqBefore) << "a rejected tamper must not poison the window";
}

TEST(ClientCryptoRegression, AadHeaderByteFlipDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 802, "aad-tamper");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);
    sealed[8] ^= 0xFF;  // sessionId field — clear, part of the AAD

    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened), 0u)
        << "a tampered sessionId (AAD) byte must be dropped even though ciphertext/tag are untouched";
}

// ===========================================================================
// Replay: resent seq dropped; reorder within the 64-window accepted;
// too-old (>64 behind) dropped; forged seq does not poison the window.
// ===========================================================================

TEST(ClientCryptoRegression, ResentSeqDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 803, "replay-me");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);

    uint8_t opened[256];
    ASSERT_EQ(cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened), plain.size());
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened), 0u)
        << "a resent (exact-duplicate) seq must be dropped";
}

TEST(ClientCryptoRegression, ReorderWithinWindowAccepted) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    uint8_t sealed1[256], sealed2[256], sealed3[256];
    const auto plain1 = buildPlainProduceFrame(FrameFixture::kSessionId, 804, "seq-1");
    const auto plain2 = buildPlainProduceFrame(FrameFixture::kSessionId, 805, "seq-2");
    const auto plain3 = buildPlainProduceFrame(FrameFixture::kSessionId, 806, "seq-3");
    const std::size_t len1 = cc::sealFrame(f.m.cryptoState(), plain1.data(), plain1.size(), sealed1, sizeof sealed1);
    const std::size_t len2 = cc::sealFrame(f.m.cryptoState(), plain2.data(), plain2.size(), sealed2, sizeof sealed2);
    const std::size_t len3 = cc::sealFrame(f.m.cryptoState(), plain3.data(), plain3.size(), sealed3, sizeof sealed3);
    ASSERT_GT(len1, 0u); ASSERT_GT(len2, 0u); ASSERT_GT(len3, 0u);

    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed1, len1, opened, sizeof opened), plain1.size());
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed3, len3, opened, sizeof opened), plain3.size());
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed2, len2, opened, sizeof opened), plain2.size())
        << "seq=2, delivered after seq=3 but still inside the window, must be accepted";
}

TEST(ClientCryptoRegression, TooOldSeqDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);

    // Advance sendSeq to 100 by sealing 99 throwaway frames first, then the real "ahead" one.
    uint8_t scratch[256];
    for (int i = 0; i < 99; ++i) {
        const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 900 + i, "filler");
        ASSERT_GT(cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(), scratch, sizeof scratch), 0u);
    }
    const auto plainAhead = buildPlainProduceFrame(FrameFixture::kSessionId, 999, "seq-100");
    uint8_t sealedAhead[256];
    const std::size_t lenAhead = cc::sealFrame(f.m.cryptoState(), plainAhead.data(), plainAhead.size(), sealedAhead, sizeof sealedAhead);
    ASSERT_GT(lenAhead, 0u);

    uint8_t opened[256];
    ASSERT_EQ(cc::openEgressFrame(brokerSide, sealedAhead, lenAhead, opened, sizeof opened), plainAhead.size());
    ASSERT_EQ(brokerSide.recvSeq, 100u);

    // seq=1 is now 99 behind the high-water mark -- too old. CryptoState's
    // own AeadCipher is private/non-rewindable, so build an equivalent c2b
    // cipher from the independently re-exported session keys to seal seq=1
    // directly (same key material sealFrame would have used for the real
    // first frame of this session).
    const auto plainOld = buildPlainProduceFrame(FrameFixture::kSessionId, 1, "seq-1-too-old");
    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
    ASSERT_NE(keys.cipher, cr::Cipher::None);
    cr::AeadCipher equivalentClientSend(keys.cipher, keys.c2b, keys.c2bPrefix);
    namespace rpc = rune::protocol::client;
    std::vector<uint8_t> wireOld(plainOld.size() + rpc::kCryptoOverhead);
    std::memcpy(wireOld.data(), plainOld.data(), rpc::MessageHeader::ENCODED_SIZE);
    rpc::CryptoSubHeader sub;
    sub.version = 1;
    sub.seq     = 1;
    sub.encode(wireOld.data() + rpc::MessageHeader::ENCODED_SIZE);
    const std::size_t bodyLen = plainOld.size() - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t n = equivalentClientSend.seal(1, wireOld.data(), rpc::kCryptoHeaderBytes,
                                                      plainOld.data() + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
                                                      wireOld.data() + rpc::kCryptoHeaderBytes);
    ASSERT_GT(n, 0u);
    wireOld.resize(rpc::kCryptoHeaderBytes + n);

    EXPECT_EQ(cc::openEgressFrame(brokerSide, wireOld.data(), wireOld.size(), opened, sizeof opened), 0u)
        << "a seq more than 64 behind the high-water mark must be dropped as too old";
    EXPECT_EQ(brokerSide.recvSeq, 100u);
}

TEST(ClientCryptoRegression, ForgedSeqDoesNotPoisonWindowAgainstLaterLegitFrame) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    namespace rpc = rune::protocol::client;

    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 1000, "forge-target");
    const uint8_t fakePrefix[cr::kNoncePrefix] = {9, 9, 9, 9};
    cr::AeadCipher wrongKeyCipher(f.m.cryptoState().cipher, cr::AeadKey{}, fakePrefix);

    std::vector<uint8_t> forged(plain.size() + rpc::kCryptoOverhead);
    std::memcpy(forged.data(), plain.data(), rpc::MessageHeader::ENCODED_SIZE);
    rpc::CryptoSubHeader sub;
    sub.version = 1;
    sub.seq     = 5;
    sub.encode(forged.data() + rpc::MessageHeader::ENCODED_SIZE);
    const std::size_t bodyLen = plain.size() - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t n = wrongKeyCipher.seal(5, forged.data(), rpc::kCryptoHeaderBytes,
                                               plain.data() + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
                                               forged.data() + rpc::kCryptoHeaderBytes);
    ASSERT_GT(n, 0u);
    forged.resize(rpc::kCryptoHeaderBytes + n);

    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(brokerSide, forged.data(), forged.size(), opened, sizeof opened), 0u);
    EXPECT_EQ(brokerSide.recvSeq, 0u) << "a forged frame must never advance the window";

    // The same seq (5), sealed LEGITIMATELY afterwards, must still succeed:
    // seal an independent frame at seq=5 using the SAME real c2b key
    // (re-exported from the broker side).
    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
    cr::AeadCipher realClientSend(keys.cipher, keys.c2b, keys.c2bPrefix);
    std::vector<uint8_t> realAtFive(plain.size() + rpc::kCryptoOverhead);
    std::memcpy(realAtFive.data(), plain.data(), rpc::MessageHeader::ENCODED_SIZE);
    rpc::CryptoSubHeader sub5;
    sub5.version = 1;
    sub5.seq     = 5;
    sub5.encode(realAtFive.data() + rpc::MessageHeader::ENCODED_SIZE);
    const std::size_t n5 = realClientSend.seal(5, realAtFive.data(), rpc::kCryptoHeaderBytes,
                                                plain.data() + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
                                                realAtFive.data() + rpc::kCryptoHeaderBytes);
    ASSERT_GT(n5, 0u);
    realAtFive.resize(rpc::kCryptoHeaderBytes + n5);

    EXPECT_EQ(cc::openEgressFrame(brokerSide, realAtFive.data(), realAtFive.size(), opened, sizeof opened),
              plain.size())
        << "the SAME seq (5), sealed legitimately, must still succeed -- the forged attempt left no trace";
    EXPECT_EQ(brokerSide.recvSeq, 5u);
}

// ===========================================================================
// Cross-session / wrong-key injection: a frame sealed under one session's
// key must fail to open under ANOTHER (real, independently handshaked)
// session's key.
// ===========================================================================

TEST(ClientCryptoRegression, CrossSessionInjectionFailsToOpenUnderDifferentSessionKey) {
    FrameFixture sessionA;
    FrameFixture sessionB;
    ASSERT_TRUE(sessionA.srv.valid());
    ASSERT_TRUE(sessionB.srv.valid());
    ASSERT_TRUE(sessionA.established(1'000'000'000LL));
    ASSERT_TRUE(sessionB.established(1'000'000'000LL));

    sm::CryptoState brokerSideA = brokerSideCryptoStateFor(sessionA.srv, sessionA.m, FrameFixture::kSessionId);
    sm::CryptoState brokerSideB = brokerSideCryptoStateFor(sessionB.srv, sessionB.m, FrameFixture::kSessionId);

    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 1100, "session-a-body");
    uint8_t sealed[256];
    const std::size_t sealedLen =
        cc::sealFrame(sessionA.m.cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);

    uint8_t opened[256];
    ASSERT_GT(cc::openEgressFrame(brokerSideA, sealed, sealedLen, opened, sizeof opened), 0u)
        << "sanity: session A's own frame opens under session A's broker-side key";

    EXPECT_EQ(cc::openEgressFrame(brokerSideB, sealed, sealedLen, opened, sizeof opened), 0u)
        << "session A's sealed frame must never open under session B's independently-handshaked key";
}

// ===========================================================================
// Per-sender (shard egress): sender 0 unchanged; distinct senders distinct
// keys/windows; unknown/out-of-range senderId dropped fail-closed; the
// senderId=64 boundary opens, 65 dropped.
// ===========================================================================

namespace {
std::vector<uint8_t> sealAsSender(cr::AeadCipher& cipher, cr::Cipher cipherId, uint8_t senderId,
                                   uint64_t seq, int64_t sessionId, int64_t correlationId,
                                   std::string_view body) {
    namespace rpc = rune::protocol::client;
    const auto plain = buildPlainProduceFrame(sessionId, correlationId, body);
    const std::size_t bodyLen = plain.size() - rpc::MessageHeader::ENCODED_SIZE;
    std::vector<uint8_t> out(rpc::kCryptoHeaderBytes + bodyLen + cr::kTagSize);
    std::memcpy(out.data(), plain.data(), rpc::MessageHeader::ENCODED_SIZE);
    rpc::CryptoSubHeader sub;
    sub.version  = 1;
    sub.cipher   = static_cast<uint8_t>(cipherId);
    sub.senderId = senderId;
    sub.seq      = seq;
    sub.encode(out.data() + rpc::MessageHeader::ENCODED_SIZE);
    const std::size_t n = cipher.seal(seq, out.data(), rpc::kCryptoHeaderBytes,
                                       plain.data() + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
                                       out.data() + rpc::kCryptoHeaderBytes);
    EXPECT_GT(n, 0u);
    out.resize(rpc::kCryptoHeaderBytes + n);
    return out;
}
}  // namespace

TEST(ClientCryptoRegression, Sender0OpensUnchanged) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
    ASSERT_NE(keys.cipher, cr::Cipher::None);
    cr::AeadCipher gatewayCipher(keys.cipher, keys.b2c, keys.b2cPrefix);
    const auto sealed = sealAsSender(gatewayCipher, keys.cipher, 0, 1, FrameFixture::kSessionId, 1200, "gw");

    uint8_t opened[256];
    EXPECT_GT(cc::openEgressFrame(f.m.cryptoState(), sealed.data(), sealed.size(), opened, sizeof opened), 0u);
}

TEST(ClientCryptoRegression, DistinctSendersDistinctKeysNoCrossOpen) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
    cr::AeadKey derived1{}, derived2{};
    ASSERT_TRUE(cr::deriveSenderKey(keys.b2c, 1, derived1));
    ASSERT_TRUE(cr::deriveSenderKey(keys.b2c, 2, derived2));
    cr::AeadCipher worker1(keys.cipher, derived1, keys.b2cPrefix);
    cr::AeadCipher worker2(keys.cipher, derived2, keys.b2cPrefix);

    const auto sealed1 = sealAsSender(worker1, keys.cipher, 1, 1, FrameFixture::kSessionId, 1201, "w1");
    uint8_t opened[256];
    EXPECT_GT(cc::openEgressFrame(f.m.cryptoState(), sealed1.data(), sealed1.size(), opened, sizeof opened), 0u);

    // Replaying sender 1's frame on its own window must be dropped.
    EXPECT_EQ(cc::openEgressFrame(f.m.cryptoState(), sealed1.data(), sealed1.size(), opened, sizeof opened), 0u);

    // The SAME seq (1) on sender 2's independent window must still succeed.
    const auto sealed2 = sealAsSender(worker2, keys.cipher, 2, 1, FrameFixture::kSessionId, 1202, "w2");
    EXPECT_GT(cc::openEgressFrame(f.m.cryptoState(), sealed2.data(), sealed2.size(), opened, sizeof opened), 0u)
        << "sender 2's window is independent of sender 1's";
}

TEST(ClientCryptoRegression, OutOfRangeSenderIdDroppedFailClosed) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
    cr::AeadCipher someCipher(keys.cipher, keys.b2c, keys.b2cPrefix);
    const auto forged = sealAsSender(someCipher, keys.cipher, 250, 1, FrameFixture::kSessionId, 1203, "forged");

    uint8_t opened[256];
    EXPECT_EQ(cc::openEgressFrame(f.m.cryptoState(), forged.data(), forged.size(), opened, sizeof opened), 0u)
        << "an out-of-range senderId must be dropped, fail-closed, with no trial decrypt";
}

TEST(ClientCryptoRegression, Sender64BoundaryOpensSender65Dropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);

    cr::AeadKey derived64{};
    ASSERT_TRUE(cr::deriveSenderKey(keys.b2c, 64, derived64));
    cr::AeadCipher cipher64(keys.cipher, derived64, keys.b2cPrefix);
    const auto sealed64 = sealAsSender(cipher64, keys.cipher, 64, 1, FrameFixture::kSessionId, 1204, "w64");
    uint8_t opened[256];
    EXPECT_GT(cc::openEgressFrame(f.m.cryptoState(), sealed64.data(), sealed64.size(), opened, sizeof opened), 0u)
        << "senderId 64 (the broker's top worker) must open";

    cr::AeadKey derived65{};
    ASSERT_TRUE(cr::deriveSenderKey(keys.b2c, 65, derived65));
    cr::AeadCipher cipher65(keys.cipher, derived65, keys.b2cPrefix);
    const auto sealed65 = sealAsSender(cipher65, keys.cipher, 65, 1, FrameFixture::kSessionId, 1205, "w65");
    EXPECT_EQ(cc::openEgressFrame(f.m.cryptoState(), sealed65.data(), sealed65.size(), opened, sizeof opened), 0u)
        << "senderId 65 is one past the broker's max worker -- must be dropped, fail-closed";
}

// ===========================================================================
// Downgrade safety: requireCrypto never proceeds plaintext on a no-cipher
// ConnectResponse - and (issue #14) never turns that unauthenticated answer
// into a terminal verdict either.
// ===========================================================================

// A plaintext ConnectResponse{OK} to a crypto offer (a stripped offer) never yields a plaintext session: the connect
// is retried as a fresh TLS episode within the connect budget, then the session is Disconnected with lastConnectError
// HandshakeFailed - never Rejected, which any forged packet could otherwise cause.  RED (issue #14): route the
// no-cipher OK to rejectHandshake_ (the old terminal path).
TEST(ClientCryptoRegression, RequireCryptoNeverProceedsPlaintextOnANoCipherResponseRegression) {
    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel = "aeron:udp?endpoint=client:40456";
    cfg.offerCrypto   = true;
    cfg.requireCrypto = true;
    cfg.tlsTrustPem   = "";
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0 = 1'000'000'000LL;
    m.connect(T0);
    for (int attempt = 0; attempt <= cfg.maxConnectRetries; ++attempt) {
        auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
        ASSERT_TRUE(req.has_value()) << attempt;
        EXPECT_FALSE(req->cipherList.empty()) << "every retry offers crypto again (" << attempt << ")";
        m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, /*assignedSessionId=*/99,
                             req->correlationId, T0 + 1 + attempt, /*chosenCipher=*/0, /*handshakeBytes=*/{});
        ASSERT_NE(m.state(), sm::SessionState::Connected) << "never a plaintext session";
        ASSERT_NE(m.state(), sm::SessionState::Rejected) << "an unauthenticated answer is never a verdict";
        EXPECT_FALSE(m.cryptoState().encrypted);
    }
    EXPECT_EQ(m.state(), sm::SessionState::Disconnected) << "the connect budget is spent";
    EXPECT_EQ(m.lastConnectError(), static_cast<int16_t>(rune::protocol::client::ErrorCode::HandshakeFailed));
}

// ===========================================================================
// Handshake: untrusted cert rejected; wrong expectHost rejected; a handshake
// blob larger than the old 512-byte scratch is handled.
// ===========================================================================

TEST(ClientCryptoRegression, UntrustedCertRejectedAtSessionMachineLevel) {
    // tlsTrustPem = "" -> TlsSession::makeClient's "explicitly trust nothing"
    // contract, driven through the REAL SessionMachine::connect() path (not
    // just rune-crypto's TlsSession directly).
    FrameFixture f(/*trustPem=*/"", /*useDefaultTrust=*/false);
    ASSERT_TRUE(f.srv.valid());

    f.m.connect(1'000'000'000LL);
    EXPECT_FALSE(driveHandshake(f.m, f.io, f.srv, FrameFixture::kSessionId, 1'000'000'001LL));
    EXPECT_EQ(f.m.state(), sm::SessionState::Rejected);
    EXPECT_FALSE(f.m.cryptoState().encrypted);
}

TEST(ClientCryptoRegression, WrongExpectHostRejected) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());

    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel = "aeron:udp?endpoint=client:40456";
    cfg.offerCrypto   = true;
    cfg.tlsTrustPem   = caPem;
    cfg.tlsExpectHost = "not-the-cert-san.example";
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0 = 1'000'000'000LL;
    m.connect(T0);
    EXPECT_FALSE(driveHandshake(m, io, srv, /*sessionId=*/7, T0 + 1));
    EXPECT_EQ(m.state(), sm::SessionState::Rejected);
}

TEST(ClientCryptoRegression, HandshakeBlobLargerThanOld512ByteScratchHandled) {
    // SessionMachine's handshake bytes (hsBytes_) are a growable
    // std::vector<uint8_t> today SPECIFICALLY so a flight (ServerHello +
    // Certificate + CertVerify + Finished, which includes the broker's
    // X.509 cert) isn't truncated by a fixed buffer. This drives a REAL
    // handshake, sums every byte transferred, and proves a real flight
    // exceeds the old 512-byte bound while still reaching Connected.
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());

    f.m.connect(1'000'000'000LL);
    std::size_t totalBytes = 0;
    ASSERT_TRUE(driveHandshake(f.m, f.io, f.srv, FrameFixture::kSessionId, 1'000'000'001LL, &totalBytes));
    EXPECT_EQ(f.m.state(), sm::SessionState::Connected);
    EXPECT_GT(totalBytes, 512u)
        << "a real TLS 1.3 handshake flight must exceed the old fixed 512-byte scratch bound "
           "and still complete (asserted above) without truncation";
}

// ===========================================================================
// No-hang fix round 5 (N17): a HandshakeContinueResponse with a code other than
// HandshakeFailed (the broker evicted this handshake from its full
// pending-connect table and answered Internal) is a retriable CONNECT failure —
// never a terminal, mislabelled Rejected(HandshakeFailed).  HandshakeFailed
// itself stays terminal.
// ===========================================================================
namespace {
// Drives the machine into Handshaking (first leg answered); returns the connect corrId.
int64_t intoHandshaking(FrameFixture& f, int64_t nowNs) {
    f.m.connect(nowNs);
    auto req = cc::decodeConnectRequest(f.io.last().data(), f.io.last().size());
    EXPECT_TRUE(req.has_value());
    const auto& hb = req->handshakeBytes;
    EXPECT_NE(f.srv.readInbound(reinterpret_cast<const uint8_t*>(hb.data()), hb.size()), cr::HsState::Failed);
    uint8_t out[16384];
    const std::size_t n = f.srv.writeOutbound(out, sizeof out);
    // Only the first half of the server's flight: the client's TLS needs more —
    // it stays Handshaking, awaiting a HandshakeContinueResponse.
    f.m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, FrameFixture::kSessionId, req->correlationId,
                          nowNs + 1, req->cipherList[0], std::string_view(reinterpret_cast<const char*>(out), n / 2));
    return req->correlationId;
}
}  // namespace

TEST(ClientCryptoRegression, HandshakeContinueInternalIsARetriableConnectFailureRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    const int64_t corr = intoHandshaking(f, 1'000'000'000LL);
    ASSERT_EQ(f.m.state(), sm::SessionState::Handshaking);
    f.m.onHandshakeContinueResponse(static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal), {}, corr,
                                    1'000'000'010LL);
    EXPECT_NE(f.m.state(), sm::SessionState::Rejected)
        << "a full broker table turned into a terminal TLS failure";
    EXPECT_NE(f.m.rejectCode(), static_cast<int16_t>(rune::protocol::client::ErrorCode::HandshakeFailed));
    EXPECT_FALSE(f.m.cryptoState().encrypted);
}

TEST(ClientCryptoRegression, HandshakeContinueHandshakeFailedStaysTerminalRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    const int64_t corr = intoHandshaking(f, 1'000'000'000LL);
    ASSERT_EQ(f.m.state(), sm::SessionState::Handshaking);
    f.m.onHandshakeContinueResponse(static_cast<int16_t>(rune::protocol::client::ErrorCode::HandshakeFailed), {},
                                    corr, 1'000'000'010LL);
    EXPECT_EQ(f.m.state(), sm::SessionState::Rejected);
}

// ===========================================================================
// DR client bugs fix round 3 (client-failover-review.md "Fix round 2 check":
// N-I1 + M-2).
// ===========================================================================

// M-2 (ruling): a handshake the broker never finishes in time is a TRANSIENT
// connect failure (a slow / recovering broker) — never a terminal verdict on its
// certificate.  Plain config: Disconnected (the seed failover moves on); a Live
// reconnectForever session: Backoff.  RED (before): terminal Rejected(HandshakeFailed).
TEST(ClientCryptoRegression, HandshakeTimeoutIsATransientConnectFailureRegression) {
    {
        FrameFixture f;
        ASSERT_TRUE(f.srv.valid());
        (void)intoHandshaking(f, 1'000'000'000LL);
        ASSERT_EQ(f.m.state(), sm::SessionState::Handshaking);
        f.m.tick(1'000'000'000LL + sm::SessionConfig{}.connectTimeoutNs + 2);
        EXPECT_EQ(f.m.state(), sm::SessionState::Disconnected);
        EXPECT_EQ(f.m.rejectCode(), 0);
        EXPECT_EQ(f.m.lastConnectError(), sm::SessionMachine::kHandshakeTimedOut);
        EXPECT_FALSE(sm::SessionMachine::nonRetriableConnectError(f.m.lastConnectError()));
        EXPECT_FALSE(f.m.cryptoState().encrypted);
        EXPECT_EQ(f.m.sessionId(), 0) << "the half-made broker session is abandoned";
    }
    {
        // A Live reconnectForever session: its fresh reconnect's handshake times out -> Backoff.
        FakeIo io;
        const std::string serverPem = slurp(testdataPath("server.pem"));
        const std::string serverKey = slurp(testdataPath("server.key"));
        const std::string caPem     = slurp(testdataPath("ca.pem"));
        auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
        sm::SessionConfig c;
        c.egressChannel = "aeron:udp?endpoint=client:40456";
        c.offerCrypto = true; c.tlsTrustPem = caPem; c.tlsExpectHost = "localhost";
        c.reconnectForeverAfterLive = true;
        sm::SessionMachine m(io, c);
        int64_t now = 1'000'000'000LL;
        m.connect(now);
        ASSERT_TRUE(driveHandshake(m, io, srv1, /*sessionId=*/42, now + 1));
        ASSERT_TRUE(m.everLive());
        m.forceReconnect(now += 1'000'000);   // a fresh connect (a failover move)
        auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
        ASSERT_TRUE(req.has_value());
        auto srv2 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
        const auto& hb = req->handshakeBytes;
        ASSERT_FALSE(hb.empty());
        ASSERT_NE(srv2.readInbound(reinterpret_cast<const uint8_t*>(hb.data()), hb.size()), cr::HsState::Failed);
        uint8_t out[16384];
        const std::size_t n = srv2.writeOutbound(out, sizeof out);
        m.onConnectResponse(0, -1, 43, req->correlationId, now + 1, req->cipherList[0],
                            std::string_view(reinterpret_cast<const char*>(out), n / 2));
        ASSERT_EQ(m.state(), sm::SessionState::Handshaking);
        m.tick(now + c.connectTimeoutNs + 2);
        EXPECT_EQ(m.state(), sm::SessionState::Backoff) << "a Live forever session backs off, never Rejected";
        EXPECT_EQ(m.rejectCode(), 0);
        EXPECT_EQ(m.lastConnectError(), sm::SessionMachine::kHandshakeTimedOut);
    }
}

namespace {
/// A Connected+encrypted reconnectForever session whose broker expired it; the resume (a
/// plaintext hint-only request) is answered SessionExpired for THAT connect: the machine
/// connects fresh — returns that request.
std::vector<uint8_t> freshConnectAfterAResumeRefused(FakeIo& io, sm::SessionMachine& m, int64_t& now) {
    namespace rpc = rune::protocol::client;
    m.onErrorResponse(static_cast<int16_t>(rpc::ErrorCode::SessionExpired), "", 0, now += 1'000'000);
    m.tick(now += 1'000'000);   // Resuming -> the resume ConnectRequest
    auto resume = cc::decodeConnectRequest(io.last().data(), io.last().size());
    EXPECT_TRUE(resume.has_value());
    EXPECT_EQ(m.state(), sm::SessionState::Connecting);
    const std::size_t before = io.sendCount();
    m.onErrorResponse(static_cast<int16_t>(rpc::ErrorCode::SessionExpired), "", resume->correlationId,
                      now += 1'000'000);
    EXPECT_GT(io.sendCount(), before) << "a fresh ConnectRequest follows the refused resume";
    return io.last();
}
}  // namespace

// N-I1: a connect that cannot resume on the same broker (the refused resume -> fresh
// connect; a shard failover move lands exactly here) negotiates crypto AFRESH: the
// request carries a new ClientHello, the old session's keys are gone, and a new
// handshake makes the session encrypted under NEW keys.  Both requireCrypto and
// offered-not-required.  RED (before): the request is plaintext and the session keeps
// the old keys; under requireCrypto the plaintext answer makes it Rejected.
TEST(ClientCryptoRegression, NonResumeReconnectNegotiatesCryptoAfreshRegression) {
    for (const bool require : {false, true}) {
        SCOPED_TRACE(require ? "requireCrypto" : "offered, not required");
        FakeIo io;
        const std::string serverPem = slurp(testdataPath("server.pem"));
        const std::string serverKey = slurp(testdataPath("server.key"));
        const std::string caPem     = slurp(testdataPath("ca.pem"));
        auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
        sm::SessionConfig c;
        c.egressChannel = "aeron:udp?endpoint=client:40456";
        c.offerCrypto = true; c.requireCrypto = require; c.tlsTrustPem = caPem; c.tlsExpectHost = "localhost";
        c.reconnectForeverAfterLive = true;
        sm::SessionMachine m(io, c);
        int64_t now = 1'000'000'000LL;
        m.connect(now);
        ASSERT_TRUE(driveHandshake(m, io, srv1, /*sessionId=*/42, now + 1));
        ASSERT_TRUE(m.cryptoState().encrypted);

        const auto fresh = freshConnectAfterAResumeRefused(io, m, now);
        auto req = cc::decodeConnectRequest(fresh.data(), fresh.size());
        ASSERT_TRUE(req.has_value());
        EXPECT_FALSE(req->handshakeBytes.empty()) << "the fresh connect carries a new ClientHello";
        EXPECT_FALSE(req->cipherList.empty());
        EXPECT_FALSE(m.cryptoState().encrypted) << "the old session's keys survived the fresh connect";

        // The new broker completes a NEW handshake: Connected, encrypted under the new keys.
        auto srv2 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
        const std::size_t start = io.sendCount() - 1;
        // Drive the legs manually: answer, then forward every new client leg.
        bool done = false;
        std::size_t deliveredFromIo = start;
        for (int leg = 0; leg < 20 && !done; ++leg) {
            if (deliveredFromIo >= io.sendCount()) break;
            const auto& frame = io.sends[deliveredFromIo++];
            std::string_view peer;
            int64_t corr = 0;
            uint8_t cipher = 0;
            if (leg == 0) {
                auto r0 = cc::decodeConnectRequest(frame.data(), frame.size());
                ASSERT_TRUE(r0.has_value());
                peer = r0->handshakeBytes; corr = r0->correlationId; cipher = r0->cipherList.empty() ? 0 : r0->cipherList[0];
            } else {
                auto hc = cc::decodeHandshakeContinue(frame.data(), frame.size());
                ASSERT_TRUE(hc.has_value());
                peer = hc->handshakeBytes; corr = hc->correlationId;
            }
            ASSERT_NE(srv2.readInbound(reinterpret_cast<const uint8_t*>(peer.data()), peer.size()), cr::HsState::Failed);
            uint8_t out[16384];
            const std::size_t n = srv2.writeOutbound(out, sizeof out);
            const std::string_view ov(reinterpret_cast<const char*>(out), n);
            if (leg == 0) m.onConnectResponse(0, -1, /*new id*/ 77, corr, now + 1, cipher, ov);
            else if (m.state() == sm::SessionState::Handshaking) m.onHandshakeContinueResponse(0, ov, corr, now + 1);
            done = m.state() == sm::SessionState::Connected && srv2.done();
        }
        ASSERT_TRUE(done) << "the fresh handshake completes";
        ASSERT_TRUE(m.cryptoState().encrypted);
        // The NEW broker can open what the client seals (the keys are the new session's).
        sm::CryptoState broker = brokerSideCryptoStateFor(srv2, m, 77);
        const auto frame = buildPlainProduceFrame(77, 9, "hello");
        uint8_t sealed[256];
        const std::size_t sn = cc::sealFrame(m.cryptoState(), frame.data(), frame.size(), sealed, sizeof sealed);
        ASSERT_GT(sn, 0u);
        uint8_t opened[256];
        EXPECT_GT(cc::openEgressFrame(broker, sealed, sn, opened, sizeof opened), 0u)
            << "the session is sealed under keys the new broker holds";
    }
}

// N-I1 (the resume half): a session that went silent is moved to ANOTHER broker (a shard
// failover re-point) — its resume hint and keys are meaningless there (a require_tls
// broker rejects a plaintext resume as HandshakeFailed, terminally; another broker would
// accept it plaintext while the client keeps the old keys).  After abandonResumeHint()
// the next request is a fresh connect with a fresh ClientHello.  RED (no abandon): a
// plaintext resume carrying the old session id.
TEST(ClientCryptoRegression, AMoveToAnotherBrokerAbandonsTheResumeHintAndNegotiatesAfreshRegression) {
    FakeIo io;
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    sm::SessionConfig c;
    c.egressChannel = "aeron:udp?endpoint=client:40456";
    c.offerCrypto = true; c.requireCrypto = true; c.tlsTrustPem = caPem; c.tlsExpectHost = "localhost";
    c.reconnectForeverAfterLive = true;
    sm::SessionMachine m(io, c);
    int64_t now = 1'000'000'000LL;
    m.connect(now);
    ASSERT_TRUE(driveHandshake(m, io, srv1, /*sessionId=*/42, now + 1));
    m.tick(now += 1'000'000);
    m.tick(now += c.responseTimeoutNs + 1);   // silent broker: stale -> Resuming
    ASSERT_EQ(m.state(), sm::SessionState::Resuming);
    m.abandonResumeHint();                    // the seed moved to another broker (ClientAgent seed move)
    const std::size_t before = io.sendCount();
    m.tick(now += 1'000'000);                 // the resume step: now a fresh connect
    ASSERT_GT(io.sendCount(), before);
    auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->sessionId, 0) << "no resume hint for another broker";
    EXPECT_FALSE(req->handshakeBytes.empty()) << "a fresh ClientHello, never a plaintext request";
    EXPECT_FALSE(m.cryptoState().encrypted) << "the old session's keys are gone";
}

// ===========================================================================
// DR client bugs fix round 4 (client-failover-review.md "Fix round 3 check":
// N3-I1 and the TLS handshake class).  The broker never resumes (handleConnect
// allocates a NEW session on every connect), so under offerCrypto EVERY
// ConnectRequest — a resume on the same broker included — negotiates afresh.
// ===========================================================================
namespace {
struct TlsMachine {
    FakeIo io;
    std::string serverPem = slurp(testdataPath("server.pem"));
    std::string serverKey = slurp(testdataPath("server.key"));
    std::string caPem     = slurp(testdataPath("ca.pem"));
    sm::SessionConfig c;
    std::optional<sm::SessionMachine> m;
    int64_t now = 1'000'000'000LL;
    explicit TlsMachine(bool require) {
        c.egressChannel = "aeron:udp?endpoint=client:40456";
        c.offerCrypto = true; c.requireCrypto = require; c.tlsTrustPem = caPem; c.tlsExpectHost = "localhost";
        c.reconnectForeverAfterLive = true;
        m.emplace(io, c);
    }
    cr::TlsSession server() { return cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str()); }
    /// Drive a NEW broker's handshake from the request at io.sends[from]; true = Connected + encrypted
    /// under keys that broker holds (a frame the client seals opens on that broker).
    bool completeFrom(std::size_t from, int64_t sid) {
        auto srv = server();
        std::size_t next = from;
        for (int leg = 0; leg < 20; ++leg) {
            if (next >= io.sendCount()) break;
            const auto& frame = io.sends[next++];
            std::string_view peer; int64_t corr = 0; uint8_t cipher = 0;
            if (leg == 0) {
                auto r = cc::decodeConnectRequest(frame.data(), frame.size());
                if (!r || r->handshakeBytes.empty() || r->cipherList.empty()) return false;
                peer = r->handshakeBytes; corr = r->correlationId; cipher = r->cipherList[0];
            } else {
                auto hc = cc::decodeHandshakeContinue(frame.data(), frame.size());
                if (!hc) return false;
                peer = hc->handshakeBytes; corr = hc->correlationId;
            }
            if (srv.readInbound(reinterpret_cast<const uint8_t*>(peer.data()), peer.size()) == cr::HsState::Failed)
                return false;
            uint8_t out[16384];
            const std::size_t n = srv.writeOutbound(out, sizeof out);
            const std::string_view ov(reinterpret_cast<const char*>(out), n);
            if (leg == 0) m->onConnectResponse(0, -1, sid, corr, now + 1, cipher, ov);
            else if (m->state() == sm::SessionState::Handshaking) m->onHandshakeContinueResponse(0, ov, corr, now + 1);
            if (m->state() == sm::SessionState::Connected && srv.done()) break;
        }
        if (m->state() != sm::SessionState::Connected || !srv.done() || !m->cryptoState().encrypted) return false;
        sm::CryptoState broker = brokerSideCryptoStateFor(srv, *m, sid);
        const auto plain = buildPlainProduceFrame(sid, 9, "hello");
        uint8_t sealed[256], opened[256];
        const std::size_t sn = cc::sealFrame(m->cryptoState(), plain.data(), plain.size(), sealed, sizeof sealed);
        return sn > 0 && cc::openEgressFrame(broker, sealed, sn, opened, sizeof opened) > 0;
    }
    /// The resume request just sent: carries a fresh ClientHello + the resume hint, and the old keys are gone.
    void expectFreshTlsResume(std::size_t before, int64_t oldSid) {
        ASSERT_GT(io.sendCount(), before) << "a resume ConnectRequest";
        auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
        ASSERT_TRUE(req.has_value());
        EXPECT_EQ(req->sessionId, oldSid) << "the resume hint stays in the request (the broker ignores it)";
        EXPECT_FALSE(req->handshakeBytes.empty()) << "a plaintext resume: a require_tls broker answers HandshakeFailed";
        EXPECT_FALSE(req->cipherList.empty());
        EXPECT_FALSE(m->cryptoState().encrypted) << "the old session's keys survived into the new session";
    }
};
}  // namespace

// N3-I1: a broker-sent SessionExpired (the F-A lease-reap notice after a client stall) on
// the SAME broker: the resume negotiates crypto afresh and the new session is encrypted
// under the new broker session's keys.  Both requireCrypto and offered-not-required.
// RED (round 3's `sessionId_ == 0` rule): a plaintext resume keeping the old keys.
TEST(ClientCryptoRegression, SessionExpiredResumeOnTheSameBrokerNegotiatesCryptoAfreshRegression) {
    for (const bool require : {true, false}) {
        SCOPED_TRACE(require ? "requireCrypto" : "offered, not required");
        TlsMachine t(require);
        t.m->connect(t.now);
        ASSERT_TRUE(t.completeFrom(0, 42));
        namespace rpc = rune::protocol::client;
        t.m->onErrorResponse(static_cast<int16_t>(rpc::ErrorCode::SessionExpired), "", 0, t.now += 1'000'000, 42);
        ASSERT_EQ(t.m->state(), sm::SessionState::Resuming);
        const std::size_t before = t.io.sendCount();
        t.m->tick(t.now += 1'000'000);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connecting);
        t.expectFreshTlsResume(before, 42);
        EXPECT_TRUE(t.completeFrom(t.io.sendCount() - 1, 43)) << "the same broker's new session is encrypted";
    }
}

// N3-I1: an own-stall loss (the broker went silent past responseTimeout; no failover move)
// resumes on the same broker: fresh TLS, never plaintext / the old keys.
TEST(ClientCryptoRegression, OwnStallResumeNegotiatesCryptoAfreshRegression) {
    for (const bool require : {true, false}) {
        SCOPED_TRACE(require ? "requireCrypto" : "offered, not required");
        TlsMachine t(require);
        t.m->connect(t.now);
        ASSERT_TRUE(t.completeFrom(0, 42));
        t.m->tick(t.now += 1'000'000);
        t.m->tick(t.now += t.c.responseTimeoutNs + 1);
        ASSERT_EQ(t.m->state(), sm::SessionState::Resuming);
        const std::size_t before = t.io.sendCount();
        t.m->tick(t.now += 1'000'000);
        t.expectFreshTlsResume(before, 42);
        EXPECT_TRUE(t.completeFrom(t.io.sendCount() - 1, 43));
    }
}

// Fix round 4: one TLS episode per ConnectRequest — a new request (a timeout retry under
// a new correlationId) carries a NEW ClientHello, while a fast retry of the SAME request
// re-sends that request's own.  RED (an episode kept across requests): the timeout retry
// replays the previous request's ClientHello.
TEST(ClientCryptoRegression, EveryConnectRequestCarriesItsOwnTlsEpisodeRegression) {
    TlsMachine t(/*require=*/true);
    t.m->connect(t.now);
    auto first = cc::decodeConnectRequest(t.io.last().data(), t.io.last().size());
    ASSERT_TRUE(first.has_value());
    const std::string hello1(first->handshakeBytes);
    ASSERT_FALSE(hello1.empty());
    t.m->tick(t.now + t.c.connectTimeoutNs + 1);   // no answer: a timeout retry (a new request)
    auto second = cc::decodeConnectRequest(t.io.last().data(), t.io.last().size());
    ASSERT_TRUE(second.has_value());
    ASSERT_NE(second->correlationId, first->correlationId);
    EXPECT_FALSE(second->handshakeBytes.empty());
    EXPECT_NE(std::string(second->handshakeBytes), hello1) << "a new request replayed the old ClientHello";
    // And the new request's handshake completes.
    t.now += t.c.connectTimeoutNs + 2;
    EXPECT_TRUE(t.completeFrom(t.io.sendCount() - 1, 42));
}

// Fix round 4: crypto REQUIRED and the local TLS cannot be built (a broken trust bundle):
// never a plaintext ConnectRequest — terminal Rejected(HandshakeFailed).  RED (before):
// a plaintext request is sent ("degrade" fallback) and the session waits for its answer.
TEST(ClientCryptoRegression, RequiredCryptoWithoutLocalTlsNeverSendsAPlaintextConnectRegression) {
    FakeIo io;
    sm::SessionConfig c;
    c.egressChannel = "aeron:udp?endpoint=client:40456";
    c.offerCrypto = true; c.requireCrypto = true;
    c.tlsTrustPem = std::string("-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n");
    c.tlsExpectHost = "localhost";
    if (cr::TlsSession::makeClient(c.tlsTrustPem->c_str(), "localhost").valid())
        GTEST_SKIP() << "this TLS build accepts the broken bundle";
    sm::SessionMachine m(io, c);
    m.connect(1'000'000'000LL);
    for (std::size_t i = 0; i < io.sendCount(); ++i) {
        auto r = cc::decodeConnectRequest(io.sends[i].data(), io.sends[i].size());
        EXPECT_FALSE(r.has_value() && r->handshakeBytes.empty()) << "a plaintext ConnectRequest left";
    }
    m.tick(1'000'000'001LL);
    EXPECT_EQ(m.state(), sm::SessionState::Rejected);
    EXPECT_EQ(m.rejectCode(), static_cast<int16_t>(rune::protocol::client::ErrorCode::HandshakeFailed));
}

// ===========================================================================
// DR client bugs fix round 5 (client-failover-review.md "Fix round 4 check").
// ===========================================================================

// N4-I1, superseded by the PR #476 review (issues #14 / #469): the broker failed the handshake on its FINAL leg after
// this machine already went Connected (a TLS 1.3 client is done once it has sent its Finished).  The frame is in clear
// and names only guessable ids, so: before the broker proved the keys (no sealed frame opened yet) it is a transient
// connect failure - keys cleared, a fresh TLS episode within the connect budget, never terminal; once a sealed frame
// opened it is a forgery and ignored.  Only a frame naming this session's current id AND its connect corrId with code
// HandshakeFailed counts.  RED: a terminal Rejected (the old ruling), or ignoring it before the keys are proven.
TEST(ClientCryptoRegression, FinalLegHandshakeFailedAfterConnectedIsRetriedThenIgnoredOnceKeysAreProvenRegression) {
    namespace rpc = rune::protocol::client;
    const auto hf = static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed);
    {
        FrameFixture f;
        ASSERT_TRUE(f.established(1'000'000'000LL));
        const std::size_t sentBefore = f.io.sendCount();
        f.m.onHandshakeContinueResponse(hf, {}, f.m.correlationId(), 1'000'000'100LL, f.m.sessionId());
        EXPECT_EQ(f.m.state(), sm::SessionState::Connecting) << "retried as a fresh episode";
        EXPECT_NE(f.m.state(), sm::SessionState::Rejected);
        EXPECT_EQ(f.m.lastConnectError(), hf);
        EXPECT_FALSE(f.m.cryptoState().encrypted) << "the keys of a session the broker never promoted survived";
        EXPECT_GT(f.io.sendCount(), sentBefore);
    }
    {
        FrameFixture f;
        ASSERT_TRUE(f.established(1'000'000'000LL));
        f.m.onSealedFrameOpened();   // a sealed answer opened: the broker proved the keys
        f.m.onHandshakeContinueResponse(hf, {}, f.m.correlationId(), 1'000'000'100LL, f.m.sessionId());
        EXPECT_EQ(f.m.state(), sm::SessionState::Connected) << "a clear verdict after proven keys is a forgery";
        EXPECT_TRUE(f.m.cryptoState().encrypted);
    }
    struct Neg { const char* what; int16_t code; int64_t corrDelta; int64_t sid; };
    const Neg negs[] = {
        {"wrong corrId", hf, 1, FrameFixture::kSessionId},
        {"wrong session id", hf, 0, FrameFixture::kSessionId + 1},
        {"session id 0", hf, 0, 0},
        {"code Internal", static_cast<int16_t>(rpc::ErrorCode::Internal), 0, FrameFixture::kSessionId},
    };
    for (const auto& n : negs) {
        SCOPED_TRACE(n.what);
        FrameFixture f;
        ASSERT_TRUE(f.established(1'000'000'000LL));
        f.m.onHandshakeContinueResponse(n.code, {}, f.m.correlationId() + n.corrDelta, 1'000'000'100LL, n.sid);
        EXPECT_EQ(f.m.state(), sm::SessionState::Connected);
        EXPECT_TRUE(f.m.cryptoState().encrypted);
    }
}

// N4-M1: a seed move while CONNECTING (abandonResumeHint) keeps the in-flight episode — the
// next new request replaces it.  RED (before): the episode is dropped, and with
// requireCrypto tickConnecting turns "no TLS" into a terminal Rejected.
TEST(ClientCryptoRegression, AbandonResumeHintWhileConnectingKeepsTheHandshakeRegression) {
    TlsMachine t(/*require=*/true);
    t.m->connect(t.now);
    ASSERT_EQ(t.m->state(), sm::SessionState::Connecting);
    t.m->abandonResumeHint();
    t.m->tick(t.now + 1);
    EXPECT_EQ(t.m->state(), sm::SessionState::Connecting) << "a transient move became a terminal Rejected";
    EXPECT_EQ(t.m->rejectCode(), 0);
    // The episode still completes the handshake for its request.
    EXPECT_TRUE(t.completeFrom(t.io.sendCount() - 1, 42));
}

// ===========================================================================
// Issue #115: with no tlsExpectHost the certificate name is the host of the
// session's endpoint - and a pre-TLS (unauthenticated) redirect never moves it.
// ===========================================================================
namespace {
sm::SessionConfig derivedHostCfg(const std::string& caPem) {
    sm::SessionConfig c;
    c.egressChannel = "aeron:udp?endpoint=client:40456";
    c.offerCrypto   = true;
    c.requireCrypto = true;
    c.tlsTrustPem   = caPem;   // tlsExpectHost left empty: derived from the endpoint
    return c;
}
}  // namespace

// The test certificate names localhost and 127.0.0.1: an endpoint at 127.0.0.1 verifies, one at another name does not
// (even under the pinned CA).  RED: makeClient with the configured (empty) tlsExpectHost - no name check, both pass.
TEST(ClientCryptoRegression, TheCertificateNameIsTheSessionEndpointsHostRegression) {
    const std::string ca = slurp(testdataPath("ca.pem"));
    for (const auto& [endpoint, ok] : std::vector<std::pair<std::string, bool>>{
             {"aeron:udp?endpoint=127.0.0.1:40200", true},
             {"aeron:udp?endpoint=localhost:40200|mtu=1408", true},
             {"aeron:udp?endpoint=other.example:40200", false}}) {
        FakeIo io;
        io.channel = endpoint;
        auto srv = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                              slurp(testdataPath("server.key")).c_str());
        sm::SessionMachine m(io, derivedHostCfg(ca));
        m.connect(1'000'000'000LL);
        EXPECT_EQ(driveHandshake(m, io, srv, 42, 1'000'000'001LL), ok) << endpoint;
        EXPECT_EQ(m.cryptoState().encrypted, ok) << endpoint;
    }
}

// A redirect answered before TLS (ConnectResponse / ErrorResponse in Connecting) is unauthenticated: the session follows
// it, but keeps verifying the name of the endpoint it was redirected FROM - a forged redirect to a host the attacker
// holds a certificate for never verifies.  Here the redirect names a host the certificate does not: the handshake
// still succeeds against the pinned original name.  RED: drop notePreTlsRedirect_ (the name follows the redirect).
TEST(ClientCryptoRegression, APreTlsRedirectNeverMovesTheCertificateNameRegression) {
    const std::string ca = slurp(testdataPath("ca.pem"));
    for (const bool viaErrorResponse : {false, true}) {
        FakeIo io;
        io.channel = "aeron:udp?endpoint=127.0.0.1:40200";
        auto srv = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                              slurp(testdataPath("server.key")).c_str());
        sm::SessionMachine m(io, derivedHostCfg(ca));
        constexpr int64_t T0 = 1'000'000'000LL;
        m.connect(T0);
        auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
        ASSERT_TRUE(req.has_value());
        if (viaErrorResponse)
            m.onErrorResponse(static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect), "elsewhere.example:40200",
                              req->correlationId, T0 + 1);
        else
            m.onConnectResponseRedirect("elsewhere.example:40200", req->correlationId, T0 + 1);
        ASSERT_EQ(io.channel, "aeron:udp?endpoint=elsewhere.example:40200") << "the session followed the redirect";
        io.sends.erase(io.sends.begin(), io.sends.end() - 1);   // drive the handshake from the resent Connect
        EXPECT_TRUE(driveHandshake(m, io, srv, 42, T0 + 2)) << "verified against 127.0.0.1, not the redirect's name";
        // A trusted move (metadata, a sealed redirect, the bootstrap) changes the endpoint: its own host counts again.
        io.channel = "aeron:udp?endpoint=other.example:40200";
        io.sends.clear();
        m.forceReconnect(T0 + 3);
        if (!io.sends.empty()) {
            auto srv2 = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                                   slurp(testdataPath("server.key")).c_str());
            EXPECT_FALSE(driveHandshake(m, io, srv2, 43, T0 + 4)) << "other.example is checked (and not in the cert)";
        }
    }
}

// ===========================================================================
// Issue #14: with crypto required, an answer that arrives before the session's
// TLS keys exist is unauthenticated - never a terminal Rejected.
// ===========================================================================

// RED: delete the requireCrypto branch at the top of onConnectError_ (ProtocolVersion / HandshakeFailed /
// AuthenticationFailed then go terminal Rejected under reconnectForeverAfterLive), or route onHandshakeFailed_ straight
// to rejectHandshake_.
TEST(SessionMachineAuthRegression, APreTlsRefusalNeverRejectsRegression) {
    namespace rpc = rune::protocol::client;
    const std::string ca = slurp(testdataPath("ca.pem"));
    constexpr int64_t T0 = 1'000'000'000LL;
    // A ConnectResponse refusal, every code (the non-retriable ones included).
    for (const auto code : {rpc::ErrorCode::ProtocolVersion, rpc::ErrorCode::HandshakeFailed,
                            rpc::ErrorCode::AuthenticationFailed, rpc::ErrorCode::Internal}) {
        for (const bool required : {true, false}) {
            FakeIo io;
            io.channel = "aeron:udp?endpoint=127.0.0.1:40200";
            sm::SessionConfig cfg = derivedHostCfg(ca);
            cfg.requireCrypto = required;
            cfg.reconnectForeverAfterLive = true;
            sm::SessionMachine m(io, cfg);
            m.connect(T0);
            auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
            ASSERT_TRUE(req.has_value());
            m.onConnectResponse(static_cast<int16_t>(code), -1, 0, req->correlationId, T0 + 1);
            const bool nonRetriable = code != rpc::ErrorCode::Internal;
            if (required)
                EXPECT_NE(m.state(), sm::SessionState::Rejected) << "code " << int(code) << ": unauthenticated, never a verdict";
            else if (nonRetriable)
                EXPECT_EQ(m.state(), sm::SessionState::Rejected) << "control: optional crypto keeps the old verdict";
            if (required) EXPECT_EQ(m.lastConnectError(), static_cast<int16_t>(code));
        }
    }
    // A HandshakeContinueResponse{HandshakeFailed} mid-handshake, and a failed verify of the server's flight.
    {
        FakeIo io;
        io.channel = "aeron:udp?endpoint=127.0.0.1:40200";
        sm::SessionConfig cfg = derivedHostCfg(ca);
        cfg.reconnectForeverAfterLive = true;
        sm::SessionMachine m(io, cfg);
        auto srv = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                              slurp(testdataPath("server.key")).c_str());
        m.connect(T0);
        auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
        ASSERT_TRUE(req.has_value());
        (void)srv.readInbound(reinterpret_cast<const uint8_t*>(req->handshakeBytes.data()), req->handshakeBytes.size());
        uint8_t out[16384];
        const std::size_t n = srv.writeOutbound(out, sizeof out);
        m.onConnectResponse(0, -1, 42, req->correlationId, T0 + 1, req->cipherList[0],
                            std::string_view(reinterpret_cast<const char*>(out), n / 2));   // half a flight
        ASSERT_EQ(m.state(), sm::SessionState::Handshaking);
        m.onHandshakeContinueResponse(static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed), {}, req->correlationId, T0 + 2);
        EXPECT_NE(m.state(), sm::SessionState::Rejected) << "a mid-handshake refusal is unauthenticated";
        EXPECT_EQ(m.lastConnectError(), static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed));
    }
    {
        FakeIo io;
        io.channel = "aeron:udp?endpoint=127.0.0.1:40200";
        sm::SessionConfig cfg = derivedHostCfg("");   // trust nothing: the server's flight fails to verify
        cfg.reconnectForeverAfterLive = true;
        sm::SessionMachine m(io, cfg);
        auto srv = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                              slurp(testdataPath("server.key")).c_str());
        m.connect(T0);
        EXPECT_FALSE(driveHandshake(m, io, srv, 42, T0 + 1));
        EXPECT_NE(m.state(), sm::SessionState::Rejected) << "a forged server flight must not end the client for good";
    }
}

// ===========================================================================
// Adversarial review of PR #476 (issue #115): the pre-TLS redirect pin must
// survive the Backoff walk.  An io that moves exactly as ClientAgent's seed
// does (SessionEndpointIo::retargetToBootstrap / retargetToLastKnownLeader:
// leaving a redirected endpoint for the bootstrap stores it as the last-known
// leader; the next odd attempt >= 3 returns to it from the bootstrap).
// ===========================================================================
namespace {
struct AgentLikeIo final : sm::SessionIo {
    std::vector<std::vector<uint8_t>> sends;
    std::string bootstrap, channel, lastKnown;
    bool sendFrame(const uint8_t* buf, std::size_t len) override { sends.emplace_back(buf, buf + len); return true; }
    [[nodiscard]] std::size_t sendCount() const { return sends.size(); }
    bool retarget(const std::string& ep) noexcept override {   // ClientAgent::retargetSeedOnConnectRedirect_
        channel = ep.rfind("aeron:", 0) == 0 ? ep : "aeron:udp?endpoint=" + ep;
        return true;
    }
    bool retargetToBootstrap() noexcept override {             // ClientAgent::retargetSeedToBootstrap_
        if (channel == bootstrap) return true;
        lastKnown = channel;
        channel = bootstrap;
        return true;
    }
    bool retargetToLastKnownLeader() noexcept override {       // ClientAgent::retargetSeedToLastKnownLeader_
        if (lastKnown.empty() || channel != bootstrap) return true;
        channel = lastKnown;
        return true;
    }
    [[nodiscard]] std::string ingressChannel() const override { return channel; }
};
bool helloNames(const std::vector<uint8_t>& frame, const std::string& name) {
    const auto req = cc::decodeConnectRequest(frame.data(), frame.size());
    return req && req->handshakeBytes.find(name) != std::string_view::npos;   // the SNI rides the ClientHello in clear
}
}  // namespace

// A forged pre-TLS redirect moves a Live session to an attacker endpoint (the pin keeps the bootstrap's name there).
// Its connects fail; Backoff goes back to the bootstrap (which stores the attacker endpoint as the last-known leader)
// and then, on the next odd attempt, returns to it.  Every ClientHello sent to the attacker endpoint must still name
// the ORIGINAL host - never the attacker's own, for which it may hold a publicly trusted certificate.
TEST(ClientCryptoRegression, APreTlsRedirectPinSurvivesTheBackoffWalkAdversarialRegression) {
    namespace rpc = rune::protocol::client;
    const std::string ca = slurp(testdataPath("ca.pem"));
    AgentLikeIo io;
    io.bootstrap = io.channel = "aeron:udp?endpoint=localhost:40200";
    sm::SessionConfig cfg = derivedHostCfg(ca);
    cfg.reconnectForeverAfterLive = true;
    cfg.maxConnectRetries = 0;
    sm::SessionMachine m(io, cfg);
    int64_t now = 1'000'000'000LL;

    // Live once over TLS against the bootstrap (the certificate names localhost).
    m.connect(now);
    {
        auto srv = cr::TlsSession::makeServer(slurp(testdataPath("server.pem")).c_str(),
                                              slurp(testdataPath("server.key")).c_str());
        ASSERT_TRUE(driveHandshake(m, io, srv, 42, ++now));
    }
    ASSERT_EQ(m.state(), sm::SessionState::Connected);

    // A reconnect episode; the attacker forges the pre-TLS redirect.
    io.sends.clear();
    m.forceReconnect(++now);
    auto req = cc::decodeConnectRequest(io.sends.back().data(), io.sends.back().size());
    ASSERT_TRUE(req.has_value());
    m.onConnectResponseRedirect("evil.example:40300", req->correlationId, ++now);
    ASSERT_EQ(io.channel, "aeron:udp?endpoint=evil.example:40300");

    const std::string evil = "aeron:udp?endpoint=evil.example:40300";
    int helloesToEvil = 0;
    std::size_t seen = 0;
    for (int round = 0; round < 12; ++round) {
        // Every new ClientHello sent while the session addresses the attacker endpoint must name the bootstrap's host.
        for (; seen < io.sends.size(); ++seen) {
            const auto r = cc::decodeConnectRequest(io.sends[seen].data(), io.sends[seen].size());
            if (!r || r->handshakeBytes.empty() || io.channel != evil) continue;
            ++helloesToEvil;
            EXPECT_TRUE(helloNames(io.sends[seen], "localhost")) << "round " << round << ": the pin was dropped";
            EXPECT_FALSE(helloNames(io.sends[seen], "evil.example"))
                << "round " << round << ": the ClientHello to the redirect target names the attacker's host";
        }
        // Fail this connect (the attacker cannot complete TLS for the pinned name), then let Backoff fire.
        if (m.state() == sm::SessionState::Connecting && !io.sends.empty()) {
            const auto r = cc::decodeConnectRequest(io.sends.back().data(), io.sends.back().size());
            ASSERT_TRUE(r.has_value());
            m.onConnectResponse(static_cast<int16_t>(rpc::ErrorCode::HandshakeFailed), -1, 0, r->correlationId, ++now);
        }
        now += 2'000'000'000LL;   // past any backoff delay (capped at 1 s)
        m.tick(now);
    }
    EXPECT_GE(helloesToEvil, 2) << "the walk returned to the redirect target at least once after the bootstrap";
}

// Re-check of PR #476 fix round 1: the redirect bindings are capped (16).  An attacker forges more redirects than
// that, each to a fresh endpoint of its own, so the first bindings are evicted.  Coming back to ANY of its endpoints -
// evicted or not - the ClientHello must still name the original host: overflow fails closed to the anchor, never to
// the endpoint's own (attacker) host.
TEST(ClientCryptoRegression, RedirectBindingOverflowFailsClosedToTheAnchorAdversarialRegression) {
    const std::string ca = slurp(testdataPath("ca.pem"));
    AgentLikeIo io;
    io.bootstrap = io.channel = "aeron:udp?endpoint=localhost:40200";
    sm::SessionConfig cfg = derivedHostCfg(ca);
    cfg.maxRedirects = 1000;
    sm::SessionMachine m(io, cfg);
    int64_t now = 1'000'000'000LL;
    m.connect(now);
    std::vector<std::string> evil;
    for (int i = 0; i < 40; ++i) {
        const auto req = cc::decodeConnectRequest(io.sends.back().data(), io.sends.back().size());
        ASSERT_TRUE(req.has_value());
        evil.push_back("evil" + std::to_string(i) + ".example:" + std::to_string(41000 + i));
        m.onConnectResponseRedirect(evil.back(), req->correlationId, ++now);
        ASSERT_TRUE(helloNames(io.sends.back(), "localhost")) << "redirect " << i;
        ASSERT_FALSE(helloNames(io.sends.back(), "evil")) << "redirect " << i;
    }
    // Return to every attacker endpoint (the Backoff walk, a resume, a failover re-point): never its own name.
    for (std::size_t i = 0; i < evil.size(); ++i) {
        io.channel = "aeron:udp?endpoint=" + evil[i];
        io.sends.clear();
        m.forceReconnect(++now);
        ASSERT_FALSE(io.sends.empty());
        EXPECT_TRUE(helloNames(io.sends.back(), "localhost")) << evil[i];
        EXPECT_FALSE(helloNames(io.sends.back(), "evil")) << evil[i] << ": the attacker's own name is verified";
    }
}

// ===========================================================================
// Issue #116: per-receiver client->broker keys and sequence spaces.  A session whose client (>= 12) and broker (>= 7)
// both speak receiver keys seals each c2b frame for ONE receiver (0 = the gateway, w+1 = worker w): the receiver id
// rides CryptoSubHeader.senderId (in the AAD), the key is deriveReceiverKey(c2b, id) and each receiver has its own
// seq space.  A frame sealed for one receiver never opens at another, so a replay to another endpoint is refused.
// ===========================================================================
namespace {
namespace rpc116 = rune::protocol::client;
/// Opens `frame` under `key` (AAD = header + subheader) - what a receiver holding exactly that key does.
bool opensUnder(const cr::AeadKey& key, const uint8_t prefix[4], cr::Cipher c, const std::vector<uint8_t>& frame) {
    const auto sub = rpc116::CryptoSubHeader::decode(frame.data() + rpc116::MessageHeader::ENCODED_SIZE,
                                                     frame.size() - rpc116::MessageHeader::ENCODED_SIZE);
    if (!sub) return false;
    cr::AeadCipher opener(c, key, prefix);
    std::vector<uint8_t> out(frame.size());
    return opener.open(sub->seq, frame.data(), rpc116::kCryptoHeaderBytes, frame.data() + rpc116::kCryptoHeaderBytes,
                       frame.size() - rpc116::kCryptoHeaderBytes, out.data()) >= 0;
}
std::vector<uint8_t> sealFor(sm::CryptoState& c, uint8_t receiverId, int64_t corr) {
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, corr, "receiver-bound");
    std::vector<uint8_t> out(plain.size() + rpc116::kCryptoOverhead);
    const std::size_t n = cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), receiverId);
    out.resize(n);
    return out;
}
rpc116::CryptoSubHeader subOf(const std::vector<uint8_t>& f) {
    return *rpc116::CryptoSubHeader::decode(f.data() + rpc116::MessageHeader::ENCODED_SIZE,
                                            f.size() - rpc116::MessageHeader::ENCODED_SIZE);
}
}  // namespace

// RED: sealFrame ignores receiverId (every frame senderId 0 under the raw c2b key).
TEST(ClientCryptoRegression, ASealedFrameNamesItsReceiverAndOpensOnlyUnderItsKeyRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys));
    ASSERT_TRUE(f.m.cryptoState().receiverKeys) << "client 12 x broker 7: receiver keys";
    const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);

    const auto w1 = sealFor(f.m.cryptoState(), 2, 1);   // worker 1
    ASSERT_FALSE(w1.empty());
    EXPECT_EQ(subOf(w1).senderId, 2);
    cr::AeadKey r1{}, r2{};
    ASSERT_TRUE(cr::deriveReceiverKey(keys.c2b, 1, r1));
    ASSERT_TRUE(cr::deriveReceiverKey(keys.c2b, 2, r2));
    EXPECT_TRUE(opensUnder(r2, keys.c2bPrefix, keys.cipher, w1)) << "worker 1 (receiver 2) opens it";
    EXPECT_FALSE(opensUnder(keys.c2b, keys.c2bPrefix, keys.cipher, w1)) << "the gateway (raw c2b) never opens it";
    EXPECT_FALSE(opensUnder(r1, keys.c2bPrefix, keys.cipher, w1)) << "worker 0 (receiver 1) never opens it";

    const auto gw = sealFor(f.m.cryptoState(), 0, 2);
    EXPECT_EQ(subOf(gw).senderId, 0);
    EXPECT_TRUE(opensUnder(keys.c2b, keys.c2bPrefix, keys.cipher, gw)) << "receiver 0 stays the raw c2b key";
    EXPECT_FALSE(opensUnder(r2, keys.c2bPrefix, keys.cipher, gw));
}

// RED: one shared sendSeq (gateway 1, 2 then worker 3).
TEST(ClientCryptoRegression, EachReceiverHasItsOwnSeqSpaceRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys));
    auto& c = f.m.cryptoState();
    EXPECT_EQ(subOf(sealFor(c, 0, 1)).seq, 1u);
    EXPECT_EQ(subOf(sealFor(c, 0, 2)).seq, 2u);
    EXPECT_EQ(subOf(sealFor(c, 1, 3)).seq, 1u) << "worker 0's first frame starts its own seq space";
    EXPECT_EQ(subOf(sealFor(c, 1, 4)).seq, 2u);
    EXPECT_EQ(subOf(sealFor(c, 64, 5)).seq, 1u) << "the top worker (64 workers) has a slot";
    EXPECT_EQ(subOf(sealFor(c, 0, 6)).seq, 3u);
    EXPECT_TRUE(sealFor(c, 65, 7).empty()) << "a receiver id above the broker's max worker count is refused";
    EXPECT_TRUE(sealFor(c, 255, 8).empty());
    EXPECT_EQ(subOf(sealFor(c, 1, 9)).seq, 3u) << "a refused seal consumes no seq anywhere";
}

// A legacy session (broker below 7, or client below 12) seals exactly as before #116: senderId 0, the raw c2b key and
// one seq space, whatever receiver id the caller names.
TEST(ClientCryptoRegression, ALegacySessionSealsByteIdenticallyRegression) {
    for (const bool oldBroker : {true, false}) {
        FrameFixture f;
        ASSERT_TRUE(f.srv.valid());
        if (oldBroker) {
            ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys - 1));
        } else {
            // A client pinned below 12 against a new broker.
            sm::SessionConfig cfg;
            cfg.egressChannel = "aeron:udp?endpoint=client:40456";
            cfg.offerCrypto   = true;
            cfg.tlsTrustPem   = f.caPem;
            cfg.tlsExpectHost = "localhost";
            cfg.clientVersion = rpc116::kClientVersionReceiverKeys - 1;
            FakeIo io;
            sm::SessionMachine m(io, cfg);
            m.connect(1'000'000'000LL);
            ASSERT_TRUE(driveHandshake(m, io, f.srv, FrameFixture::kSessionId, 1'000'000'001LL, nullptr,
                                       rpc116::kBrokerVersionReceiverKeys));
            EXPECT_FALSE(m.cryptoState().receiverKeys) << "client 11 x broker 7: legacy";
            const auto keys = f.srv.exportKeys(m.cryptoState().cipher, FrameFixture::kSessionId);
            const auto a = sealFor(m.cryptoState(), 3, 1);
            EXPECT_EQ(subOf(a).senderId, 0);
            EXPECT_EQ(subOf(a).seq, 1u);
            EXPECT_TRUE(opensUnder(keys.c2b, keys.c2bPrefix, keys.cipher, a));
            continue;
        }
        EXPECT_FALSE(f.m.cryptoState().receiverKeys) << "client 12 x broker 6: legacy";
        const auto keys = f.srv.exportKeys(f.m.cryptoState().cipher, FrameFixture::kSessionId);
        // The pre-#116 bytes: senderId 0, seq 1 then 2, sealed under the raw c2b key.
        const auto a = sealFor(f.m.cryptoState(), 3, 1);
        const auto b = sealFor(f.m.cryptoState(), 0, 2);
        cr::AeadCipher ref(keys.cipher, keys.c2b, keys.c2bPrefix);
        const auto expect = [&](uint64_t seq, int64_t corr) {
            const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, corr, "receiver-bound");
            std::vector<uint8_t> out(plain.size() + rpc116::kCryptoOverhead);
            std::memcpy(out.data(), plain.data(), rpc116::MessageHeader::ENCODED_SIZE);
            rpc116::CryptoSubHeader sub;
            sub.cipher = static_cast<uint8_t>(keys.cipher);
            sub.seq = seq;
            sub.encode(out.data() + rpc116::MessageHeader::ENCODED_SIZE);
            const std::size_t n = ref.seal(seq, out.data(), rpc116::kCryptoHeaderBytes,
                                           plain.data() + rpc116::MessageHeader::ENCODED_SIZE,
                                           plain.size() - rpc116::MessageHeader::ENCODED_SIZE,
                                           out.data() + rpc116::kCryptoHeaderBytes);
            out.resize(rpc116::kCryptoHeaderBytes + n);
            return out;
        };
        EXPECT_EQ(a, expect(1, 1)) << "receiver id ignored: byte-identical to the legacy seal";
        EXPECT_EQ(b, expect(2, 2));
    }
}

// ── Adversarial review of PR #528 (issue #116) ──────────────────────────────────────────────────────────────────────
#include "../support/AllocCounter.h"

// Input bounds: every receiver id 1..64 seals under ITS OWN derived key with its own seq space (two receivers' first
// frames never share a ciphertext), and every id above the broker's 64 workers (65, 128, 255) is refused with no seq
// consumed anywhere.  RED (mutation): selectSendSlot indexing without the bound check, or one shared slot.
TEST(ClientCryptoRegression, EveryReceiverIdFromOneToSixtyFourHasItsOwnKeyAndAboveIsRefusedAdversarialRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys));
    auto& c = f.m.cryptoState();
    ASSERT_TRUE(c.receiverKeys);
    const auto keys = f.srv.exportKeys(c.cipher, FrameFixture::kSessionId);
    for (int id = 1; id <= 64; ++id) {
        SCOPED_TRACE(id);
        const auto fr = sealFor(c, static_cast<uint8_t>(id), id);
        ASSERT_FALSE(fr.empty());
        EXPECT_EQ(subOf(fr).senderId, id);
        EXPECT_EQ(subOf(fr).seq, 1u) << "each receiver's first frame is seq 1 of its own space";
        cr::AeadKey mine{}, next{};
        ASSERT_TRUE(cr::deriveReceiverKey(keys.c2b, static_cast<uint8_t>(id), mine));
        ASSERT_TRUE(cr::deriveReceiverKey(keys.c2b, static_cast<uint8_t>(id == 64 ? 1 : id + 1), next));
        EXPECT_TRUE(opensUnder(mine, keys.c2bPrefix, keys.cipher, fr));
        EXPECT_FALSE(opensUnder(next, keys.c2bPrefix, keys.cipher, fr)) << "a neighbouring worker never opens it";
        EXPECT_FALSE(opensUnder(keys.c2b, keys.c2bPrefix, keys.cipher, fr)) << "the gateway never opens it";
    }
    for (const int id : {65, 66, 128, 254, 255}) {
        EXPECT_TRUE(sealFor(c, static_cast<uint8_t>(id), 1000 + id).empty()) << id << ": above 64 workers, refused";
    }
    EXPECT_EQ(subOf(sealFor(c, 0, 2000)).seq, 1u) << "no refused seal consumed the gateway's seq";
    EXPECT_EQ(subOf(sealFor(c, 64, 2001)).seq, 2u) << "no refused seal consumed worker 63's seq";
}

// Crash and restart: a fresh connect episode (new TLS keys) drops EVERY derived receiver slot and its seq - a slot kept
// from the previous episode would seal under the OLD key (the worker never opens it again) and continue the old seq.
// After the fresh handshake receiver 1 seals under the NEW session's derived key starting at seq 1.
// RED (mutation): keep workerSend across episodes (reset only send/sendSeq).
TEST(ClientCryptoRegression, AFreshEpisodeRederivesEveryReceiverSlotAdversarialRegression) {
    FakeIo io;
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    sm::SessionConfig cfg;
    cfg.egressChannel = "aeron:udp?endpoint=client:40456";
    cfg.offerCrypto = true; cfg.tlsTrustPem = caPem; cfg.tlsExpectHost = "localhost";
    cfg.reconnectForeverAfterLive = true;
    sm::SessionMachine m(io, cfg);
    int64_t now = 1'000'000'000LL;
    m.connect(now);
    ASSERT_TRUE(driveHandshake(m, io, srv1, /*sessionId=*/42, now + 1, nullptr, rpc116::kBrokerVersionReceiverKeys));
    ASSERT_TRUE(m.cryptoState().receiverKeys);
    for (int i = 0; i < 3; ++i) ASSERT_FALSE(sealFor(m.cryptoState(), 1, 10 + i).empty());
    ASSERT_TRUE(m.cryptoState().workerSend[0].cipher.has_value());
    ASSERT_EQ(m.cryptoState().workerSend[0].sendSeq, 3u);

    (void)freshConnectAfterAResumeRefused(io, m, now);
    EXPECT_FALSE(m.cryptoState().encrypted);
    EXPECT_FALSE(m.cryptoState().receiverKeys);
    EXPECT_FALSE(m.cryptoState().workerSend[0].cipher.has_value()) << "an old episode's receiver cipher survived";
    EXPECT_EQ(m.cryptoState().workerSend[0].sendSeq, 0u) << "an old episode's receiver seq survived";
    EXPECT_TRUE(sealFor(m.cryptoState(), 1, 20).empty()) << "no keys between episodes: nothing is sealed";
}

// Hot path: once a receiver's slot is derived, sealing for it (and for the gateway) allocates nothing - the slot is an
// array index, the cipher is reused.  RED (mutation): derive the receiver key / build the cipher per frame.
TEST(ClientCryptoRegression, SealingForAWorkerReceiverAllocatesNothingAfterWarmUpAdversarialRegression) {
    // #535: OpenSSL allocates through CRYPTO_malloc, which the replaced operator new never sees; OpenSslAllocCounter.cpp
    // routes it into the same window.  The gate is only meaningful if that install won the race with OpenSSL's first use.
    ASSERT_TRUE(openSslAllocCounterInstalled()) << "CRYPTO_set_mem_functions was refused: OpenSSL allocated first";
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys));
    auto& c = f.m.cryptoState();
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 1, "zero-alloc-receiver-bound");
    std::vector<uint8_t> out(plain.size() + rpc116::kCryptoOverhead);
    for (const uint8_t id : {uint8_t{0}, uint8_t{1}, uint8_t{7}, uint8_t{64}})   // warm-up: the cold derive, once
        ASSERT_GT(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), id), 0u);
    alloc_counter::startThisThread();
    std::size_t sealed = 0;
    for (int i = 0; i < 1000; ++i)
        for (const uint8_t id : {uint8_t{0}, uint8_t{1}, uint8_t{7}, uint8_t{64}})
            sealed += cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), id) > 0 ? 1u : 0u;
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(sealed, 4000u);
    EXPECT_EQ(allocs, 0) << "sealing for a derived receiver allocated (" << alloc_counter::externalThisThread()
                         << " of the counted allocations were OpenSSL's CRYPTO_malloc, the rest operator new)";
}

// #535: the instrumentation itself - an OpenSSL allocation inside the window is counted (and attributed to the external
// counter), so a gate that reads zero means zero.  RED if the CRYPTO_set_mem_functions wrappers are not installed.
TEST(ClientCryptoRegression, OpenSslAllocationsAreCountedByTheGateCounterRegression) {
    ASSERT_TRUE(openSslAllocCounterInstalled());
    alloc_counter::startThisThread();
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_free(ctx);
    const long counted  = alloc_counter::stopThisThread();
    const long external = alloc_counter::externalThisThread();
    EXPECT_GE(counted, 1) << "an OpenSSL allocation went unseen";
    EXPECT_GE(external, 1) << "and was not attributed to the OpenSSL counter";
    EXPECT_LE(external, counted);
}

// #535 (cold path, client): a receiver key that fails to derive seals NOTHING - sealFrame returns 0, the receiver's seq
// space is untouched (no seq consumed for a frame that was never produced), its slot cipher stays empty (no half-built
// cipher is cached), the gateway's and the other receivers' state are untouched; once the derive works again the very
// next seal succeeds as the receiver's first frame (seq 1) under the properly derived key.
// RED (mutation): selectSendSlot ignores the derive result and builds the cipher from the half-written key.
namespace {
bool deriveFailsForReceiverThree(const cr::AeadKey& c2b, uint8_t id, cr::AeadKey& out) noexcept {
    return id != 3 && cr::deriveReceiverKey(c2b, id, out);
}
}  // namespace

TEST(ClientCryptoRegression, UnderivableReceiverKeySealsNothingAdversarialRegression) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL, rpc116::kBrokerVersionReceiverKeys));
    auto& c = f.m.cryptoState();
    ASSERT_TRUE(c.receiverKeys);
    const auto keys = f.srv.exportKeys(c.cipher, FrameFixture::kSessionId);
    sm::CryptoStateTestAccess::setDeriveFn(c, &deriveFailsForReceiverThree);

    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 1, "underivable-receiver-key");
    std::vector<uint8_t> out(plain.size() + rpc116::kCryptoOverhead);
    EXPECT_EQ(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), 3), 0u)
        << "an underivable receiver key must never produce a frame";
    EXPECT_EQ(c.workerSend[2].sendSeq, 0u) << "no seq consumed by the refused seal";
    EXPECT_FALSE(c.workerSend[2].cipher.has_value()) << "no cipher cached from a failed derive";
    EXPECT_EQ(c.sendSeq, 0u) << "the gateway's seq is untouched";
    // Refused every time it is asked, still without side effects.
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), 3), 0u);
        EXPECT_EQ(c.workerSend[2].sendSeq, 0u);
        EXPECT_FALSE(c.workerSend[2].cipher.has_value());
    }
    // The other receivers are unaffected: they derive and seal normally on their own seq spaces.
    EXPECT_GT(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), 2), 0u);
    EXPECT_EQ(c.workerSend[1].sendSeq, 1u);
    EXPECT_EQ(c.workerSend[2].sendSeq, 0u);

    // The real derive is back and the session reconnected (#673: the held flag is an episode's; modelled by clearing
    // it): receiver 3's next frame is its first, sealed under its properly derived key.
    sm::CryptoStateTestAccess::setDeriveFn(c, &cr::deriveReceiverKey);
    c.workerSend[2].deriveFailed = false;
    const auto fr = sealFor(c, 3, 77);
    ASSERT_FALSE(fr.empty()) << "the seal succeeds once the key derives";
    EXPECT_EQ(subOf(fr).senderId, 3);
    EXPECT_EQ(subOf(fr).seq, 1u);
    cr::AeadKey r3{};
    ASSERT_TRUE(cr::deriveReceiverKey(keys.c2b, 3, r3));
    EXPECT_TRUE(opensUnder(r3, keys.c2bPrefix, keys.cipher, fr));
    EXPECT_TRUE(c.workerSend[2].cipher.has_value());
    EXPECT_EQ(c.workerSend[2].sendSeq, 1u);
}

// Attack (C10 review, #535 x N-I1): a receiver slot derived in one connect episode must never seal a frame of the
// next.  Episode 1 derives receiver 3's cipher and seals with it; the resume is refused and a fresh connect negotiates
// NEW keys; in episode 2 receiver 3's derivation fails.  The seal must return 0 (nothing to transmit) - the episode-1
// cipher is not reused under the new session, no seq is consumed - and once the derive works the frame opens under
// episode 2's receiver key only, as receiver 3's FIRST frame (seq 1), never under episode 1's key.
// RED (mutation): beginCryptoHandshake_ resets only encrypted/send/recv instead of `cryptoState_ = CryptoState{}` -
// episode 1's workerSend[2] cipher survives and seals episode 2's frame under the old key.
TEST(ClientCryptoRegression, AReceiverSlotFromAnEarlierEpisodeNeverSealsAfterAFailedDeriveAdversarialRegression) {
    FakeIo io;
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    sm::SessionConfig cfg;
    cfg.egressChannel = "aeron:udp?endpoint=client:40456";
    cfg.offerCrypto = true; cfg.tlsTrustPem = slurp(testdataPath("ca.pem")); cfg.tlsExpectHost = "localhost";
    cfg.reconnectForeverAfterLive = true;
    sm::SessionMachine m(io, cfg);
    int64_t now = 1'000'000'000LL;
    m.connect(now);
    ASSERT_TRUE(driveHandshake(m, io, srv1, FrameFixture::kSessionId, now + 1, nullptr,
                               rpc116::kBrokerVersionReceiverKeys));
    ASSERT_TRUE(m.cryptoState().receiverKeys);
    const auto keys1 = srv1.exportKeys(m.cryptoState().cipher, FrameFixture::kSessionId);
    const auto ep1 = sealFor(m.cryptoState(), 3, 1);
    ASSERT_FALSE(ep1.empty());
    ASSERT_EQ(m.cryptoState().workerSend[2].sendSeq, 1u);

    (void)freshConnectAfterAResumeRefused(io, m, now);
    EXPECT_FALSE(m.cryptoState().workerSend[2].cipher.has_value()) << "episode 1's receiver cipher survived";
    EXPECT_TRUE(sealFor(m.cryptoState(), 3, 2).empty()) << "nothing seals between episodes";

    io.sends.erase(io.sends.begin(), io.sends.end() - 1);   // the fresh ConnectRequest leads the new handshake
    auto srv2 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(driveHandshake(m, io, srv2, FrameFixture::kSessionId, now += 1'000'000, nullptr,
                               rpc116::kBrokerVersionReceiverKeys));
    auto& c = m.cryptoState();
    ASSERT_TRUE(c.receiverKeys);
    const auto keys2 = srv2.exportKeys(c.cipher, FrameFixture::kSessionId);
    ASSERT_NE(std::memcmp(keys1.c2b.bytes, keys2.c2b.bytes, sizeof keys1.c2b.bytes), 0) << "new episode, new keys";

    sm::CryptoStateTestAccess::setDeriveFn(c, &deriveFailsForReceiverThree);
    for (int i = 0; i < 3; ++i) {
        const auto fr = sealFor(c, 3, 10 + i);
        EXPECT_TRUE(fr.empty()) << "an underivable receiver sealed a frame (episode 1's cipher reused?)";
        if (!fr.empty()) {
            cr::AeadKey old3{};
            ASSERT_TRUE(cr::deriveReceiverKey(keys1.c2b, 3, old3));
            EXPECT_FALSE(opensUnder(old3, keys1.c2bPrefix, keys1.cipher, fr)) << "sealed under episode 1's key";
        }
        EXPECT_EQ(c.workerSend[2].sendSeq, 0u);
        EXPECT_FALSE(c.workerSend[2].cipher.has_value());
    }

    sm::CryptoStateTestAccess::setDeriveFn(c, &cr::deriveReceiverKey);
    c.workerSend[2].deriveFailed = false;   // #673: held for the episode; modelled as the reconnect
    const auto fr = sealFor(c, 3, 20);
    ASSERT_FALSE(fr.empty());
    EXPECT_EQ(subOf(fr).seq, 1u) << "receiver 3's first frame of the new episode";
    cr::AeadKey new3{}, old3{};
    ASSERT_TRUE(cr::deriveReceiverKey(keys2.c2b, 3, new3));
    ASSERT_TRUE(cr::deriveReceiverKey(keys1.c2b, 3, old3));
    EXPECT_TRUE(opensUnder(new3, keys2.c2bPrefix, keys2.cipher, fr));
    EXPECT_FALSE(opensUnder(old3, keys1.c2bPrefix, keys1.cipher, fr));
}

// Handshaking holds no authenticated session (plaintext, forgeable): a corr-0 session-end notice (46 / 51) is ignored.
TEST(ClientCryptoRegression, SessionEndNoticeIsIgnoredWhileHandshakingRegression) {
    for (const int16_t code : {int16_t{46}, int16_t{51}}) {
        FrameFixture f;
        ASSERT_TRUE(f.srv.valid());
        (void)intoHandshaking(f, 1'000'000'000LL);
        ASSERT_EQ(f.m.state(), sm::SessionState::Handshaking);
        f.m.onErrorResponse(code, "end", /*corr=*/0, 1'000'000'020LL, FrameFixture::kSessionId);
        EXPECT_EQ(f.m.state(), sm::SessionState::Handshaking) << code;
    }
}

// #673: a receiver key that fails to derive is counted ONCE per episode and never retried per offer: five seals for
// receiver 3 run the derive exactly once; receiver 2 still seals; a new episode (CryptoState reset) derives again.
namespace {
int g_deriveCalls = 0;   // single test thread
bool countingDeriveFailsForReceiverThree(const cr::AeadKey& c2b, uint8_t id, cr::AeadKey& out) noexcept {
    ++g_deriveCalls;
    return id != 3 && cr::deriveReceiverKey(c2b, id, out);
}
}  // namespace

TEST(ClientCryptoRegression, AFailedReceiverDeriveIsCountedOnceAndNeverRetriedRegression) {
    FakeIo io;
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    auto srv1 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    sm::SessionConfig cfg;
    cfg.egressChannel = "aeron:udp?endpoint=client:40457";
    cfg.offerCrypto = true; cfg.tlsTrustPem = slurp(testdataPath("ca.pem")); cfg.tlsExpectHost = "localhost";
    cfg.reconnectForeverAfterLive = true;
    sm::SessionMachine m(io, cfg);
    int64_t now = 1'000'000'000LL;
    m.connect(now);
    ASSERT_TRUE(driveHandshake(m, io, srv1, FrameFixture::kSessionId, now + 1, nullptr,
                               rpc116::kBrokerVersionReceiverKeys));
    auto& c = m.cryptoState();
    ASSERT_TRUE(c.receiverKeys);
    g_deriveCalls = 0;
    sm::CryptoStateTestAccess::setDeriveFn(c, &countingDeriveFailsForReceiverThree);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 1, "derive-fails-once");
    std::vector<uint8_t> out(plain.size() + rpc116::kCryptoOverhead);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), 3), 0u);
        EXPECT_EQ(cc::selectSendSlot(c, 3).cipher, nullptr);
    }
    EXPECT_EQ(g_deriveCalls, 1) << "the derive ran once, not per offer";
    EXPECT_EQ(c.receiverDeriveFailures, 1u);
    EXPECT_GT(cc::sealFrame(c, plain.data(), plain.size(), out.data(), out.size(), 2), 0u) << "receiver 2 still seals";
    EXPECT_EQ(g_deriveCalls, 2);
    EXPECT_EQ(c.receiverDeriveFailures, 1u);

    // A new episode resets the state (counter, held flags, the seam): the derive runs again.
    (void)freshConnectAfterAResumeRefused(io, m, now);
    EXPECT_EQ(m.cryptoState().receiverDeriveFailures, 0u);
    io.sends.erase(io.sends.begin(), io.sends.end() - 1);
    auto srv2 = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(driveHandshake(m, io, srv2, FrameFixture::kSessionId, now += 1'000'000, nullptr,
                               rpc116::kBrokerVersionReceiverKeys));
    auto& c2 = m.cryptoState();
    ASSERT_TRUE(c2.receiverKeys);
    sm::CryptoStateTestAccess::setDeriveFn(c2, &countingDeriveFailsForReceiverThree);
    const int before = g_deriveCalls;
    EXPECT_EQ(cc::sealFrame(c2, plain.data(), plain.size(), out.data(), out.size(), 3), 0u);
    EXPECT_EQ(g_deriveCalls, before + 1) << "the new episode derives again";
    EXPECT_EQ(c2.receiverDeriveFailures, 1u);
}
