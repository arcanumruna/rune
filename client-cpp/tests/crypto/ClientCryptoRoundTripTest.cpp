// tests/crypto/ClientCryptoRoundTripTest.cpp
//
// client<->broker TLS/AEAD — handshake half (Task 5) + data-plane half (Task 6).
//
// Handshake half: drives a real SessionMachine (client role, fake SessionIo —
// no Aeron) against a real rune::crypto::TlsSession::makeServer loopback,
// using rune-crypto's own test certs.  Asserts the machine reaches Connected +
// encrypted, and that the derived per-direction AeadCiphers round-trip a frame
// against an INDEPENDENTLY exported server-side key (proving key agreement
// without reaching into SessionMachine's private TlsSession).
//
// Data-plane half (Task 6, ClientCryptoFrame* below): exercises
// rune::client::codec::sealFrame / openEgressFrame — the pure, Aeron-free
// envelope seal/open ClientAgent::offerProduceToLeader_ / offerControlTo /
// offerIngress_ / offerOnSession / dispatchEgressFrame call — standalone,
// with a real AeadCipher derived the same way (TLS exporter + makeServer
// loopback).  sealFrame is generic (NOT produce-specific — Task 6b renamed it
// from sealProduceFrame once every non-handshake client->broker send site
// started reusing it), so ClientCryptoFrame.ControlFrameRoundTrip below seals
// a Keepalive (a control template, not Produce) to prove the two sides agree
// beyond the Produce-only case Task 6 originally covered.  The
// ClientAgent/AeronTransport *integration* of these functions is aeron-linked
// and verified on the box.

#include <gtest/gtest.h>

#include <rune/client/codec/Codec.h>
#include <rune/client/codec/CryptoFrame.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/crypto/TlsSession.h>

#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace cc  = rune::client::codec;
namespace sm  = rune::client::session;
namespace cr  = rune::crypto;

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

// ---------------------------------------------------------------------------
// FakeIo — records every sendFrame call (no Aeron).  Mirrors
// tests/session/SessionMachineTest.cpp's FakeIo, trimmed to what this test needs.
// ---------------------------------------------------------------------------
struct FakeIo final : sm::SessionIo {
    std::vector<std::vector<uint8_t>> sends;

    bool sendFrame(const uint8_t* buf, std::size_t len) override {
        sends.emplace_back(buf, buf + len);
        return true;
    }
    bool retarget(const std::string&) noexcept override { return true; }

    [[nodiscard]] std::size_t sendCount() const { return sends.size(); }

    [[nodiscard]] std::optional<uint16_t> lastTemplateId() const {
        if (sends.empty()) return std::nullopt;
        return cc::peekTemplate(sends.back().data(), sends.back().size());
    }
    [[nodiscard]] const std::vector<uint8_t>& last() const { return sends.back(); }
};

// ---------------------------------------------------------------------------
// Drive the handshake to completion (or failure), delivering every frame the
// client has sent to the server in order — INCLUDING the client's trailing
// leg after it already reaches Connected (TLS 1.3: the client's own
// "Finished" can complete the client's done() before the server has
// processed it; the server only reaches its own done() once that last frame
// is delivered, even though the client doesn't need an answer to it).
// Returns true iff BOTH sides end up done().  Bounded iteration count — a
// real bug (infinite HandshakeContinue ping-pong) fails the test instead of
// hanging it.
// ---------------------------------------------------------------------------
bool driveHandshake(sm::SessionMachine& m, FakeIo& io, cr::TlsSession& srv,
                     int64_t sessionId, int64_t nowNs) {
    std::size_t delivered = 0;
    for (int i = 0; i < 20; ++i) {
        if (io.sendCount() <= delivered) break;  // nothing new from the client yet
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

        if (srv.readInbound(reinterpret_cast<const uint8_t*>(peerBytes.data()), peerBytes.size()) ==
            cr::HsState::Failed) {
            return false;
        }

        uint8_t out[16384];
        const std::size_t n = srv.writeOutbound(out, sizeof out);
        const std::string_view outView(reinterpret_cast<const char*>(out), n);

        if (isFirst) {
            m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, /*assignedSessionId=*/sessionId,
                                 corrId, nowNs, chosenCipher, outView);
        } else if (m.state() == sm::SessionState::Handshaking) {
            // Only answer while the client is still waiting — once it's
            // Connected (its own Finished sent) there's nothing more to drive
            // on its side, but the server-delivery loop above still ran.
            m.onHandshakeContinueResponse(/*code=*/0, outView, corrId, nowNs);
        }
    }

    return m.state() == sm::SessionState::Connected && srv.done();
}

}  // namespace

TEST(ClientCryptoRoundTrip, HandshakeReachesConnectedAndEncrypted) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));
    ASSERT_FALSE(serverPem.empty());
    ASSERT_FALSE(serverKey.empty());
    ASSERT_FALSE(caPem.empty());

    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());

    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId = 1001;
    cfg.offerCrypto    = true;
    cfg.tlsTrustPem    = caPem;
    cfg.tlsExpectHost  = "localhost";
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0         = 1'000'000'000LL;
    constexpr int64_t kSessionId = 42;

    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    ASSERT_TRUE(driveHandshake(m, io, srv, kSessionId, T0 + 1))
        << "handshake did not reach Connected (state=" << static_cast<int>(m.state())
        << " rejectCode=" << m.rejectCode() << ")";

    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_EQ(m.sessionId(), kSessionId);
    EXPECT_TRUE(m.cryptoState().encrypted);
    EXPECT_NE(m.cryptoState().cipher, rune::crypto::Cipher::None);
    ASSERT_TRUE(m.cryptoState().send.has_value());
    ASSERT_TRUE(m.cryptoState().recv.has_value());

    // Key-agreement proof: export the SAME keys independently from the server's
    // TlsSession (same sessionId -> same exporter context) and round-trip a
    // frame in each direction between the client's CryptoState and a
    // server-side AeadCipher built from the independently exported keys.
    const auto srvKeys = srv.exportKeys(m.cryptoState().cipher, kSessionId);
    ASSERT_NE(srvKeys.cipher, rune::crypto::Cipher::None);

    rune::crypto::AeadCipher srvSend(srvKeys.cipher, srvKeys.b2c, srvKeys.b2cPrefix);  // broker -> client
    rune::crypto::AeadCipher srvRecv(srvKeys.cipher, srvKeys.c2b, srvKeys.c2bPrefix);  // client -> broker

    const uint8_t aad[4] = {1, 2, 3, 4};

    // client -> broker
    {
        const uint8_t plain[] = "hello broker";
        uint8_t ct[sizeof(plain) + rune::crypto::kTagSize];
        const std::size_t n = m.cryptoState().send->seal(0, aad, sizeof aad, plain, sizeof plain, ct);
        ASSERT_GT(n, 0u);
        uint8_t recovered[sizeof(plain)];
        const long m2 = srvRecv.open(0, aad, sizeof aad, ct, n, recovered);
        ASSERT_EQ(m2, static_cast<long>(sizeof plain));
        EXPECT_EQ(0, std::memcmp(plain, recovered, sizeof plain));
    }

    // broker -> client
    {
        const uint8_t plain[] = "hello client";
        uint8_t ct[sizeof(plain) + rune::crypto::kTagSize];
        const std::size_t n = srvSend.seal(0, aad, sizeof aad, plain, sizeof plain, ct);
        ASSERT_GT(n, 0u);
        uint8_t recovered[sizeof(plain)];
        const long m2 = m.cryptoState().recv->open(0, aad, sizeof aad, ct, n, recovered);
        ASSERT_EQ(m2, static_cast<long>(sizeof plain));
        EXPECT_EQ(0, std::memcmp(plain, recovered, sizeof plain));
    }
}

TEST(ClientCryptoRoundTrip, WrongHostnameFailsClosedNonRetriable) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    const std::string caPem     = slurp(testdataPath("ca.pem"));

    cr::TlsSession srv = cr::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str());
    ASSERT_TRUE(srv.valid());

    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.offerCrypto    = true;
    cfg.tlsTrustPem    = caPem;
    cfg.tlsExpectHost  = "not-the-cert-san.example";  // deliberate mismatch
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0 = 1'000'000'000LL;
    m.connect(T0);

    EXPECT_FALSE(driveHandshake(m, io, srv, /*sessionId=*/7, T0 + 1));
    EXPECT_EQ(m.state(), sm::SessionState::Rejected);
    EXPECT_EQ(m.rejectCode(),
              static_cast<int16_t>(static_cast<uint16_t>(
                  rune::protocol::client::ErrorCode::HandshakeFailed)));
}

TEST(ClientCryptoRoundTrip, CryptoOffConnectIsByteIdenticalToPreTask5) {
    // offerCrypto defaults to false — the toggle (design spec §11) must leave the
    // plaintext connect path untouched, no TlsSession ever constructed.
    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId = 1001;
    cfg.clientVersion  = 1;  // pre-crypto clientVersion, as many existing tests pin
    sm::SessionMachine m(io, cfg);

    m.connect(1'000'000'000LL);

    ASSERT_EQ(io.sendCount(), 1u);
    auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->clientVersion, 1);
    EXPECT_TRUE(req->cipherList.empty());
    EXPECT_TRUE(req->handshakeBytes.empty());
    EXPECT_FALSE(m.cryptoState().encrypted);
}

TEST(ClientCryptoRoundTrip, OfferCryptoProducesCryptoConnectRequest) {
    // Task 9: offerCrypto=true -> a v(crypto) ConnectRequest carrying a
    // non-empty cipherList + handshakeBytes (the client's ClientHello),
    // mirroring CryptoOffConnectIsByteIdenticalToPreTask5's negative case.
    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId = 1001;
    cfg.offerCrypto    = true;
    cfg.tlsTrustPem    = "";  // trust nothing — this test never completes the handshake
    sm::SessionMachine m(io, cfg);

    m.connect(1'000'000'000LL);

    ASSERT_EQ(io.sendCount(), 1u);
    auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
    ASSERT_TRUE(req.has_value());
    EXPECT_FALSE(req->cipherList.empty());
    EXPECT_FALSE(req->handshakeBytes.empty());
}

TEST(ClientCryptoRoundTrip, RequireCryptoFailsTerminallyOnNoCipherResponse) {
    // Task 9 (downgrade safety): requireCrypto=true + a ConnectResponse{OK}
    // that negotiates NO cipher (chosenCipher=0 — a plaintext broker, or a
    // MITM that stripped the crypto offer off the wire) must NOT silently
    // proceed to a plaintext Connected session. It must fail exactly like a
    // real TLS handshake failure: terminal Rejected(HandshakeFailed).
    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId = 1001;
    cfg.offerCrypto    = true;
    cfg.requireCrypto  = true;
    cfg.tlsTrustPem    = "";
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0 = 1'000'000'000LL;
    m.connect(T0);
    ASSERT_EQ(m.state(), sm::SessionState::Connecting);

    auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
    ASSERT_TRUE(req.has_value());
    ASSERT_FALSE(req->cipherList.empty()) << "requireCrypto implies offerCrypto's crypto offer";

    // The broker answers OK but negotiates no cipher (chosenCipher=0,
    // handshakeBytes empty) — exactly what onConnectResponse's "happy path —
    // no crypto chosen" branch used to accept unconditionally.
    m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, /*assignedSessionId=*/99,
                         req->correlationId, T0 + 1, /*chosenCipher=*/0, /*handshakeBytes=*/{});

    EXPECT_EQ(m.state(), sm::SessionState::Rejected)
        << "requireCrypto must fail terminally, never silently proceed plaintext";
    EXPECT_EQ(m.rejectCode(),
              static_cast<int16_t>(static_cast<uint16_t>(
                  rune::protocol::client::ErrorCode::HandshakeFailed)));
    EXPECT_FALSE(m.cryptoState().encrypted);
}

TEST(ClientCryptoRoundTrip, RequireCryptoOffNoCipherResponseStillPlaintextConnects) {
    // Control case: requireCrypto=false (default) with the SAME no-cipher
    // ConnectResponse — the pre-Task-9 degrade-to-plaintext behaviour must be
    // unchanged (back-compat with a pre-v4 / RUNE_TLS=OFF broker).
    FakeIo io;
    sm::SessionConfig cfg;
    cfg.egressChannel  = "aeron:udp?endpoint=client:40456";
    cfg.egressStreamId = 1001;
    cfg.offerCrypto    = true;
    cfg.requireCrypto  = false;
    cfg.tlsTrustPem    = "";
    sm::SessionMachine m(io, cfg);

    constexpr int64_t T0 = 1'000'000'000LL;
    m.connect(T0);
    auto req = cc::decodeConnectRequest(io.last().data(), io.last().size());
    ASSERT_TRUE(req.has_value());

    m.onConnectResponse(/*code=*/0, /*leaderMemberId=*/-1, /*assignedSessionId=*/99,
                         req->correlationId, T0 + 1, /*chosenCipher=*/0, /*handshakeBytes=*/{});

    EXPECT_EQ(m.state(), sm::SessionState::Connected);
    EXPECT_FALSE(m.cryptoState().encrypted);
}

// ---------------------------------------------------------------------------
// client<->broker TLS/AEAD (Task 6 / 6b) — data-plane half: sealFrame /
// openEgressFrame.  Standalone (no Aeron): a real handshake derives the
// client-side CryptoState (cc::sealFrame's `send`); an independently
// exported "broker-side" CryptoState (cc::openEgressFrame's `recv`, bound to
// the SAME c2b key/prefix) proves the wire format + AEAD binding end to end,
// the same technique ClientCryptoRoundTrip.HandshakeReachesConnectedAndEncrypted
// uses above.  sealFrame is generic (Task 6b: NOT produce-specific), so
// buildPlainFrame below builds ANY templateId's [24B header][body] —
// ClientCryptoFrame.RoundTripSealThenOpenRecoversPlaintext (and friends)
// exercise Produce; ClientCryptoFrame.ControlFrameRoundTripSealThenOpen below
// exercises Keepalive, proving the two sides agree beyond the Produce-only
// case Task 6 originally covered (the bug Task 6b fixes: ClientAgent only
// sealed Produce, so an encrypted session's Keepalive/Subscribe/... frames
// went out in the clear and the broker's open() dropped them).
// ---------------------------------------------------------------------------
namespace {

/// Build [24B MessageHeader][body] for `templateId` — the plaintext every
/// sealFrame call site (offerProduceToLeader_ for Produce; offerControlTo /
/// offerIngress_ / offerOnSession for every other non-handshake template)
/// hands to sealFrame, sessionId already patched in (Task 1.3's in-place
/// patch for Produce; already-final for session-machine-originated control
/// frames — see ClientAgent::sealForDestination_'s doc comment).
std::vector<uint8_t> buildPlainFrame(rune::protocol::client::TemplateId templateId,
                                      int64_t sessionId, int64_t correlationId,
                                      std::string_view body) {
    namespace rpc = rune::protocol::client;
    std::vector<uint8_t> buf(rpc::MessageHeader::ENCODED_SIZE + body.size());
    rpc::MessageHeader h;
    h.blockLength   = static_cast<uint16_t>(body.size());
    h.templateId    = static_cast<uint16_t>(templateId);
    h.sessionId     = sessionId;
    h.correlationId = correlationId;
    h.encode(buf.data());
    std::memcpy(buf.data() + rpc::MessageHeader::ENCODED_SIZE, body.data(), body.size());
    return buf;
}

/// Produce-specific convenience wrapper (keeps the existing Produce tests
/// below unchanged in spirit — same frame shape as before Task 6b).
std::vector<uint8_t> buildPlainProduceFrame(int64_t sessionId, int64_t correlationId,
                                             std::string_view body) {
    return buildPlainFrame(rune::protocol::client::TemplateId::Produce, sessionId, correlationId, body);
}

/// Independently-exported "broker-side" CryptoState: `recv` is bound to the
/// SAME c2b key/prefix the client's `send` (m.cryptoState().send) seals with —
/// i.e. what the broker would use to open a client->broker frame.  `send`
/// (b2c) is populated too so the fixture mirrors a real CryptoState shape,
/// though these tests only exercise `recv`.
sm::CryptoState brokerSideCryptoStateFor(cr::TlsSession& srv, const sm::SessionMachine& m,
                                          int64_t sessionId) {
    const auto keys = srv.exportKeys(m.cryptoState().cipher, sessionId);
    sm::CryptoState c;
    c.encrypted = true;
    c.cipher    = keys.cipher;
    c.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);  // broker -> client
    c.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);  // client -> broker
    return c;
}

/// Shared setup: drive a real TLS handshake to Connected+encrypted and return
/// the matching broker-side CryptoState.  `m`/`srv`/`io` are caller-owned (TLS
/// session + session machine are non-movable) so they outlive the returned
/// broker-side state for the duration of the test.
struct FrameFixture {
    std::string    serverPem, serverKey, caPem;
    cr::TlsSession  srv;
    FakeIo          io;
    sm::SessionMachine m;
    static constexpr int64_t kSessionId = 42;

    FrameFixture()
        : serverPem(slurp(testdataPath("server.pem")))
        , serverKey(slurp(testdataPath("server.key")))
        , caPem(slurp(testdataPath("ca.pem")))
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

    bool established(int64_t nowNs) {
        m.connect(nowNs);
        return driveHandshake(m, io, srv, kSessionId, nowNs + 1) && m.cryptoState().encrypted;
    }
};

}  // namespace

TEST(ClientCryptoFrame, RoundTripSealThenOpenRecoversPlaintext) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, /*correlationId=*/777,
                                               "hello-produce-body");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(),
                                                        sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);
    EXPECT_EQ(sealedLen, plain.size() + rune::protocol::client::kCryptoOverhead);

    uint8_t opened[256];
    const std::size_t openedLen =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    ASSERT_EQ(openedLen, plain.size());
    EXPECT_EQ(0, std::memcmp(plain.data(), opened, plain.size()));
}

TEST(ClientCryptoFrame, TamperedCiphertextIsDroppedWindowNotPoisoned) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 778, "tamper-me");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(),
                                                        sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);
    sealed[sealedLen - 1] ^= 0x01;  // flip a tag byte -> AEAD auth must fail

    const uint64_t recvSeqBefore    = brokerSide.recvSeq;
    const uint64_t recvWindowBefore = brokerSide.recvWindow;

    uint8_t opened[256];
    const std::size_t openedLen =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    EXPECT_EQ(openedLen, 0u);
    // A forged/tampered frame must never poison the anti-replay window.
    EXPECT_EQ(brokerSide.recvSeq, recvSeqBefore);
    EXPECT_EQ(brokerSide.recvWindow, recvWindowBefore);
}

TEST(ClientCryptoFrame, TamperedHeaderBreaksAadBindingAndIsDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 779, "aad-bound-body");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(),
                                                        sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);
    // Flip a byte inside the CLEAR MessageHeader (correlationId, offset 16) —
    // the ciphertext/tag are untouched, but the header is part of the AAD, so
    // this must still fail authentication.
    sealed[16] ^= 0x01;

    uint8_t opened[256];
    const std::size_t openedLen =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    EXPECT_EQ(openedLen, 0u);
}

TEST(ClientCryptoFrame, ReplayedSeqIsDropped) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);
    const auto plain = buildPlainProduceFrame(FrameFixture::kSessionId, 780, "replay-me");

    uint8_t sealed[256];
    const std::size_t sealedLen = cc::sealFrame(f.m.cryptoState(), plain.data(), plain.size(),
                                                        sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u);

    uint8_t opened[256];
    const std::size_t first =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    ASSERT_EQ(first, plain.size());

    // Deliver the EXACT SAME sealed frame again — a replay.
    const std::size_t second =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    EXPECT_EQ(second, 0u);
}

TEST(ClientCryptoFrame, InWindowReorderIsAcceptedOutOfOrderDeliveryWorks) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);

    uint8_t sealed1[256], sealed2[256], sealed3[256];
    const auto plain1 = buildPlainProduceFrame(FrameFixture::kSessionId, 781, "seq-1");
    const auto plain2 = buildPlainProduceFrame(FrameFixture::kSessionId, 782, "seq-2");
    const auto plain3 = buildPlainProduceFrame(FrameFixture::kSessionId, 783, "seq-3");
    const std::size_t len1 = cc::sealFrame(f.m.cryptoState(), plain1.data(), plain1.size(), sealed1, sizeof sealed1);
    const std::size_t len2 = cc::sealFrame(f.m.cryptoState(), plain2.data(), plain2.size(), sealed2, sizeof sealed2);
    const std::size_t len3 = cc::sealFrame(f.m.cryptoState(), plain3.data(), plain3.size(), sealed3, sizeof sealed3);
    ASSERT_GT(len1, 0u); ASSERT_GT(len2, 0u); ASSERT_GT(len3, 0u);

    uint8_t opened[256];
    // Deliver seq=1, then seq=3 (skipping 2), then seq=2 — a legitimate
    // network reorder.  seq=2 is still inside the 64-wide window and unseen.
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed1, len1, opened, sizeof opened), plain1.size());
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed3, len3, opened, sizeof opened), plain3.size());
    const std::size_t reordered = cc::openEgressFrame(brokerSide, sealed2, len2, opened, sizeof opened);
    ASSERT_EQ(reordered, plain2.size());
    EXPECT_EQ(0, std::memcmp(plain2.data(), opened, plain2.size()));

    // A SECOND delivery of the reordered seq=2 is now a replay.
    EXPECT_EQ(cc::openEgressFrame(brokerSide, sealed2, len2, opened, sizeof opened), 0u);
}

// ---------------------------------------------------------------------------
// Task 6b: a NON-Produce (control) template seals on the client and opens on
// the broker side, proving sealFrame (renamed from sealProduceFrame) is
// genuinely generic — this is the bug Task 6b fixes: ClientAgent used to seal
// ONLY Produce, so on an encrypted session a Keepalive went out in the clear
// and the broker's open() dropped it (session eventually times out).
// ---------------------------------------------------------------------------
TEST(ClientCryptoFrame, ControlFrameRoundTripSealThenOpen) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);

    // A REAL Keepalive wire frame (rpc::Keepalive, TemplateId::Keepalive==3) —
    // exactly what ClientAgent::offerControlTo seals today via
    // sealForDestination_ (SessionMachine's tickConnected sends an encodePing
    // Ping=6 frame in practice; Keepalive is the sibling control template this
    // test exercises, both equally NOT Produce and equally in scope).
    uint8_t plain[rune::protocol::client::Keepalive::ENCODED_SIZE];
    const std::size_t plainLen =
        cc::encodeKeepalive(plain, sizeof plain, FrameFixture::kSessionId, /*correlationId=*/-1);
    ASSERT_GT(plainLen, 0u);

    uint8_t sealed[256];
    const std::size_t sealedLen =
        cc::sealFrame(f.m.cryptoState(), plain, plainLen, sealed, sizeof sealed);
    ASSERT_GT(sealedLen, 0u) << "sealFrame must seal ANY non-handshake template, not just Produce";
    EXPECT_EQ(sealedLen, plainLen + rune::protocol::client::kCryptoOverhead);

    uint8_t opened[256];
    const std::size_t openedLen =
        cc::openEgressFrame(brokerSide, sealed, sealedLen, opened, sizeof opened);
    ASSERT_EQ(openedLen, plainLen)
        << "the broker's open() must recover a sealed Keepalive exactly like a sealed Produce";
    EXPECT_EQ(0, std::memcmp(plain, opened, plainLen));

    // And the recovered plaintext decodes back to the same Keepalive fields.
    auto decoded = cc::decodeKeepalive(opened, openedLen);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId, FrameFixture::kSessionId);
    EXPECT_EQ(decoded->correlationId, -1);
}

// RED-evidence companion (kept GREEN; see task-6b-report.md for the transcript
// of actually flipping this to prove it fails): sending the Keepalive frame
// UNSEALED (i.e. in the clear) must fail to open on an encrypted broker-side
// CryptoState — the broker's open() must never silently accept plaintext on
// an encrypted session.  This is exactly what used to happen before Task 6b
// (ClientAgent sealed only Produce): a plaintext control frame reached the
// broker, which looked for a sealed envelope and failed/dropped it.
TEST(ClientCryptoFrame, UnsealedControlFrameFailsToOpenOnEncryptedSession) {
    FrameFixture f;
    ASSERT_TRUE(f.srv.valid());
    ASSERT_TRUE(f.established(1'000'000'000LL));

    sm::CryptoState brokerSide = brokerSideCryptoStateFor(f.srv, f.m, FrameFixture::kSessionId);

    uint8_t plain[rune::protocol::client::Keepalive::ENCODED_SIZE];
    const std::size_t plainLen =
        cc::encodeKeepalive(plain, sizeof plain, FrameFixture::kSessionId, /*correlationId=*/-1);
    ASSERT_GT(plainLen, 0u);

    // The broker only ever sees sealed wire frames on an encrypted session —
    // openEgressFrame on the plain 24-byte frame (no CryptoSubHeader, no tag)
    // must fail (too short to even decode a subheader), not silently succeed.
    uint8_t opened[256];
    const std::size_t openedLen =
        cc::openEgressFrame(brokerSide, plain, plainLen, opened, sizeof opened);
    EXPECT_EQ(openedLen, 0u) << "an unsealed control frame must never be accepted on an encrypted session";
}
