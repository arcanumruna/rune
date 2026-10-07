// tests/crypto/ClientCryptoRoundTripTest.cpp
//
// client<->broker TLS/AEAD (Task 5) — handshake half.
//
// Drives a real SessionMachine (client role, fake SessionIo — no Aeron) against a
// real rune::crypto::TlsSession::makeServer loopback, using rune-crypto's own test
// certs.  Asserts the machine reaches Connected + encrypted, and that the derived
// per-direction AeadCiphers round-trip a frame against an INDEPENDENTLY exported
// server-side key (proving key agreement without reaching into SessionMachine's
// private TlsSession).
//
// The "data half" (sealing/opening real Produce/egress frames through
// ClientAgent) is Task 6 — out of scope here.

#include <gtest/gtest.h>

#include <rune/client/codec/Codec.h>
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
