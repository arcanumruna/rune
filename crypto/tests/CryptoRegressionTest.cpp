// tests/CryptoRegressionTest.cpp
//
// Task 15 (client<->broker TLS/AEAD): SECURITY REGRESSION SUITE, named
// `*Regression*` so it is selected as the run-first priority gate (CLAUDE.md
// rule 6/7) ahead of the rest of the test matrix.
//
// This is the rune-crypto layer of the three-repo regression gate (the other
// two are libs/gateway/tests/GatewayCryptoRegressionTest.cpp in rune-broker
// and tests/crypto/ClientCryptoRegressionTest.cpp in rune-client-cpp).  It
// consolidates the lowest-level AEAD primitive + TLS 1.3 handshake security
// properties that AeadTest.cpp / TlsSessionTest.cpp already assert piecemeal,
// plus TWO gaps those files did not cover explicitly:
//   - a dedicated AEAD *tag*-byte-flip case (as opposed to a ciphertext-byte
//     flip — both must independently fail auth; AeadTest only flips a
//     ciphertext byte);
//   - exportKeys() failing CLOSED (Cipher::None sentinel) BEFORE done(), at
//     the rune-crypto layer itself (GatewayCryptoTest in rune-broker already
//     proves the broker-side guard built on top of this, but the primitive's
//     own contract deserves its own regression case here).
// Nothing here duplicates AeadTest.cpp/TlsSessionTest.cpp's role as the
// primary unit tests — those files are untouched; this is the consolidated,
// named run-first gate.
//
// What this proves:
//   AeadRegression.*
//     - CiphertextByteFlipDropped / TagByteFlipDropped — AEAD tamper: a
//       flipped ciphertext byte AND a flipped tag byte each independently
//       fail auth (seal() output is ciphertext || 16B tag, contiguous).
//     - AadTamperDropped — flipping a single AAD byte (the clear header this
//       primitive is always bound to in production) fails auth even though
//       ciphertext+tag are untouched.
//     - WrongNonceSeqDropped — opening under a different `seq` (part of the
//       96-bit nonce) fails auth.
//     - CrossKeyInjectionFailsOpen — a frame sealed under one key never
//       opens under an unrelated key (the AEAD-primitive-level form of
//       "cross-session/wrong-key injection": two independent sessions never
//       derive the same key, so this is the floor every higher-level
//       cross-session test rests on).
//     - DeriveSenderKeyZeroIdentityAndDistinctPerSender — per-sender key
//       derivation's two core guarantees (senderId==0 byte-identical to b2c;
//       distinct senderIds never collide) pinned here too, since the whole
//       per-sender/shard-egress regression story in the broker+client suites
//       depends on this primitive being correct.
//   TlsRegression.*
//     - UntrustedCertRejected / WrongExpectHostRejected — handshake server-
//       auth: a client that doesn't trust the broker's CA, or that checks the
//       wrong hostname, must never complete the handshake (MITM defense).
//     - ExportKeysFailsClosedBeforeDone — the fail-closed sentinel contract:
//       exportKeys() on a TlsSession that never reached done() MUST return
//       Cipher::None, never a derivable (and therefore all-zero-equivalent,
//       catastrophic) key.

#include <gtest/gtest.h>

#include <cstring>
#include <fstream>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

#include "rune/crypto/Aead.h"
#include "rune/crypto/TlsSession.h"

using namespace rune::crypto;

namespace {

AeadKey keyFilledWith(uint8_t base) {
    AeadKey k{};
    for (int i = 0; i < 32; ++i) k.bytes[i] = static_cast<uint8_t>(base + i);
    return k;
}

void forEachCipher(const std::function<void(Cipher)>& f) {
    f(Cipher::Aes256Gcm);
    f(Cipher::ChaCha20Poly1305);
}

std::string slurp(const char* path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Pump two TlsSessions until both are done() or one fails. Mirrors
// TlsSessionTest.cpp's pump() exactly (kept local/self-contained per this
// file's own regression-suite remit).
bool pump(TlsSession& cli, TlsSession& srv) {
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

}  // namespace

// ===========================================================================
// AeadRegression — AEAD tamper / AAD tamper / nonce / cross-key injection.
// ===========================================================================

TEST(AeadRegression, CiphertextByteFlipDropped) {
    forEachCipher([](Cipher c) {
        uint8_t pfx[4] = {1, 2, 3, 4};
        AeadCipher sealer(c, keyFilledWith(1), pfx);
        AeadCipher opener(c, keyFilledWith(1), pfx);
        const uint8_t aad[4] = {9, 9, 9, 9};
        const uint8_t plain[16] = {0};
        std::vector<uint8_t> ct(16 + kTagSize), pt(16);

        const size_t n = sealer.seal(1, aad, sizeof aad, plain, sizeof plain, ct.data());
        ASSERT_GT(n, 0u);
        ct[0] ^= 0x01;  // flip the FIRST ciphertext byte (never the tag)

        EXPECT_EQ(-1, opener.open(1, aad, sizeof aad, ct.data(), n, pt.data()))
            << "a flipped ciphertext byte must fail AEAD auth";
    });
}

TEST(AeadRegression, TagByteFlipDropped) {
    forEachCipher([](Cipher c) {
        uint8_t pfx[4] = {1, 2, 3, 4};
        AeadCipher sealer(c, keyFilledWith(2), pfx);
        AeadCipher opener(c, keyFilledWith(2), pfx);
        const uint8_t aad[4] = {7, 7, 7, 7};
        const uint8_t plain[16] = {0};
        std::vector<uint8_t> ct(16 + kTagSize), pt(16);

        const size_t n = sealer.seal(1, aad, sizeof aad, plain, sizeof plain, ct.data());
        ASSERT_GT(n, 0u);
        // seal()'s output is ciphertext || 16B tag, contiguous — the LAST
        // byte of `n` is always inside the tag.
        ct[n - 1] ^= 0x01;

        EXPECT_EQ(-1, opener.open(1, aad, sizeof aad, ct.data(), n, pt.data()))
            << "a flipped tag byte must fail AEAD auth, independently of a ciphertext flip";
    });
}

TEST(AeadRegression, AadTamperDropped) {
    forEachCipher([](Cipher c) {
        uint8_t pfx[4] = {1, 2, 3, 4};
        AeadCipher sealer(c, keyFilledWith(3), pfx);
        AeadCipher opener(c, keyFilledWith(3), pfx);
        uint8_t aad[4] = {1, 1, 1, 1};
        const uint8_t plain[16] = {0};
        std::vector<uint8_t> ct(16 + kTagSize), pt(16);

        const size_t n = sealer.seal(5, aad, sizeof aad, plain, sizeof plain, ct.data());
        ASSERT_GT(n, 0u);

        uint8_t tamperedAad[4] = {1, 1, 1, 2};  // one AAD byte flipped
        EXPECT_EQ(-1, opener.open(5, tamperedAad, sizeof tamperedAad, ct.data(), n, pt.data()))
            << "a tampered AAD byte must fail auth even though ciphertext/tag are untouched";
    });
}

TEST(AeadRegression, WrongNonceSeqDropped) {
    forEachCipher([](Cipher c) {
        uint8_t pfx[4] = {1, 2, 3, 4};
        AeadCipher sealer(c, keyFilledWith(4), pfx);
        AeadCipher opener(c, keyFilledWith(4), pfx);
        const uint8_t aad[4] = {2, 2, 2, 2};
        const uint8_t plain[16] = {0};
        std::vector<uint8_t> ct(16 + kTagSize), pt(16);

        const size_t n = sealer.seal(10, aad, sizeof aad, plain, sizeof plain, ct.data());
        ASSERT_GT(n, 0u);
        EXPECT_EQ(-1, opener.open(11, aad, sizeof aad, ct.data(), n, pt.data()))
            << "opening under the wrong seq (nonce component) must fail";
    });
}

TEST(AeadRegression, CrossKeyInjectionFailsOpen) {
    // The AEAD-primitive floor of "cross-session/wrong-key injection": a
    // frame sealed under session A's key must never open under session B's
    // unrelated key. Every higher-level cross-session regression case (the
    // gateway/client suites) rests on this.
    forEachCipher([](Cipher c) {
        uint8_t pfx[4] = {5, 6, 7, 8};
        AeadCipher sealerA(c, keyFilledWith(10), pfx);
        AeadCipher openerB(c, keyFilledWith(99), pfx);  // a DIFFERENT session's key
        const uint8_t aad[4] = {0, 0, 0, 0};
        const uint8_t plain[16] = {0};
        std::vector<uint8_t> ct(16 + kTagSize), pt(16);

        const size_t n = sealerA.seal(1, aad, sizeof aad, plain, sizeof plain, ct.data());
        ASSERT_GT(n, 0u);
        EXPECT_EQ(-1, openerB.open(1, aad, sizeof aad, ct.data(), n, pt.data()))
            << "a frame sealed under one key must never open under a different session's key";
    });
}

TEST(AeadRegression, DeriveSenderKeyZeroIdentityAndDistinctPerSender) {
    const AeadKey b2c = keyFilledWith(1);
    AeadKey sender0{}, sender1{}, sender2{};
    ASSERT_TRUE(deriveSenderKey(b2c, 0, sender0));
    ASSERT_TRUE(deriveSenderKey(b2c, 1, sender1));
    ASSERT_TRUE(deriveSenderKey(b2c, 2, sender2));

    EXPECT_EQ(0, std::memcmp(sender0.bytes, b2c.bytes, sizeof b2c.bytes))
        << "senderId==0 must be byte-identical to the gateway's own b2c (back-compat)";
    EXPECT_NE(0, std::memcmp(sender1.bytes, sender2.bytes, sizeof sender1.bytes))
        << "distinct senderIds must derive distinct keys -- independent nonce spaces";
    EXPECT_NE(0, std::memcmp(sender1.bytes, b2c.bytes, sizeof b2c.bytes));
}

// ===========================================================================
// TlsRegression — handshake server-auth + fail-closed key export.
// ===========================================================================

TEST(TlsRegression, UntrustedCertRejected) {
    auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(),
                                       slurp("testdata/server.key").c_str());
    ASSERT_TRUE(srv.valid());
    auto cli = TlsSession::makeClient("", "localhost");  // "" = trust nothing
    ASSERT_TRUE(cli.valid());
    cli.readInbound(nullptr, 0);
    EXPECT_FALSE(pump(cli, srv)) << "an untrusted server cert must fail the handshake";
}

TEST(TlsRegression, WrongExpectHostRejected) {
    auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(),
                                       slurp("testdata/server.key").c_str());
    ASSERT_TRUE(srv.valid());
    const std::string caPem = slurp("testdata/ca.pem");
    auto cli = TlsSession::makeClient(caPem.c_str(), "not-the-cert-san.example");
    ASSERT_TRUE(cli.valid());
    cli.readInbound(nullptr, 0);
    EXPECT_FALSE(pump(cli, srv)) << "a hostname mismatch must fail the handshake (MITM defense)";
}

TEST(TlsRegression, ExportKeysFailsClosedBeforeDone) {
    auto srv = TlsSession::makeServer(slurp("testdata/server.pem").c_str(),
                                       slurp("testdata/server.key").c_str());
    ASSERT_TRUE(srv.valid());
    ASSERT_FALSE(srv.done());  // handshake never driven

    const SessionKeys keys = srv.exportKeys(Cipher::Aes256Gcm, /*sessionId=*/1);
    EXPECT_EQ(keys.cipher, Cipher::None)
        << "exportKeys() before done() must return the Cipher::None fail-closed sentinel, "
           "never a derivable (and therefore usable) key";
}
