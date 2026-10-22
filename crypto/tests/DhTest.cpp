// rune-crypto Dh (issue #242): X25519 pinned to RFC 7748 and fail-closed on a low-order peer point.
#include "rune/crypto/Dh.h"
#include <gtest/gtest.h>
#include <string>

namespace rc = rune::crypto;
namespace {
std::array<uint8_t, 32> hex32(std::string_view h) {
    std::array<uint8_t, 32> v{};
    for (std::size_t i = 0; i < 32; ++i) v[i] = static_cast<uint8_t>(std::stoi(std::string(h.substr(2 * i, 2)), nullptr, 16));
    return v;
}
const std::array<uint8_t, 32> kZero{};
} // namespace

TEST(DhRegression, X25519MatchesRfc7748Section61Regression) {
    const auto alicePriv = hex32("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
    const auto alicePub  = hex32("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
    const auto bobPriv   = hex32("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
    const auto bobPub    = hex32("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
    const auto want      = hex32("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
    std::array<uint8_t, 32> k1{}, k2{};
    ASSERT_TRUE(rc::x25519Shared(alicePriv, bobPub, k1));
    ASSERT_TRUE(rc::x25519Shared(bobPriv, alicePub, k2));
    EXPECT_EQ(k1, want);
    EXPECT_EQ(k2, want);
}

// Issue #18: the public key of a given private key, pinned to the RFC 7748 §6.1 vector (Alice and Bob).
TEST(DhRegression, X25519PublicMatchesRfc7748Section61Regression) {
    std::array<uint8_t, 32> pub{};
    ASSERT_TRUE(rc::x25519Public(hex32("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a"), pub));
    EXPECT_EQ(pub, hex32("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"));
    ASSERT_TRUE(rc::x25519Public(hex32("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"), pub));
    EXPECT_EQ(pub, hex32("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"));
    std::array<uint8_t, 32> priv{}, kpPub{};
    ASSERT_TRUE(rc::x25519Keypair(priv, kpPub));
    ASSERT_TRUE(rc::x25519Public(priv, pub));
    EXPECT_EQ(pub, kpPub) << "the same public key x25519Keypair returned";
}

TEST(DhRegression, TwoRandomKeypairsAgreeRegression) {
    std::array<uint8_t, 32> aPriv{}, aPub{}, bPriv{}, bPub{}, k1{}, k2{};
    ASSERT_TRUE(rc::x25519Keypair(aPriv, aPub));
    ASSERT_TRUE(rc::x25519Keypair(bPriv, bPub));
    EXPECT_NE(aPub, bPub);
    EXPECT_NE(aPriv, aPub);
    ASSERT_TRUE(rc::x25519Shared(aPriv, bPub, k1));
    ASSERT_TRUE(rc::x25519Shared(bPriv, aPub, k2));
    EXPECT_EQ(k1, k2);
    EXPECT_NE(k1, kZero);
    std::array<uint8_t, 32> cPriv{}, cPub{};
    ASSERT_TRUE(rc::x25519Keypair(cPriv, cPub));
    EXPECT_NE(cPriv, aPriv) << "every keypair is fresh";
}

TEST(DhRegression, ALowOrderPeerPointFailsClosedRegression) {
    std::array<uint8_t, 32> priv{}, pub{}, shared{};
    ASSERT_TRUE(rc::x25519Keypair(priv, pub));
    shared.fill(0xAA);
    EXPECT_FALSE(rc::x25519Shared(priv, kZero, shared)) << "the all-zero point";
    EXPECT_EQ(shared, kZero) << "no partial output survives a failure";
    std::array<uint8_t, 32> one{}; one[0] = 1;                                          // order 1 (u = 1)
    EXPECT_FALSE(rc::x25519Shared(priv, one, shared));
}
