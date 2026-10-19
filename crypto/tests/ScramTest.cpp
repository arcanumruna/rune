// SCRAM (RFC 5802): the RFC 7677 SCRAM-SHA-256 vector pins the construction; SHA-512
// (what Rune ships, spec §5) is exercised end to end, both active verifiers included (D5).
#include "rune/crypto/Kdf.h"
#include "rune/crypto/Scram.h"
#include <gtest/gtest.h>

namespace rc = rune::crypto;
namespace rs = rune::crypto::scram;

TEST(ScramRegression, Rfc7677Sha256VectorProofAndServerSignature) {
    const auto salt = rc::base64Decode("W22ZaJ0SNY7soEsUEjb6gQ==").value();
    const std::string cfb  = rs::clientFirstBare("user", "rOprNGfwEbeRWgbNEkqO");
    const std::string nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const std::string sf   = rs::serverFirst(nonce, salt, 4096);
    const std::string cfwp = rs::clientFinalWithoutProof(nonce);
    EXPECT_EQ(cfb, "n=user,r=rOprNGfwEbeRWgbNEkqO");
    EXPECT_EQ(sf, "r=" + nonce + ",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
    EXPECT_EQ(cfwp, "c=biws,r=" + nonce);
    const std::string am = rs::authMessage(cfb, sf, cfwp);
    auto sp = rs::saltPassword(rc::Digest::Sha256, "pencil", salt, 4096);
    ASSERT_TRUE(sp.has_value());
    auto proof = rs::clientProof(rc::Digest::Sha256, *sp, am);
    ASSERT_TRUE(proof.has_value());
    EXPECT_EQ(rc::base64Encode(proof->view()), "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
    auto v = rs::makeVerifier(rc::Digest::Sha256, *sp, salt, 4096);
    ASSERT_TRUE(v.has_value());
    auto sig = rs::serverSignature(rc::Digest::Sha256, v->serverKey, am);
    ASSERT_TRUE(sig.has_value());
    EXPECT_EQ(rc::base64Encode(sig->view()), "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    EXPECT_TRUE(rs::verifyClientProof(rc::Digest::Sha256, v->storedKey, am, proof->view()));
}

TEST(ScramRegression, Sha512ExchangeVerifiesAndAWrongPasswordDoesNot) {
    auto v = rs::makeVerifier("correct horse battery");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->salt.size(), rs::kSaltBytes);
    EXPECT_EQ(v->iterations, rs::kDefaultIterations);
    EXPECT_EQ(v->storedKey.size, 64);
    const std::string cfb = rs::clientFirstBare("ws1", rs::makeNonce());
    const std::string sf  = rs::serverFirst("cn+sn", v->salt, v->iterations);
    const std::string am  = rs::authMessage(cfb, sf, rs::clientFinalWithoutProof("cn+sn"));
    auto good = rs::saltPassword(rc::Digest::Sha512, "correct horse battery", v->salt, v->iterations);
    auto bad  = rs::saltPassword(rc::Digest::Sha512, "correct horse batterY", v->salt, v->iterations);
    EXPECT_TRUE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, rs::clientProof(rc::Digest::Sha512, *good, am)->view()));
    EXPECT_FALSE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, rs::clientProof(rc::Digest::Sha512, *bad, am)->view()));
    EXPECT_EQ(*rs::expectedServerSignature(rc::Digest::Sha512, *good, am),
              *rs::serverSignature(rc::Digest::Sha512, v->serverKey, am)) << "the client can verify the server";
}

TEST(ScramRegression, IterationBoundsAreEnforced) {
    const std::vector<uint8_t> salt(16, 7);
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", salt, rs::kMinIterations - 1).has_value());
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", salt, rs::kMaxIterations + 1).has_value());
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", {}, rs::kMinIterations).has_value()) << "empty salt";
}

TEST(ScramRegression, ProofOfTheWrongLengthNeverVerifies) {
    auto v = rs::makeVerifier("some long password");
    const std::vector<uint8_t> shortProof(63, 0);
    EXPECT_FALSE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, "am", shortProof));
}

TEST(ScramRegression, NoncesArePrintableWithoutCommasAndUnique) {
    const std::string a = rs::makeNonce(), b = rs::makeNonce();
    EXPECT_NE(a, b);
    EXPECT_EQ(a.find(','), std::string::npos);
    for (char c : a) EXPECT_TRUE(c >= 0x21 && c <= 0x7e);
}
