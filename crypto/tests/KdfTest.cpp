// rune-crypto: the primitives authentication is built on, pinned to their RFC vectors.
#include "rune/crypto/Kdf.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace rc = rune::crypto;
namespace {
std::vector<uint8_t> hex(std::string_view h) {
    std::vector<uint8_t> v;
    for (std::size_t i = 0; i + 1 < h.size(); i += 2) v.push_back(static_cast<uint8_t>(std::stoi(std::string(h.substr(i, 2)), nullptr, 16)));
    return v;
}
std::span<const uint8_t> bytesOf(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
} // namespace

TEST(KdfRegression, HmacSha512MatchesRfc4231Case1) {
    const std::vector<uint8_t> key(20, 0x0b);
    rc::DigestBytes out;
    ASSERT_TRUE(rc::hmac(rc::Digest::Sha512, key, bytesOf("Hi There"), out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cde"
                  "daa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854"));
}

TEST(KdfRegression, HmacSha256MatchesRfc4231Case1) {
    const std::vector<uint8_t> key(20, 0x0b);
    rc::DigestBytes out;
    ASSERT_TRUE(rc::hmac(rc::Digest::Sha256, key, bytesOf("Hi There"), out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"));
}

TEST(KdfRegression, HkdfSha256MatchesRfc5869Case1) {
    const std::vector<uint8_t> ikm(22, 0x0b);
    const auto salt = hex("000102030405060708090a0b0c");
    const auto info = hex("f0f1f2f3f4f5f6f7f8f9");
    std::vector<uint8_t> okm(42);
    ASSERT_TRUE(rc::hkdf(rc::Digest::Sha256, ikm, salt, info, okm));
    EXPECT_EQ(okm, hex("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"));
}

TEST(KdfRegression, ConstantTimeEqualComparesLengthAndBytes) {
    const std::vector<uint8_t> a{1, 2, 3}, b{1, 2, 3}, c{1, 2, 4}, d{1, 2};
    EXPECT_TRUE(rc::constantTimeEqual(a, b));
    EXPECT_FALSE(rc::constantTimeEqual(a, c));
    EXPECT_FALSE(rc::constantTimeEqual(a, d));
}

TEST(KdfRegression, Base64RoundTripsAndRejectsGarbage) {
    const std::vector<uint8_t> v{0, 1, 2, 250, 251, 252, 253};
    const std::string s = rc::base64Encode(v);
    auto back = rc::base64Decode(s);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, v);
    EXPECT_FALSE(rc::base64Decode("not base64!").has_value());
    EXPECT_FALSE(rc::base64Decode("abc").has_value()) << "unpadded input is refused (strict)";
}

TEST(KdfRegression, ClusterKeyDerivationTrimsAndRefusesShortKeys) {
    const std::string k(40, 'k');
    auto a = rc::deriveClusterKey(bytesOf(k)), b = rc::deriveClusterKey(bytesOf(k + "\n  "));
    ASSERT_TRUE(a && b);
    EXPECT_EQ(a->keyId, b->keyId);
    EXPECT_EQ(a->master, b->master);
    EXPECT_NE(a->keyId, rc::deriveClusterKey(bytesOf(std::string(40, 'j')))->keyId);
    EXPECT_FALSE(rc::deriveClusterKey(bytesOf(std::string(31, 'k'))).has_value());
}

TEST(KdfRegression, RandomBytesAreNotAllZeroAndDiffer) {
    std::array<uint8_t, 32> a{}, b{};
    ASSERT_TRUE(rc::randomBytes(a));
    ASSERT_TRUE(rc::randomBytes(b));
    EXPECT_NE(a, b);
}

TEST(KdfRegression, Pbkdf2Sha256MatchesPublishedVectorAt4096) {
    rc::DigestBytes out;
    const auto salt = hex("73616c74");   // "salt"
    ASSERT_TRUE(rc::pbkdf2(rc::Digest::Sha256, "password", salt, 4096, out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"));
}

TEST(KdfRegression, Pbkdf2Sha512MatchesPublishedVectors) {
    rc::DigestBytes out;
    const auto salt = hex("73616c74");
    ASSERT_TRUE(rc::pbkdf2(rc::Digest::Sha512, "password", salt, 1, out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252"
                  "c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce"));
    ASSERT_TRUE(rc::pbkdf2(rc::Digest::Sha512, "password", salt, 2, out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("e1d9c16aa681708a45f5c7c4e215ceb66e011a2e9f0040713f18aefdb866d53c"
                  "f76cab2868a39b9f7840edce4fef5a82be67335c77a6068e04112754f27ccf4e"));
    EXPECT_FALSE(rc::pbkdf2(rc::Digest::Sha512, "password", salt, 0, out)) << "zero iterations refused";
}

TEST(KdfRegression, HmacSha512MatchesRfc4231Case2AndAcceptsEmptyInputs) {
    rc::DigestBytes out;
    ASSERT_TRUE(rc::hmac(rc::Digest::Sha512, bytesOf("Jefe"), bytesOf("what do ya want for nothing?"), out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea250554"
                  "9758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737"));
    EXPECT_TRUE(rc::hmac(rc::Digest::Sha512, {}, {}, out));
    EXPECT_EQ(out.size, 64);
}

TEST(KdfRegression, HkdfSha256MatchesRfc5869Case3EmptySaltAndInfo) {
    const std::vector<uint8_t> ikm(22, 0x0b);
    std::vector<uint8_t> okm(42);
    ASSERT_TRUE(rc::hkdf(rc::Digest::Sha256, ikm, {}, {}, okm));
    EXPECT_EQ(okm, hex("8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"));
    EXPECT_FALSE(rc::hkdf(rc::Digest::Sha256, ikm, {}, {}, std::span<uint8_t>{})) << "empty output refused";
}

TEST(KdfRegression, DigestBytesEqualityIsOverSizeAndContent) {
    rc::DigestBytes a, b;
    ASSERT_TRUE(rc::hash(rc::Digest::Sha256, bytesOf("x"), a));
    ASSERT_TRUE(rc::hash(rc::Digest::Sha256, bytesOf("x"), b));
    EXPECT_TRUE(a == b);
    ASSERT_TRUE(rc::hash(rc::Digest::Sha512, bytesOf("x"), b));
    EXPECT_FALSE(a == b);
}

TEST(KdfRegression, Base64MatchesRfc4648AndRefusesMisplacedPadding) {
    EXPECT_EQ(rc::base64Encode(std::vector<uint8_t>{'f', 'o'}), "Zm8=");
    EXPECT_EQ(rc::base64Encode(std::vector<uint8_t>{'f'}), "Zg==");
    EXPECT_EQ(rc::base64Encode({}), "");
    EXPECT_EQ(*rc::base64Decode("Zm9vYg=="), (std::vector<uint8_t>{'f', 'o', 'o', 'b'}));
    EXPECT_TRUE(rc::base64Decode("")->empty());
    EXPECT_FALSE(rc::base64Decode("Zm=v").has_value());
    EXPECT_FALSE(rc::base64Decode("Z===").has_value());
    EXPECT_FALSE(rc::base64Decode("Zg==Zg==").has_value());
}

TEST(WorkerBindTokenRegression, DerivationIsStableKeyedAndPerSession) {
    std::array<uint8_t, 32> key{};
    for (std::size_t i = 0; i < key.size(); ++i) key[i] = static_cast<uint8_t>(i);
    const auto a = rc::deriveWorkerBindToken(key, 7);
    // Pinned vector (RFC 5869 HKDF-SHA256, cross-checked with an independent HMAC computation):
    // ikm = 00..1f, salt = 07 00 00 00 00 00 00 00, info = "rune-worker-bind-v1", first 16 bytes.
    const std::array<uint8_t, 16> pinned = {0x9e, 0xf9, 0xe6, 0xdc, 0x16, 0x05, 0xd1, 0xf4,
                                            0x83, 0x8f, 0x59, 0xd8, 0xcc, 0x07, 0x53, 0x06};
    EXPECT_EQ(a, pinned) << "the derivation changed: every deployed client and broker must agree";
    EXPECT_EQ(rc::deriveWorkerBindToken(key, 7), a) << "deterministic";
    EXPECT_NE(a, (std::array<uint8_t, 16>{})) << "never the all-zero 'none'";
    EXPECT_NE(rc::deriveWorkerBindToken(key, 8), a) << "another session id, another token";
    EXPECT_NE(rc::deriveWorkerBindToken(key, -7), a);
    auto key2 = key; key2[31] ^= 1;
    EXPECT_NE(rc::deriveWorkerBindToken(key2, 7), a) << "another key, another token";
    EXPECT_EQ(rc::deriveWorkerBindToken(std::span<const uint8_t>(key.data(), 31), 7), (std::array<uint8_t, 16>{})) << "wrong key size fails closed";
    // independent recomputation through the primitive
    std::array<uint8_t, 16> ref{};
    const uint8_t salt[8] = {7, 0, 0, 0, 0, 0, 0, 0};
    const std::string_view info = "rune-worker-bind-v1";
    ASSERT_TRUE(rc::hkdf(rc::Digest::Sha256, key, salt, {reinterpret_cast<const uint8_t*>(info.data()), info.size()}, ref));
    EXPECT_EQ(a, ref);
}
