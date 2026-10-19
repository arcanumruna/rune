#include <gtest/gtest.h>
#include "rune/crypto/Cipher.h"
TEST(Caps, BestCipherIsOneOfTheTwo) {
    auto c = rune::crypto::bestLocalCipher();
    EXPECT_TRUE(c == rune::crypto::Cipher::Aes256Gcm || c == rune::crypto::Cipher::ChaCha20Poly1305);
}
TEST(Caps, HardwareAesImpliesAesPreferred) {
    if (rune::crypto::hasHardwareAes())
        EXPECT_EQ(rune::crypto::bestLocalCipher(), rune::crypto::Cipher::Aes256Gcm);
    else
        EXPECT_EQ(rune::crypto::bestLocalCipher(), rune::crypto::Cipher::ChaCha20Poly1305);
}
TEST(Caps, HardwareAesMatchesIndependentGroundTruth) {
#if defined(__x86_64__) || defined(_M_X64)
    __builtin_cpu_init();
    EXPECT_EQ(rune::crypto::hasHardwareAes(), __builtin_cpu_supports("aes") != 0);
#else
    GTEST_SKIP() << "independent AES-NI ground truth only defined on x86_64";
#endif
}
