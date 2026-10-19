#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include "rune/crypto/Aead.h"
using namespace rune::crypto;
static AeadKey k1() { AeadKey k{}; for (int i=0;i<32;++i) k.bytes[i]=uint8_t(i+1); return k; }
static void each(const std::function<void(Cipher)>& f){ f(Cipher::Aes256Gcm); f(Cipher::ChaCha20Poly1305); }

TEST(Aead, RoundTripRecoversPlaintext) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4};
    AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    const char* msg="produce-batch-body"; uint8_t aad[8]={9,8,7,6,5,4,3,2};
    std::vector<uint8_t> ct(strlen(msg)+kTagSize), pt(strlen(msg));
    size_t n=s.seal(1,aad,8,(const uint8_t*)msg,strlen(msg),ct.data());
    ASSERT_EQ(n,strlen(msg)+kTagSize);
    long m=o.open(1,aad,8,ct.data(),n,pt.data());
    ASSERT_EQ(m,(long)strlen(msg));
    EXPECT_EQ(0,memcmp(pt.data(),msg,strlen(msg)));
  });
}
TEST(Aead, TamperedCiphertextFailsOpen) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4}; AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    uint8_t in[16]={0}; std::vector<uint8_t> ct(16+kTagSize), pt(16);
    size_t n=s.seal(1,nullptr,0,in,16,ct.data());
    ct[3]^=0x40; // flip a ciphertext byte
    EXPECT_EQ(-1, o.open(1,nullptr,0,ct.data(),n,pt.data()));
  });
}
TEST(Aead, DeriveSenderKeyZeroIsB2cUnchanged) {
  AeadKey b2c = k1();
  AeadKey out{};
  ASSERT_TRUE(deriveSenderKey(b2c, 0, out));
  EXPECT_EQ(0, memcmp(out.bytes, b2c.bytes, sizeof(out.bytes)));
}
TEST(Aead, DeriveSenderKeyDistinctPerSender) {
  AeadKey b2c = k1();
  AeadKey k0{}, k1_{}, k2{};
  ASSERT_TRUE(deriveSenderKey(b2c, 0, k0));
  ASSERT_TRUE(deriveSenderKey(b2c, 1, k1_));
  ASSERT_TRUE(deriveSenderKey(b2c, 2, k2));
  EXPECT_NE(0, memcmp(k1_.bytes, b2c.bytes, sizeof(b2c.bytes)));
  EXPECT_NE(0, memcmp(k2.bytes, b2c.bytes, sizeof(b2c.bytes)));
  EXPECT_NE(0, memcmp(k1_.bytes, k2.bytes, sizeof(b2c.bytes)));
}
TEST(Aead, DeriveSenderKeyIsDeterministic) {
  AeadKey b2c = k1();
  AeadKey a{}, b{};
  ASSERT_TRUE(deriveSenderKey(b2c, 7, a));
  ASSERT_TRUE(deriveSenderKey(b2c, 7, b));
  EXPECT_EQ(0, memcmp(a.bytes, b.bytes, sizeof(a.bytes)));
}
// Known-answer test: pins HKDF-Expand(PRK=b2c, info="rune-b2c-sender"||0x01, SHA-256, L=32)
// for the fixed key k1(). If the derivation ever changes, this MUST fail -- that would
// break interop between client and broker (both must derive the identical bytes).
TEST(Aead, DeriveSenderKeyOneKnownAnswer) {
  AeadKey b2c = k1();
  AeadKey out{};
  ASSERT_TRUE(deriveSenderKey(b2c, 1, out));
  // Cross-checked against an independent RFC 5869 HKDF-Expand implementation (Python
  // hashlib/hmac), not just this code's own output, so this pins the actual standard --
  // not merely self-consistency.
  static const uint8_t expected[32] = {
    0x42, 0xf4, 0x3c, 0xa3, 0x73, 0x8d, 0x7c, 0xcb,
    0xeb, 0xd3, 0xd5, 0x20, 0x6d, 0x25, 0x3b, 0x92,
    0x5b, 0x60, 0x76, 0xe0, 0x55, 0x3d, 0x82, 0x81,
    0xc7, 0x29, 0x0a, 0x3e, 0x5d, 0x38, 0xb6, 0x17,
  };
  EXPECT_EQ(0, memcmp(out.bytes, expected, sizeof(expected)))
      << "senderId=1 derivation changed -- this breaks interop; "
         "only update `expected` if the HKDF derivation intentionally changed on BOTH peers.";
}

TEST(Aead, WrongSeqOrAadFailsOpen) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4}; AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    uint8_t in[16]={0}, aad[4]={1,1,1,1}; std::vector<uint8_t> ct(16+kTagSize), pt(16);
    size_t n=s.seal(5,aad,4,in,16,ct.data());
    EXPECT_EQ(-1,o.open(6,aad,4,ct.data(),n,pt.data()));   // wrong seq (nonce)
    EXPECT_EQ(-1,o.open(5,nullptr,0,ct.data(),n,pt.data())); // wrong aad
  });
}
