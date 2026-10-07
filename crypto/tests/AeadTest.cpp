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
TEST(Aead, WrongSeqOrAadFailsOpen) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4}; AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    uint8_t in[16]={0}, aad[4]={1,1,1,1}; std::vector<uint8_t> ct(16+kTagSize), pt(16);
    size_t n=s.seal(5,aad,4,in,16,ct.data());
    EXPECT_EQ(-1,o.open(6,aad,4,ct.data(),n,pt.data()));   // wrong seq (nonce)
    EXPECT_EQ(-1,o.open(5,nullptr,0,ct.data(),n,pt.data())); // wrong aad
  });
}
