#include "rune/crypto/Aead.h"
#include <openssl/evp.h>
#include <cstring>
namespace rune::crypto {
static const EVP_CIPHER* evpFor(Cipher c){
  return c==Cipher::Aes256Gcm ? EVP_aes_256_gcm() : EVP_chacha20_poly1305();
}
AeadCipher::AeadCipher(Cipher c, const AeadKey& key, const uint8_t pfx[kNoncePrefix]) noexcept
  : ctx_(EVP_CIPHER_CTX_new()), evp_(evpFor(c)) {
  std::memcpy(prefix_, pfx, kNoncePrefix);
  // Install the key once; nonce is set per call (key schedule expanded once).
  EVP_CipherInit_ex(ctx_, evp_, nullptr, key.bytes, nullptr, -1);
}
AeadCipher::~AeadCipher(){ if(ctx_) EVP_CIPHER_CTX_free(ctx_); }
AeadCipher::AeadCipher(AeadCipher&& o) noexcept : ctx_(o.ctx_), evp_(o.evp_){ std::memcpy(prefix_,o.prefix_,kNoncePrefix); o.ctx_=nullptr; }
AeadCipher& AeadCipher::operator=(AeadCipher&& o) noexcept { if(this!=&o){ if(ctx_)EVP_CIPHER_CTX_free(ctx_); ctx_=o.ctx_; evp_=o.evp_; std::memcpy(prefix_,o.prefix_,kNoncePrefix); o.ctx_=nullptr;} return *this; }

static void makeNonce(const uint8_t pfx[4], uint64_t seq, uint8_t nonce[12]){
  std::memcpy(nonce, pfx, 4);
  for(int i=0;i<8;++i) nonce[4+i]=uint8_t(seq>>(8*(7-i))); // big-endian seq
}
size_t AeadCipher::seal(uint64_t seq,const uint8_t* aad,size_t aadLen,
                        const uint8_t* plain,size_t plainLen,uint8_t* out) noexcept {
  uint8_t nonce[12]; makeNonce(prefix_,seq,nonce);
  int len=0;
  // Fail closed: if the nonce-reset step itself silently failed, the ctx would still hold
  // the PRIOR nonce, so encrypting now would reuse keystream/tag under the same key — the
  // caller must not transmit, hence 0 ("do not transmit") rather than proceeding.
  if(EVP_EncryptInit_ex(ctx_,nullptr,nullptr,nullptr,nonce)!=1) return 0; // reuse key, new nonce
  if(aadLen){ if(EVP_EncryptUpdate(ctx_,nullptr,&len,aad,(int)aadLen)!=1) return 0; }
  if(EVP_EncryptUpdate(ctx_,out,&len,plain,(int)plainLen)!=1) return 0;
  int fin=0; if(EVP_EncryptFinal_ex(ctx_,out+len,&fin)!=1) return 0;
  if(EVP_CIPHER_CTX_ctrl(ctx_,EVP_CTRL_AEAD_GET_TAG,(int)kTagSize,out+plainLen)!=1) return 0;
  return plainLen+kTagSize;
}
long AeadCipher::open(uint64_t seq,const uint8_t* aad,size_t aadLen,
                      const uint8_t* ct,size_t ctLen,uint8_t* out) noexcept {
  if(ctLen<kTagSize) return -1;
  size_t bodyLen=ctLen-kTagSize;
  uint8_t nonce[12]; makeNonce(prefix_,seq,nonce);
  int len=0;
  if(EVP_DecryptInit_ex(ctx_,nullptr,nullptr,nullptr,nonce)!=1) return -1;
  if(aadLen){ if(EVP_DecryptUpdate(ctx_,nullptr,&len,aad,(int)aadLen)!=1) return -1; }
  if(EVP_DecryptUpdate(ctx_,out,&len,ct,(int)bodyLen)!=1) return -1;
  if(EVP_CIPHER_CTX_ctrl(ctx_,EVP_CTRL_AEAD_SET_TAG,(int)kTagSize,(void*)(ct+bodyLen))!=1) return -1;
  int fin=0;
  if(EVP_DecryptFinal_ex(ctx_,out+len,&fin)<=0) return -1; // tag mismatch
  return (long)bodyLen;
}
}
