#pragma once
#include <cstddef>
#include <cstdint>
#include "rune/crypto/Cipher.h"

typedef struct evp_cipher_ctx_st EVP_CIPHER_CTX;
typedef struct evp_cipher_st EVP_CIPHER;

namespace rune::crypto {
struct AeadKey { uint8_t bytes[32]; };            // 256-bit
inline constexpr size_t kTagSize = 16;
inline constexpr size_t kNoncePrefix = 4;         // 4-byte fixed + 8-byte seq = 96-bit nonce
class AeadCipher {
public:
    // noncePrefix: 4 bytes bound to (sessionId, direction) so c2b/b2c never share a nonce space.
    AeadCipher(Cipher c, const AeadKey& key, const uint8_t noncePrefix[kNoncePrefix]) noexcept;
    ~AeadCipher();
    AeadCipher(AeadCipher&&) noexcept; AeadCipher& operator=(AeadCipher&&) noexcept;
    AeadCipher(const AeadCipher&) = delete; AeadCipher& operator=(const AeadCipher&) = delete;
    // out must have room for plainLen + kTagSize. noexcept, no alloc.
    // Returns plainLen + kTagSize on success, or 0 on cipher error (e.g. the underlying
    // EVP nonce-reset/update/finalize/get-tag call failed). On 0 the caller MUST NOT
    // transmit `out` — treating a cipher error as success would risk encrypting under a
    // stale nonce (keystream/tag reuse under the same key).
    size_t seal(uint64_t seq, const uint8_t* aad, size_t aadLen,
                const uint8_t* plain, size_t plainLen, uint8_t* out) noexcept;
    // out must have room for ctLen - kTagSize. no alloc.
    // Returns the plaintext length on success, or -1 on auth failure (tag mismatch) or
    // cipher error. On -1 the contents of `out` are UNSPECIFIED and UNAUTHENTICATED —
    // the caller MUST discard them and must not act on any bytes written there.
    long open(uint64_t seq, const uint8_t* aad, size_t aadLen,
              const uint8_t* ct, size_t ctLen, uint8_t* out) noexcept;
private:
    EVP_CIPHER_CTX* ctx_; const EVP_CIPHER* evp_; uint8_t prefix_[kNoncePrefix];
};
}
