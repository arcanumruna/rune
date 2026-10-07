#include "rune/crypto/Cipher.h"
#include <openssl/crypto.h>
namespace rune::crypto {
bool hasHardwareAes() noexcept {
    // OPENSSL_ia32cap_P is an array of 32-bit words; AES-NI is capability
    // bit 57 overall, i.e. word[1] bit (57-32)=25 (matches OpenSSL's own
    // AESNI_CAPABLE in crypto/aes_platform.h). On other arches OpenSSL still
    // runtime-selects HW AES-GCM, so fall back to "AES-GCM fetch succeeds + fast".
#if defined(__x86_64__) || defined(_M_X64)
    return (OPENSSL_ia32cap_P[1] & (1u << (57 - 32))) != 0;
#else
    return true; // ARMv8 crypto / other: OpenSSL picks HW impl when present
#endif
}
Cipher bestLocalCipher() noexcept {
    return hasHardwareAes() ? Cipher::Aes256Gcm : Cipher::ChaCha20Poly1305;
}
}
