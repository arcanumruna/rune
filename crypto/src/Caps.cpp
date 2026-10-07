#include "rune/crypto/Cipher.h"
#include <openssl/evp.h>
#include <openssl/crypto.h>
namespace rune::crypto {
bool hasHardwareAes() noexcept {
    // OPENSSL_ia32cap bit 57 = AES-NI on x86; on other arches OpenSSL still
    // runtime-selects HW AES-GCM, so fall back to "AES-GCM fetch succeeds + fast".
#if defined(__x86_64__) || defined(_M_X64)
    return (OPENSSL_ia32cap_P[0] & (1ULL << 57)) != 0;
#else
    return true; // ARMv8 crypto / other: OpenSSL picks HW impl when present
#endif
}
Cipher bestLocalCipher() noexcept {
    return hasHardwareAes() ? Cipher::Aes256Gcm : Cipher::ChaCha20Poly1305;
}
}
