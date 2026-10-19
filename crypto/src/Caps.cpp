#include "rune/crypto/Cipher.h"
#include <openssl/crypto.h>

#if defined(__x86_64__) || defined(_M_X64)
// OpenSSL exports the processor-capability vector OPENSSL_ia32cap_P from
// libcrypto (set up by OPENSSL_cpuid_setup at load time) but does NOT declare
// it in any public 3.x header — <openssl/crypto.h> only exposes the accessor
// semantics, not the symbol.  Declare it ourselves (external linkage, resolved
// against OpenSSL::Crypto at link time).  Four 32-bit words on x86_64; we read
// word[1] bit 25.  (The arm64 host never compiles this branch, which is why
// the missing declaration only surfaced on the real x86_64 build.)
extern "C" unsigned int OPENSSL_ia32cap_P[4];
#endif

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
