// rune-crypto Dh: X25519 through OpenSSL 3 EVP_PKEY_X25519 (issue #242).  Every step checked, any failure returns
// false with the outputs zeroed (fail closed); every temporary secret is scrubbed before return.
#include "rune/crypto/Dh.h"
#include "rune/crypto/Kdf.h"
#include <openssl/crypto.h>
#include <openssl/evp.h>

namespace rune::crypto {

bool x25519Keypair(std::array<uint8_t, kX25519Bytes>& priv, std::array<uint8_t, kX25519Bytes>& pub) noexcept {
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_X25519, nullptr);
    EVP_PKEY* key = nullptr;
    size_t privLen = priv.size(), pubLen = pub.size();
    const bool ok = ctx != nullptr && EVP_PKEY_keygen_init(ctx) == 1 && EVP_PKEY_keygen(ctx, &key) == 1 && key != nullptr &&
                    EVP_PKEY_get_raw_private_key(key, priv.data(), &privLen) == 1 && privLen == priv.size() &&
                    EVP_PKEY_get_raw_public_key(key, pub.data(), &pubLen) == 1 && pubLen == pub.size();
    EVP_PKEY_free(key);        // OpenSSL clears the key's own private bytes on free
    EVP_PKEY_CTX_free(ctx);
    if (!ok) { secureZero(priv); secureZero(pub); }
    return ok;
}

bool x25519Shared(const std::array<uint8_t, kX25519Bytes>& priv, const std::array<uint8_t, kX25519Bytes>& peerPub,
                  std::array<uint8_t, kX25519Bytes>& shared) noexcept {
    EVP_PKEY* own = EVP_PKEY_new_raw_private_key(EVP_PKEY_X25519, nullptr, priv.data(), priv.size());
    EVP_PKEY* peer = EVP_PKEY_new_raw_public_key(EVP_PKEY_X25519, nullptr, peerPub.data(), peerPub.size());
    EVP_PKEY_CTX* ctx = own != nullptr ? EVP_PKEY_CTX_new(own, nullptr) : nullptr;
    size_t len = shared.size();
    bool ok = own != nullptr && peer != nullptr && ctx != nullptr && EVP_PKEY_derive_init(ctx) == 1 &&
              EVP_PKEY_derive_set_peer(ctx, peer) == 1 && EVP_PKEY_derive(ctx, shared.data(), &len) == 1 &&
              len == shared.size();
    EVP_PKEY_CTX_free(ctx);
    EVP_PKEY_free(peer);
    EVP_PKEY_free(own);
    // A low-order peer point yields the all-zero "secret" (RFC 7748 §6.1): refuse it.  Constant time.
    static constexpr std::array<uint8_t, kX25519Bytes> kZero{};
    if (ok && CRYPTO_memcmp(shared.data(), kZero.data(), shared.size()) == 0) ok = false;
    if (!ok) secureZero(shared);
    return ok;
}

} // namespace rune::crypto
