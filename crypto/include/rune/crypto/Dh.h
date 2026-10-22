// rune-crypto Dh: X25519 (RFC 7748) ephemeral Diffie-Hellman for the inter-broker link handshake (issue #242).
// OpenSSL 3 EVP_PKEY_X25519 only, the same backend as Aead.cpp and Kdf.cpp; every failure returns false (fail
// closed) and every temporary secret is scrubbed.  Cold path: one keypair per handshake attempt.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

namespace rune::crypto {

inline constexpr std::size_t kX25519Bytes = 32;

/// A fresh X25519 keypair.  On failure both outputs are zeroed.
[[nodiscard]] bool x25519Keypair(std::array<uint8_t, kX25519Bytes>& priv, std::array<uint8_t, kX25519Bytes>& pub) noexcept;

/// shared = X25519(priv, peerPub).  False (and `shared` zeroed) on any OpenSSL error and on an all-zero result - a
/// low-order peer point, which would make the "secret" public (checked in constant time).
[[nodiscard]] bool x25519Shared(const std::array<uint8_t, kX25519Bytes>& priv, const std::array<uint8_t, kX25519Bytes>& peerPub,
                                std::array<uint8_t, kX25519Bytes>& shared) noexcept;

} // namespace rune::crypto
