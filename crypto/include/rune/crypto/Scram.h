// rune-crypto SCRAM (RFC 5802 / 7677).  The construction is generic over the digest so RFC 7677's
// SCRAM-SHA-256 vector pins it; production uses SHA-512 only (authentication plan D7).
#pragma once
#include "rune/crypto/Kdf.h"
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::crypto::scram {

inline constexpr uint32_t    kDefaultIterations = 4096, kMinIterations = 4096, kMaxIterations = 1'000'000;
inline constexpr std::size_t kSaltBytes = 16, kNonceBytes = 24;

struct SaltedPassword { DigestBytes bytes; };                 // Hi(password, salt, i) — the client caches it
struct Verifier { Digest digest = Digest::Sha512; std::vector<uint8_t> salt; uint32_t iterations = 0;
                  DigestBytes storedKey, serverKey; };

[[nodiscard]] std::optional<SaltedPassword> saltPassword(Digest, std::string_view password,
                                                         std::span<const uint8_t> salt, uint32_t iterations) noexcept;
[[nodiscard]] std::optional<Verifier> makeVerifier(Digest, const SaltedPassword&, std::span<const uint8_t> salt,
                                                   uint32_t iterations);
[[nodiscard]] std::optional<Verifier> makeVerifier(std::string_view password, uint32_t iterations = kDefaultIterations);
[[nodiscard]] std::string makeNonce();                        // kNonceBytes random, base64 (no ','); "" = local failure

// RFC 5802 message texts (gs2 header "n,,", no channel binding — D7)
[[nodiscard]] std::string clientFirstBare(std::string_view user, std::string_view clientNonce);   // "n=<u>,r=<c>"
[[nodiscard]] std::string serverFirst(std::string_view combinedNonce, std::span<const uint8_t> salt, uint32_t i);
[[nodiscard]] std::string clientFinalWithoutProof(std::string_view combinedNonce);               // "c=biws,r=<n>"
[[nodiscard]] std::string authMessage(std::string_view cfb, std::string_view sf, std::string_view cfwp);
[[nodiscard]] std::optional<DigestBytes> clientProof(Digest, const SaltedPassword&, std::string_view authMessage) noexcept;
[[nodiscard]] std::optional<DigestBytes> expectedServerSignature(Digest, const SaltedPassword&,
                                                                 std::string_view authMessage) noexcept;
[[nodiscard]] std::optional<DigestBytes> serverSignature(Digest, const DigestBytes& serverKey,
                                                         std::string_view authMessage) noexcept;
/// H(ClientKey) == StoredKey where ClientKey = proof XOR HMAC(StoredKey, authMessage) — constant time.
[[nodiscard]] bool verifyClientProof(Digest, const DigestBytes& storedKey, std::string_view authMessage,
                                     std::span<const uint8_t> proof) noexcept;

} // namespace rune::crypto::scram
