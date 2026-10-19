// rune-crypto: hash / HMAC / PBKDF2 / HKDF / secure random / constant-time compare / base64 and the
// cluster-key derivation (authentication plan D32).  OpenSSL 3 EVP only; every failure returns false /
// nullopt (fail closed), nothing allocates except the base64 conveniences.
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::crypto {

enum class Digest : uint8_t { Sha256 = 0, Sha512 = 1 };
inline constexpr std::size_t kMaxDigest = 64;
[[nodiscard]] constexpr std::size_t digestSize(Digest d) noexcept { return d == Digest::Sha256 ? 32 : 64; }

/// Fixed storage: never allocates.
struct DigestBytes {
    std::array<uint8_t, kMaxDigest> bytes{};
    uint8_t size = 0;
    [[nodiscard]] std::span<const uint8_t> view() const noexcept { return {bytes.data(), size}; }
    bool operator==(const DigestBytes& o) const noexcept;   // constant-time over size
};

[[nodiscard]] bool hash(Digest, std::span<const uint8_t> msg, DigestBytes& out) noexcept;
[[nodiscard]] bool hmac(Digest, std::span<const uint8_t> key, std::span<const uint8_t> msg, DigestBytes& out) noexcept;
[[nodiscard]] bool pbkdf2(Digest, std::string_view password, std::span<const uint8_t> salt,
                          uint32_t iterations, DigestBytes& out) noexcept;      // dkLen = digestSize
[[nodiscard]] bool hkdf(Digest, std::span<const uint8_t> ikm, std::span<const uint8_t> salt,
                        std::span<const uint8_t> info, std::span<uint8_t> out) noexcept;   // extract + expand
[[nodiscard]] bool randomBytes(std::span<uint8_t> out) noexcept;                // RAND_bytes; false = refuse
[[nodiscard]] bool constantTimeEqual(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept;
[[nodiscard]] std::string base64Encode(std::span<const uint8_t> in);
[[nodiscard]] std::optional<std::vector<uint8_t>> base64Decode(std::string_view in);   // strict, padded

/// D32 — the ONE derivation of a cluster key file (the broker's ClusterKeys and the CLI's bootstrap MAC):
/// contents with trailing whitespace trimmed (>= 32 bytes, else nullopt); master = HKDF-Extract(SHA-512,
/// salt "rune-cluster-key", contents); keyId = first 8 bytes (LE) of HMAC-SHA-512(master, "rune-cluster-key-id").
struct ClusterKeyMaterial { std::array<uint8_t, 64> master{}; uint64_t keyId = 0; };
[[nodiscard]] std::optional<ClusterKeyMaterial> deriveClusterKey(std::span<const uint8_t> fileContents) noexcept;

} // namespace rune::crypto
