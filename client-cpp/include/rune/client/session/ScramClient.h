// rune/client/session/ScramClient.h
//
// Authentication (spec §5, D5): the client half of one SCRAM-SHA-512 exchange.
// Namespace: rune::client::session
//
//   - ScramSentFirst / ScramSentFinal / ScramExchange: the variables of the
//     SessionMachine's Authenticating state and nothing else (rule 3).
//   - ScramCache: the duty thread's salted-password cache.  PBKDF2 is the
//     expensive step of a login; a client holding one session per broker pays it
//     once per verifier, not once per session (every broker replicates the same
//     verifier, hence the same (salt, iterations)).  Fixed storage, no heap,
//     never logged, zeroed on eviction and on destruction.  The key is (password tag,
//     salt, iterations): two passwords never share an entry.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <variant>

#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>

namespace rune::client::session {

/// AuthClientFirst sent; waiting for the broker's AuthServerFirst.
struct ScramSentFirst {
    std::string clientNonce;
    std::string clientFirstBare;
    bool        sent = false;   // the transport accepted the frame; false = back-pressured, re-offered every tick
};

/// AuthClientFinal sent (one proof per active verifier); waiting for AuthServerFinal.  `authMessages` and
/// `expected` are indexed by verifier; `count` is how many (1 or 2, D5).
struct ScramSentFinal {
    std::array<std::string, 2>                authMessages;
    std::array<rune::crypto::DigestBytes, 2>  expected;     // the server signature each verifier would sign with
    uint8_t                                   count = 1;
    // Issue #114: what a later own-credential change of this session is proven with - the combined nonce and the
    // (salt, iterations) of each slot (the salted password itself stays in the ScramCache: no extra key copy).
    std::string                               combinedNonce;
    std::array<std::string, 2>                salts;
    std::array<uint32_t, 2>                   iterations{};
    bool                                      sent = false;   // as ScramSentFirst::sent
};

using ScramExchange = std::variant<ScramSentFirst, ScramSentFinal>;

class ScramCache {
public:
    static constexpr std::size_t kCapacity = 8;
    static constexpr std::size_t kMaxSalt  = 64;   // rune::protocol::client::kAuthSaltMax

    /// Draws the cache's random tag key (#239).  If the system RNG refuses, nothing is ever cached (every login
    /// salts afresh) — the cache never falls back to a guessable tag.
    ScramCache() noexcept;
    ~ScramCache();
    ScramCache(const ScramCache&)            = delete;
    ScramCache& operator=(const ScramCache&) = delete;

    /// The cached Hi(password, salt, iterations), or nullptr.  The password is part of the key (as a keyed tag).
    [[nodiscard]] const rune::crypto::scram::SaltedPassword* find(std::string_view password,
                                                                  std::span<const uint8_t> salt,
                                                                  uint32_t iterations) const noexcept;
    /// Stores one entry (evicting the oldest, zeroed first).  A salt longer than kMaxSalt is not cached.
    void put(std::string_view password, std::span<const uint8_t> salt, uint32_t iterations,
             const rune::crypto::scram::SaltedPassword& sp) noexcept;

    /// find(), else PBKDF2 + put().  nullptr = local crypto failure.  The pointer is valid until the next call.
    [[nodiscard]] const rune::crypto::scram::SaltedPassword* saltedPassword(std::string_view password,
                                                                            std::span<const uint8_t> salt,
                                                                            uint32_t iterations) noexcept;

    /// Zeroes and forgets everything.
    void clear() noexcept;

    // ---- test seam (counters only; never key material) ----
    [[nodiscard]] uint64_t hits() const noexcept { return hits_; }
    [[nodiscard]] uint64_t pbkdf2Computations() const noexcept { return computations_; }
    [[nodiscard]] std::size_t size() const noexcept;
    /// The password's tag under THIS cache's key (#239 regression seam: two caches tag one password differently).
    [[nodiscard]] bool tagForTest(std::string_view password, std::array<uint8_t, 64>& out) const noexcept {
        return tagOf_(password, out);
    }

private:
    struct Entry {
        std::array<uint8_t, kMaxSalt>          salt{};
        uint8_t                                saltLen = 0;
        uint32_t                               iterations = 0;     // 0 = empty
        std::array<uint8_t, 64>                pwTag{};            // HMAC-SHA-512(tagKey_, password) — not the password
        rune::crypto::scram::SaltedPassword    sp{};
    };
    using Tag = std::array<uint8_t, 64>;
    static void wipe_(Entry& e) noexcept;
    [[nodiscard]] bool tagOf_(std::string_view password, Tag& out) const noexcept;
    [[nodiscard]] const rune::crypto::scram::SaltedPassword* find_(const Tag& tag, std::span<const uint8_t> salt,
                                                                   uint32_t iterations) const noexcept;
    void put_(const Tag& tag, std::span<const uint8_t> salt, uint32_t iterations,
              const rune::crypto::scram::SaltedPassword& sp) noexcept;

    /// #239: a per-cache random key, so a tag can be neither precomputed nor matched across processes; the
    /// password is the HMAC message, not the key.  Wiped by the destructor.
    std::array<uint8_t, 32>      tagKey_{};
    bool                         tagKeyed_ = false;   // false = the RNG refused: cache nothing
    std::array<Entry, kCapacity> entries_{};
    Entry                        overflow_{};      // a salt too long to cache: held for the caller
    std::size_t                  next_ = 0;        // round-robin victim
    uint64_t                     hits_ = 0;
    uint64_t                     computations_ = 0;
};

} // namespace rune::client::session
