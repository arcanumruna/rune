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
//     never logged, zeroed on eviction and on destruction.  One cache serves one
//     password (the Client's): the key is (salt, iterations) only.
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

    ScramCache() noexcept = default;
    ~ScramCache() { clear(); }
    ScramCache(const ScramCache&)            = delete;
    ScramCache& operator=(const ScramCache&) = delete;

    /// The cached Hi(password, salt, iterations), or nullptr.
    [[nodiscard]] const rune::crypto::scram::SaltedPassword* find(std::span<const uint8_t> salt,
                                                                  uint32_t iterations) const noexcept;
    /// Stores one entry (evicting the oldest, zeroed first).  A salt longer than kMaxSalt is not cached.
    void put(std::span<const uint8_t> salt, uint32_t iterations,
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

private:
    struct Entry {
        std::array<uint8_t, kMaxSalt>          salt{};
        uint8_t                                saltLen = 0;
        uint32_t                               iterations = 0;     // 0 = empty
        rune::crypto::scram::SaltedPassword    sp{};
    };
    static void wipe_(Entry& e) noexcept;

    std::array<Entry, kCapacity> entries_{};
    Entry                        overflow_{};      // a salt too long to cache: held for the caller
    std::size_t                  next_ = 0;        // round-robin victim
    uint64_t                     hits_ = 0;
    uint64_t                     computations_ = 0;
};

} // namespace rune::client::session
