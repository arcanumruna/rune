#pragma once
// LoginThrottle.h - the brute-force curve of the SCRAM login (authentication plan D14, spec §5).
//
// Failures are keyed by (user, source host), by source host AND by user (issue #131).  The first kFreePairFailures (2)
// of a pair, the first kFreeHostFailures (10) of a host and the first kFreeUserFailures (10) of a user are free; every
// further failure delays the NEXT attempt by min(250 ms * 2^(k-1), cap) where k counts the failures beyond the free
// ones and the cap is 30 s for a pair or a host and kMaxUserDelayNs (2 s) for a user.  There is NO hard lockout: an
// attacker at one address delays the same user at another address by at most the user cap, and not at all at an
// address the user has logged in from (a KNOWN pair - a success within kForgetAfterNs - bypasses the user component).
// The user component exists because the source host is attacker-influenced (a raw-UDP client declares its own reply
// endpoint; an IPv6 /64 is many hosts), so (user, host) alone gives free guesses per address against one user.  An
// attempt inside the delay is answered AuthenticationFailed with retryAfterMs and is not counted (the gateway does not
// call onFailure for it).  A success resets the PAIR only (a host sweep or a user campaign must not be reset by
// logging in with the attacker's own account / the victim's own login) and marks the pair known.  An entry idle
// kForgetAfterNs is forgotten.
//
// Cold control-thread object (hard rules 1, 4): owned by the gateway on the control thread, four fixed tables
// allocated once at construction (4096 entries each: pair, host, user, known pair; bounded-probe open addressing with
// eviction of the OLDEST entry of the probe window when it is full) - check / onFailure / onSuccess never allocate and
// never refuse.  An entry is occupied iff it holds at least one failure (a known-pair entry: a success).  Keys are
// stored as a 128-bit hash: two SipHash-2-4 of the key under two halves of a random per-process 256-bit key (issue
// #131), so an attacker can neither predict a name's slot (grind names into one probe window to evict its own
// entries) nor find a collision.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace rune::gateway {

/// The throttle's source host from an Aeron image's `sourceIdentity()` ("1.2.3.4:5678", "[::1]:5678"); "" for an IPC image
/// ("aeron:ipc...") or an empty identity (the caller then falls back to the client's reply endpoint host).  Returns an
/// OWNED string: `Image::sourceIdentity()` returns a std::string BY VALUE, so a std::string_view bound to it dangles - this
/// takes the view only for the duration of the call (a temporary lives to the end of the full expression) and copies out.
[[nodiscard]] std::string hostOfSourceIdentity(std::string_view identity);

class LoginThrottle {
public:
    struct Verdict { bool allowed = true; uint32_t retryAfterMs = 0; };

    static constexpr uint32_t kFreePairFailures = 2, kFreeHostFailures = 10, kFreeUserFailures = 10;
    static constexpr int64_t  kBaseDelayNs = 250'000'000, kMaxDelayNs = 30'000'000'000, kForgetAfterNs = 900'000'000'000;
    static constexpr int64_t  kMaxUserDelayNs = 2'000'000'000;   ///< the user component's low ceiling (never a lockout)
    static constexpr std::size_t kCapacity = 4096;   ///< entries per table (pair, host, user, known); a power of two
    static constexpr std::size_t kProbe    = 8;      ///< slots examined per key
    using HashKey = std::array<uint8_t, 32>;         ///< two SipHash keys (h1, h2)

    /// A random per-process hash key (RAND_bytes; std::random_device if that refuses - unpredictable either way).
    LoginThrottle();
    /// A fixed hash key (tests).
    explicit LoginThrottle(const HashKey& key) : key_(key), pairs_(kCapacity), hosts_(kCapacity), users_(kCapacity),
                                                 known_(kCapacity) {}

    [[nodiscard]] Verdict check(std::string_view user, std::string_view host, int64_t nowNs) const noexcept;
    void onFailure(std::string_view user, std::string_view host, int64_t nowNs) noexcept;
    void onSuccess(std::string_view user, std::string_view host, int64_t nowNs) noexcept;

    /// Test seam (#131): the home slot of a (user, host) pair key - where its probe window starts.
    [[nodiscard]] std::size_t pairSlotForTest(std::string_view user, std::string_view host) const noexcept {
        return static_cast<std::size_t>(pairKey(user, host).h1) & (kCapacity - 1);
    }

private:
    struct Entry {
        uint64_t h1 = 0, h2 = 0;
        uint32_t failures = 0;          // 0 = empty
        int64_t  lastFailNs = 0;
        int64_t  nextAllowedNs = 0;
    };
    struct Key { uint64_t h1, h2; };
    [[nodiscard]] Key pairKey(std::string_view user, std::string_view host) const noexcept;
    [[nodiscard]] Key hostKey(std::string_view host) const noexcept;
    [[nodiscard]] Key userKey(std::string_view user) const noexcept;
    [[nodiscard]] Key hash_(char tag, std::string_view a, std::string_view b) const noexcept;
    [[nodiscard]] static const Entry* find_(const std::vector<Entry>&, Key, int64_t nowNs) noexcept;
    [[nodiscard]] static Entry& claim_(std::vector<Entry>&, Key, int64_t nowNs) noexcept;
    static void fail_(Entry&, uint32_t freeFailures, int64_t maxDelayNs, int64_t nowNs) noexcept;

    HashKey key_;
    std::vector<Entry> pairs_, hosts_, users_;
    std::vector<Entry> known_;   ///< (user, host) pairs with a success: `lastFailNs` holds the last success time
};

} // namespace rune::gateway
