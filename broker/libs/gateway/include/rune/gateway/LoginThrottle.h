#pragma once
// LoginThrottle.h - the brute-force curve of the SCRAM login (authentication plan D14, spec §5).
//
// Failures are keyed by (user, source host) AND by source host.  The first kFreePairFailures (2) of a pair and the
// first kFreeHostFailures (10) of a host are free; every further failure delays the NEXT attempt by
// min(250 ms * 2^(k-1), 30 s) where k counts the failures beyond the free ones.  There is NO hard lockout: an
// attacker at one address never delays the same user at another address (the pair key includes the host, the host
// key is the attacker's own).  An attempt inside the delay is answered AuthenticationFailed with retryAfterMs and is
// not counted (the gateway does not call onFailure for it).  A success resets the PAIR only (a host sweep must not
// be reset by logging in with the attacker's own account).  An entry idle kForgetAfterNs is forgotten.
//
// Cold control-thread object (hard rules 1, 4): owned by the gateway on the control thread, two fixed tables
// allocated once at construction (4096 pair + 4096 host entries, bounded-probe open addressing with eviction of the
// OLDEST entry of the probe window when it is full) - check / onFailure / onSuccess never allocate and never refuse.
// An entry is occupied iff it holds at least one failure (an empty entry is the zero state).  Keys are stored as a
// 128-bit hash (two independent 64-bit FNV variants): a collision between two different keys needs ~2^64 work.

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

    static constexpr uint32_t kFreePairFailures = 2, kFreeHostFailures = 10;
    static constexpr int64_t  kBaseDelayNs = 250'000'000, kMaxDelayNs = 30'000'000'000, kForgetAfterNs = 900'000'000'000;
    static constexpr std::size_t kCapacity = 4096;   ///< entries per table (pair, host); a power of two
    static constexpr std::size_t kProbe    = 8;      ///< slots examined per key

    LoginThrottle() : pairs_(kCapacity), hosts_(kCapacity) {}

    [[nodiscard]] Verdict check(std::string_view user, std::string_view host, int64_t nowNs) const noexcept;
    void onFailure(std::string_view user, std::string_view host, int64_t nowNs) noexcept;
    void onSuccess(std::string_view user, std::string_view host) noexcept;

private:
    struct Entry {
        uint64_t h1 = 0, h2 = 0;
        uint32_t failures = 0;          // 0 = empty
        int64_t  lastFailNs = 0;
        int64_t  nextAllowedNs = 0;
    };
    struct Key { uint64_t h1, h2; };
    [[nodiscard]] static Key pairKey(std::string_view user, std::string_view host) noexcept;
    [[nodiscard]] static Key hostKey(std::string_view host) noexcept;
    [[nodiscard]] static const Entry* find_(const std::vector<Entry>&, Key, int64_t nowNs) noexcept;
    [[nodiscard]] static Entry& claim_(std::vector<Entry>&, Key, int64_t nowNs) noexcept;
    static void fail_(Entry&, uint32_t freeFailures, int64_t nowNs) noexcept;

    std::vector<Entry> pairs_, hosts_;
};

} // namespace rune::gateway
