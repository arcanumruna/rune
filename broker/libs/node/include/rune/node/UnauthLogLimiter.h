#pragma once
// UnauthLogLimiter.h - combined fix program M13 (X103): log lines an UNAUTHENTICATED sender can drive (a worker's decrypt
// failure on the produce thread, a require-TLS refusal, a bad bootstrap proof, login failures) are logged the first
// time and then every 1024th occurrence of their kind; every other occurrence only bumps `suppressed()` (NOT yet published as
// counter 1'000'239: that needs the worker-to-control fold and a BrokerNode registration).  Single-writer: owned by the one thread that logs; no atomics, no allocation.
#include <cstddef>
#include <cstdint>

namespace rune::node {

class UnauthLogLimiter {
public:
    static constexpr uint64_t kEvery = 1024;
    static constexpr std::size_t kKinds = 22;   // 21: worker bind token derivation failed (I-C), 16: response egress unavailable (no port), 17: (no request image), 18-20: worker bind (Task 6)
    /// true = log this occurrence of `kind` (< kKinds); false = suppressed (counted).
    [[nodiscard]] bool due(std::size_t kind) noexcept {
        if (seen_[kind % kKinds]++ % kEvery == 0) return true;
        ++suppressed_;
        return false;
    }
    [[nodiscard]] uint64_t suppressed() const noexcept { return suppressed_; }
    [[nodiscard]] uint64_t seen(std::size_t kind) const noexcept { return seen_[kind % kKinds]; }

private:
    uint64_t seen_[kKinds] = {};
    uint64_t suppressed_ = 0;
};

} // namespace rune::node
