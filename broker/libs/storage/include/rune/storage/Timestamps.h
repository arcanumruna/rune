#pragma once
// Record timestamps reach the broker in either unit: rune-client-cpp stamps batch timestamps in epoch
// NANOSECONDS, Kafka-style producers in epoch MILLISECONDS, and the broker never restamps them.  Every
// time-based decision (producer-tail expiry, time retention) compares in epoch ms via toEpochMs.
// A value above 1e15 cannot be epoch ms for 30+ millennia, and epoch ns passed 1e15 on 1970-01-12,
// so the unit is unambiguous for any real clock.
#include <cstdint>

namespace rune::storage {

inline constexpr int64_t kEpochNsThreshold = 1'000'000'000'000'000LL;   // 1e15

[[nodiscard]] constexpr int64_t toEpochMs(int64_t ts) noexcept {
    return ts > kEpochNsThreshold ? ts / 1'000'000 : ts;
}

} // namespace rune::storage
