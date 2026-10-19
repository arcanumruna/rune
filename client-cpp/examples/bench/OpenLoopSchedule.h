// examples/bench/OpenLoopSchedule.h — fixed-rate open-loop send schedule.  Record i
// is due at startNs + floor(i * 1e9 / rate): exact integer math, no drift.
// Latency is measured from scheduledNs(i) (coordinated-omission-safe, spec §5).
#pragma once

#include <cstdint>

namespace rune::bench {

class OpenLoopSchedule {
public:
    OpenLoopSchedule(int64_t startNs, uint64_t ratePerSec) noexcept
        : startNs_(startNs), rate_(ratePerSec == 0 ? 1 : ratePerSec) {}

    [[nodiscard]] int64_t scheduledNs(uint64_t i) const noexcept {
        return startNs_ + static_cast<int64_t>((static_cast<unsigned __int128>(i) * kNsPerSec) / rate_);
    }

    /// Number of records with scheduledNs(i) <= nowNs, i.e. ceil((elapsed + 1) * rate / 1e9).
    [[nodiscard]] uint64_t dueCount(int64_t nowNs) const noexcept {
        if (nowNs < startNs_) return 0;
        const unsigned __int128 elapsed = static_cast<uint64_t>(nowNs - startNs_);
        return static_cast<uint64_t>(((elapsed + 1) * rate_ + (kNsPerSec - 1)) / kNsPerSec);
    }

    [[nodiscard]] int64_t  startNs() const noexcept { return startNs_; }
    [[nodiscard]] uint64_t rate() const noexcept { return rate_; }

private:
    static constexpr uint64_t kNsPerSec = 1'000'000'000ULL;
    int64_t  startNs_;
    uint64_t rate_;
};

} // namespace rune::bench
