#pragma once
// SlowTick.h — Aeron's slow-tick gate (io.aeron.cluster.ConsensusModuleAgent:
// `if (nowNs >= slowTickDeadlineNs) { work = slowTickWork(nowNs);
//   slowTickDeadlineNs = work > 0 ? nowNs + 1 : nowNs + SLOW_TICK_INTERVAL_NS; }`).
//
// Work that need not run every duty cycle (e.g. an O(partitions) scan) runs when
// due.  When it did work it is due again on the NEXT cycle — convergence never
// waits out an interval (time-independent) — otherwise after intervalNs.
// intervalNs == 0: due every cycle (the step behaves as if ungated).
#include <cstdint>
#include <limits>

namespace rune::consensus {

struct SlowTick {
    int64_t intervalNs = 0;
    int64_t deadlineNs = std::numeric_limits<int64_t>::min();   // due on the first cycle

    [[nodiscard]] bool due(int64_t nowNs) const noexcept { return nowNs >= deadlineNs; }
    void done(int64_t nowNs, int work) noexcept { deadlineNs = work > 0 ? nowNs + 1 : nowNs + intervalNs; }
};

} // namespace rune::consensus
