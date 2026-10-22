#pragma once
// LeaseClock.h — issue #82 (VLS Part A, A.3 C2): the clock every view-read-lease bound is measured on.
//
// A lease clock must count time the machine spends SUSPENDED: a node that slept and woke believing less time passed
// would serve under a lease that ran out while it slept.  The duty clock (MonotonicNanoClock, CLOCK_MONOTONIC_RAW)
// stops during suspend on Linux, so it is never used for a lease:
//   Linux  — CLOCK_BOOTTIME (vDSO since 5.3; Rune requires 5.6+): CLOCK_MONOTONIC plus the time suspended.
//   macOS  — clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW): mach_continuous_time, which keeps counting while asleep.
// A VM pause is covered as long as the guest's boot-time clock catches up after the pause (kvmclock does); that is an
// operational requirement (docs/configuration.md).
//
// No two nodes' lease clocks are ever compared (C3): every bound is a duration measured on one node.  Bounded drift (C1):
// each clock advances at real rate within [1/(1+rho), 1+rho], rho = 1 % (kLeaseDriftBudget in ReadLease.h).
//
// One clock read per call (~20 ns, a vDSO call); no state, no allocation.
#include <cstdint>
#include <time.h>

namespace rune::consensus {

[[nodiscard]] inline int64_t leaseClockNowNs() noexcept {
#if defined(__APPLE__)
    return static_cast<int64_t>(::clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW));
#else
    struct timespec ts{};
    ::clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * INT64_C(1'000'000'000) + static_cast<int64_t>(ts.tv_nsec);
#endif
}

/// An injected lease clock (tests): a plain function pointer + context - no virtual call, no std::function.
using LeaseClockFn = int64_t (*)(void* ctx) noexcept;

} // namespace rune::consensus
