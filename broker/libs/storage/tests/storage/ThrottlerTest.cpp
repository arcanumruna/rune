// ThrottlerTest.cpp — dedicated unit tests for rune::storage::Throttler (CP-8).
//
// Previously covered only indirectly via CompactionCleanerAgentTest.  These
// tests isolate the token-bucket behaviour: the unlimited fast path, the getter,
// and that a burst exceeding the configured rate actually sleeps.  Timing
// assertions are one-directional (lower-bound only) to stay robust under load.

#include <gtest/gtest.h>
#include <rune/storage/Throttler.h>

#include <chrono>

using rune::storage::Throttler;
using clock_type = std::chrono::steady_clock;

namespace {

int64_t elapsedMs(clock_type::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               clock_type::now() - start).count();
}

} // namespace

TEST(Throttler, UnlimitedNeverSleeps) {
    Throttler t(-1);
    auto start = clock_type::now();
    for (int i = 0; i < 100000; ++i) t.maybeThrottle(1 << 20); // 100k * 1 MiB
    EXPECT_LT(elapsedMs(start), 100) << "unlimited (-1) must be a no-op fast path";
    EXPECT_EQ(t.maxBytesPerSec(), -1);
}

TEST(Throttler, ZeroIsTreatedAsUnlimited) {
    Throttler t(0);
    auto start = clock_type::now();
    for (int i = 0; i < 100000; ++i) t.maybeThrottle(1 << 20);
    EXPECT_LT(elapsedMs(start), 100) << "maxBytesPerSec <= 0 must not throttle";
    EXPECT_EQ(t.maxBytesPerSec(), 0);
}

TEST(Throttler, GetterReturnsConfiguredRate) {
    EXPECT_EQ(Throttler(12345).maxBytesPerSec(), 12345);
}

TEST(Throttler, BurstOverLimitSleeps) {
    // 100 MB/s ceiling; submit 10 MB in one shot → target 0.1 s → sleeps ~100 ms.
    Throttler t(100'000'000);
    auto start = clock_type::now();
    t.maybeThrottle(10'000'000);
    // Lower bound only: sleep_for(100ms) guarantees >= 100ms; assert a generous
    // floor so a loaded scheduler can overshoot without failing.
    EXPECT_GE(elapsedMs(start), 50) << "a burst exceeding the rate must throttle";
}
