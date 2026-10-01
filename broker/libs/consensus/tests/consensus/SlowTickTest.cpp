// SlowTickTest.cpp — Aeron's slow-tick gate, on an injected clock (no time passes).
#include <rune/consensus/SlowTick.h>

#include <gtest/gtest.h>

using rune::consensus::SlowTick;

TEST(SlowTickRegression, DueOnTheFirstCycleThenEveryIntervalWhileIdle) {
    SlowTick t;
    t.intervalNs = 10'000'000;
    EXPECT_TRUE(t.due(0)) << "the first cycle always runs the slow work";
    t.done(0, /*work=*/0);
    EXPECT_FALSE(t.due(1));
    EXPECT_FALSE(t.due(9'999'999));
    EXPECT_TRUE(t.due(10'000'000));
}

TEST(SlowTickRegression, WorkDoneMakesItDueOnTheNextCycle) {
    SlowTick t;
    t.intervalNs = 10'000'000;
    t.done(5'000, /*work=*/3);
    EXPECT_TRUE(t.due(5'001)) << "convergence never waits out an interval";
    t.done(5'001, /*work=*/0);
    EXPECT_FALSE(t.due(5'002));
    EXPECT_TRUE(t.due(10'005'001));
}

TEST(SlowTickRegression, ZeroIntervalIsEveryCycle) {
    SlowTick t;                                   // thread placement: ungated
    for (int64_t now = 100; now < 110; ++now) {
        ASSERT_TRUE(t.due(now));
        t.done(now, 0);
    }
}
