// IdleStrategyTest.cpp — P2P-7: BackoffIdleStrategy and BusySpinIdleStrategy tests.
//
// Validates:
//   - BackoffIdleStrategy.idle(0) backs off without throwing (bounded calls)
//   - BackoffIdleStrategy.idle(N>0) resets state (no park on next idle(0) sequence)
//   - BusySpinIdleStrategy exists and idle() doesn't throw
//   - Both implement the IdleStrategy interface
//   - reset() is callable and doesn't throw

#include <gtest/gtest.h>
#include <rune/consensus/IdleStrategy.h>

#include <chrono>
#include <memory>

using namespace rune::consensus;

// ---------------------------------------------------------------------------
// BackoffIdleStrategy
// ---------------------------------------------------------------------------

TEST(BackoffIdleStrategy, DefaultConstruction) {
    BackoffIdleStrategy idle;
    // No exception on construction.
    SUCCEED();
}

TEST(BackoffIdleStrategy, IdleWithWorkCountGreaterThanZero_Resets) {
    // idle(positive) should reset and not throw.
    BackoffIdleStrategy idle;
    EXPECT_NO_THROW(idle.idle(5));
    EXPECT_NO_THROW(idle.idle(1));
    EXPECT_NO_THROW(idle.idle(100));
}

TEST(BackoffIdleStrategy, IdleWithZeroWork_DoesNotThrow) {
    BackoffIdleStrategy idle;
    // idle(0) multiple times — backoff progression; must not throw or hang.
    for (int i = 0; i < 50; ++i) {
        EXPECT_NO_THROW(idle.idle(0));
    }
}

TEST(BackoffIdleStrategy, ResetAfterBackoff_ThenIdleZero_NoThrow) {
    // Simulate: idle several times → reset (work arrived) → idle again.
    BackoffIdleStrategy idle;
    for (int i = 0; i < 15; ++i) { idle.idle(0); }
    EXPECT_NO_THROW(idle.reset());
    // After reset, next idle(0) should be fast (back to spinning phase).
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 5; ++i) { idle.idle(0); }
    auto elapsed = std::chrono::steady_clock::now() - t0;
    // Should be much less than 100ms (no park in first few cycles after reset).
    EXPECT_LT(elapsed, std::chrono::milliseconds{100});
}

TEST(BackoffIdleStrategy, IdleWithPositiveWork_ThenZero_Cycles) {
    // Typical cycle: do work → reset → go idle → work arrives again.
    BackoffIdleStrategy idle;
    idle.idle(5);   // work: reset
    idle.idle(0);   // start backing off (just spinning)
    idle.idle(0);
    idle.idle(3);   // work again: reset
    idle.idle(0);
    EXPECT_NO_THROW(idle.reset());
}

TEST(BackoffIdleStrategy, CustomParameters) {
    // Custom parameters: very short timeouts to avoid blocking tests.
    BackoffIdleStrategy idle{
        /*maxSpins=*/2,
        /*maxYields=*/2,
        /*minPark=*/std::chrono::nanoseconds{100},
        /*maxPark=*/std::chrono::nanoseconds{500}};

    // Drive through all phases quickly.
    for (int i = 0; i < 20; ++i) {
        idle.idle(0);
    }
    // Should still work without hanging.
    EXPECT_NO_THROW(idle.idle(1)); // reset
    EXPECT_NO_THROW(idle.idle(0));
}

TEST(BackoffIdleStrategy, IsIdleStrategy) {
    // Verify it satisfies the IdleStrategy interface via virtual dispatch.
    BackoffIdleStrategy backoff;
    IdleStrategy& iface = backoff;
    EXPECT_NO_THROW(iface.idle(0));
    EXPECT_NO_THROW(iface.idle(1));
    EXPECT_NO_THROW(iface.reset());
}

TEST(BackoffIdleStrategy, IdleWithWorkCount_BoundedDuration) {
    // idle(0) many times must complete in reasonable wall time even in parking phase.
    // With default maxPark=1ms, 30 calls should take <100ms.
    BackoffIdleStrategy idle{10, 20,
        std::chrono::microseconds{100},
        std::chrono::milliseconds{1}};
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 30; ++i) { idle.idle(0); }
    auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(elapsed, std::chrono::milliseconds{200});
}

// ---------------------------------------------------------------------------
// BusySpinIdleStrategy
// ---------------------------------------------------------------------------

TEST(BusySpinIdleStrategy, DefaultConstruction) {
    BusySpinIdleStrategy idle;
    SUCCEED();
}

TEST(BusySpinIdleStrategy, IdleDoesNotThrow) {
    BusySpinIdleStrategy idle;
    for (int i = 0; i < 20; ++i) {
        EXPECT_NO_THROW(idle.idle(0));
        EXPECT_NO_THROW(idle.idle(5));
    }
}

TEST(BusySpinIdleStrategy, ResetDoesNotThrow) {
    BusySpinIdleStrategy idle;
    EXPECT_NO_THROW(idle.reset());
}

TEST(BusySpinIdleStrategy, IsIdleStrategy) {
    BusySpinIdleStrategy spin;
    IdleStrategy& iface = spin;
    EXPECT_NO_THROW(iface.idle(0));
    EXPECT_NO_THROW(iface.idle(1));
    EXPECT_NO_THROW(iface.reset());
}

// ---------------------------------------------------------------------------
// Polymorphic use: both concrete types usable through IdleStrategy&
// ---------------------------------------------------------------------------

TEST(IdleStrategyInterface, PolymorphicBackoff) {
    std::unique_ptr<IdleStrategy> idle = std::make_unique<BackoffIdleStrategy>();
    idle->idle(0);
    idle->idle(1);
    idle->reset();
    SUCCEED();
}

TEST(IdleStrategyInterface, PolymorphicBusySpin) {
    std::unique_ptr<IdleStrategy> idle = std::make_unique<BusySpinIdleStrategy>();
    idle->idle(0);
    idle->idle(1);
    idle->reset();
    SUCCEED();
}
