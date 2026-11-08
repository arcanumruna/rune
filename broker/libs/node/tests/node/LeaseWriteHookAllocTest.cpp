// LeaseWriteHookAllocTest.cpp - lease topics Task 5a (hard rule): the lease branch of the write hook (check + grant stamping)
// allocates nothing in steady state.  Lives in the alloc-regression binary: AllocWindow.cpp replaces the global operator new.
#include "support/AllocWindow.h"
#include "support/LeaseHookRig.h"

#include <gtest/gtest.h>

using namespace rune::node::leasehook;

TEST(LeaseWriteHookAllocRegression, HookNoAllocRegression) {
    Rig r;
    r.preload(7);
    std::vector<LRec> recs;
    for (int i = 0; i < 8; ++i) recs.push_back(grantOf(uint64_t(i + 1), i % 2 == 0 ? 0 : 5000));
    const auto batch = batchOf(recs);
    const auto updates = batchOf({updateOf(1, 4), revokeOf(2, 5)});
    const auto runOnce = [&](const std::vector<uint8_t>& b, int32_t n) {
        rune::node::CheckedWriteHook hook(r.state, r.facts, r.scratch);
        if (hook.check(b.data(), b.size(), n) != ErrorCode::OK) return false;
        return hook.stage(r.tlog, b.data(), b.size()).error == ErrorCode::OK;
    };
    for (int i = 0; i < 100; ++i) ASSERT_TRUE(runOnce(batch, 8) && runOnce(updates, 2));   // warm-up
    uint64_t failures = 0;
    rune::testing::AllocWindow window;
    for (int i = 0; i < 10'000; ++i) {
        failures += runOnce(batch, 8) ? 0 : 1;
        failures += runOnce(updates, 2) ? 0 : 1;
    }
    EXPECT_EQ(window.count(), 0u);
    EXPECT_EQ(failures, 0u);
}
