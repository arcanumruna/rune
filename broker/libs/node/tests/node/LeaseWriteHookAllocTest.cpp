// LeaseWriteHookAllocTest.cpp - lease topics Task 5a (hard rule): the lease branch of the write hook (check + grant stamping)
// allocates nothing in steady state.  Lives in the alloc-regression binary: AllocWindow.cpp replaces the global operator new.
#include "support/AllocWindow.h"
#include "support/LeaseHookRig.h"

#include <gtest/gtest.h>

using namespace rune::node::leasehook;

TEST(LeaseWriteHookAllocRegression, HookNoAllocRegression) {
    Rig r;
    r.withIndex();
    r.state.judgedThrough = INT64_MAX;
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

// Task 5b: revoke batches (row lookup, re-encode into the scratch, rebuild, lastAppended) and mixed update/revoke batches.
TEST(LeaseWriteHookAllocRegression, RevokeNoAllocRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.state.judgedThrough = INT64_MAX;   // every earlier append is applied
    for (uint64_t id = 1; id <= 8; ++id) r.addRow(id, int64_t(id), 7000, 0, "x", 5);
    std::vector<LRec> revokes, mixed;
    for (uint64_t id = 1; id <= 8; ++id) revokes.push_back(revokeOf(id, int64_t(id)));
    for (uint64_t id = 1; id <= 4; ++id) mixed.push_back(updateOf(id, int64_t(id)));
    for (uint64_t id = 5; id <= 8; ++id) mixed.push_back(revokeOf(id, int64_t(id)));
    mixed.push_back(grantOf(100));
    const auto revokeBatch = batchOf(revokes), mixedBatch = batchOf(mixed);
    const auto runOnce = [&](const std::vector<uint8_t>& b, int32_t n) {
        rune::node::CheckedWriteHook hook(r.state, r.facts, r.scratch);
        if (hook.check(b.data(), b.size(), n) != ErrorCode::OK) return false;
        if (hook.stage(r.tlog, b.data(), b.size()).error != ErrorCode::OK) return false;
        hook.onAppended(0, 1);
        return true;
    };
    for (int i = 0; i < 100; ++i) ASSERT_TRUE(runOnce(revokeBatch, 8) && runOnce(mixedBatch, 9));   // warm-up
    uint64_t failures = 0;
    rune::testing::AllocWindow window;
    for (int i = 0; i < 10'000; ++i) {
        failures += runOnce(revokeBatch, 8) ? 0 : 1;
        failures += runOnce(mixedBatch, 9) ? 0 : 1;
    }
    EXPECT_EQ(window.count(), 0u);
    EXPECT_EQ(failures, 0u);
}
