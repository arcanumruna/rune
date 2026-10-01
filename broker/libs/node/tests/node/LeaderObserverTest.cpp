// LeaderObserverTest.cpp — F9a: the control thread's leadership view reaches the
// observer thread over a ring (no atomics).  Change-only publishing; a full ring
// (observer not draining) never loses the LATEST view — the next control cycle
// re-publishes it.
#include <rune/node/LeaderObserver.h>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using rune::node::LeaderObserver;
using rune::node::LeaderSnapshot;

TEST(LeaderObserver, UnboundObserverReportsNoLeader) {
    LeaderObserver o{3};
    EXPECT_EQ(o.computeOnControl(), LeaderSnapshot{});
    EXPECT_EQ(o.publishOnControl(), 0);
    EXPECT_EQ(o.observe().leaderId, -1);
    EXPECT_FALSE(o.observe().isLeader);
}

TEST(LeaderObserver, ObserverSeesLatestPublishedSnapshot) {
    LeaderObserver o{1};
    o.bindObserver();
    o.publish({/*leaderId=*/2, /*term=*/5, /*isLeader=*/false});
    o.publish({/*leaderId=*/1, /*term=*/6, /*isLeader=*/true});
    const LeaderSnapshot& v = o.observe();
    EXPECT_EQ(v.leaderId, 1);
    EXPECT_EQ(v.term, 6);
    EXPECT_TRUE(v.isLeader);
}

// Ring full because nobody observes: the latest view must still arrive once
// the observer drains and the control thread runs its next cycle.
TEST(LeaderObserverRegression, FullRingConvergesOnLatestView) {
    LeaderObserver o{1};
    o.bindObserver();
    LeaderSnapshot last{};
    for (int i = 0; i < 10'000; ++i) {          // >> ring capacity (128 snapshots)
        last = {static_cast<rune::consensus::NodeId>(i % 3), i, (i % 3) == 1};
        o.publish(last);
    }
    (void)o.observe();                          // drains stale snapshots
    o.publish(last);                            // next control cycle: re-publish
    const LeaderSnapshot& v = o.observe();
    EXPECT_EQ(v, last);
}

// Cross-thread: one control thread publishes a monotonically increasing term;
// the observer never sees a torn {leaderId, term} pair and ends on the final.
TEST(LeaderObserverRegression, CrossThreadSnapshotsAreNeverTorn) {
    LeaderObserver o{0};
    o.bindObserver();
    constexpr int kN = 200'000;
    std::thread control([&o] {
        for (int t = 1; t <= kN; ++t) {
            // leaderId is derived from the term so a torn pair is detectable.
            o.publish({static_cast<rune::consensus::NodeId>(t % 7), t, false});
        }
        // Keep "cycling" until the final view is published (ring may be full).
        for (int i = 0; i < 1000; ++i) {
            o.publish({static_cast<rune::consensus::NodeId>(kN % 7), kN, false});
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
    });
    rune::consensus::Term prev = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (prev < kN && std::chrono::steady_clock::now() < deadline) {
        const LeaderSnapshot v = o.observe();
        if (v.term != 0) {
            ASSERT_EQ(v.leaderId, static_cast<rune::consensus::NodeId>(v.term % 7)) << "torn snapshot";
            ASSERT_GE(v.term, prev) << "snapshots must arrive in publish order";
            prev = v.term;
        }
    }
    control.join();
    EXPECT_EQ(o.observe().term, kN);
}

// Fix round 1: single reader BY CONSTRUCTION.  The bound observer thread drains;
// any other thread is refused (critical log + abort in debug, "no leader"
// snapshot in release) — it can never become a second ring consumer.
TEST(LeaderObserverRegression, SecondThreadObserverIsRefused) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    LeaderObserver o{1};
    o.bindObserver();                                  // this thread observes
    o.publish({/*leaderId=*/1, /*term=*/4, /*isLeader=*/true});
#ifndef NDEBUG
    EXPECT_DEATH({
        std::thread t([&o] { (void)o.observe(); });
        t.join();
    }, "");
#else
    LeaderSnapshot foreign{7, 7, true};
    std::thread t([&o, &foreign] { foreign = o.observe(); });
    t.join();
    EXPECT_EQ(foreign, LeaderSnapshot{}) << "a foreign observer gets nothing";
#endif
    EXPECT_EQ(o.observe().term, 4) << "the bound observer still drains (ring untouched)";
}
