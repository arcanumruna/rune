// LeaseRouteHintTest.cpp - L22 fix 2: a partition-routed control op (lease keepalive, view read) starts on the node that last served its
// partition, not on the seed that would Redirect it (two round trips per op).  THREE real brokers (LeaseTrio.h), a real client seeded on a
// follower of the catalog; every wait is bounded.
#include "support/DdlHarness.h"   // charBlob / viewPlanBlob
#include "support/LeaseTrio.h"

#include <gtest/gtest.h>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
using Clk = std::chrono::steady_clock;

namespace {
int64_t msSince(Clk::time_point t0) { return std::chrono::duration_cast<std::chrono::milliseconds>(Clk::now() - t0).count(); }

/// Defines one-partition lease topics until one is led by a node that is not the client's seed (bounded: 6 topics).
std::optional<LeaseTrio::Topic> nonSeedLedLease(LeaseTrio& t) {
    for (int i = 0; i < 6; ++i) {
        const auto topic = t.defineLease("claims" + std::to_string(i), 1, 30'000, 1000, 600'000);
        const int32_t l = t.leaderOf(topic, 0);
        if (l > 0 && l != t.seedNode()) return topic;
    }
    return std::nullopt;
}
}  // namespace

TEST(LeaseRouteHintRegression, KeepAliveGoesStraightToLeaderRegression) {
    LeaseTrio t("lease-hint-ka");
    const auto topic = nonSeedLedLease(t);
    ASSERT_TRUE(topic.has_value()) << "a lease topic led by a node other than the seed";
    auto lt = t.open(*topic);
    rc::LeaseOptions o;
    o.ttlMs = 30'000;
    const rc::LeaseResult g = lt.grantAndWait(lt3::row("hint", 1), o);
    ASSERT_TRUE(g.ok());
    auto& c = t.client();
    const uint64_t red0 = c.ctlRedirectsFollowed(), hit0 = c.routeHintHits();
    for (int i = 0; i < 20; ++i) ASSERT_TRUE(lt.keepAliveOnceAndWait(lt3::row("hint"), g.version).ok()) << i;
    EXPECT_LE(c.ctlRedirectsFollowed() - red0, 1u) << "only the first keepalive is redirected";
    EXPECT_GE(c.routeHintHits() - hit0, 19u) << "every later keepalive starts on the remembered leader";
    EXPECT_GE(c.routeHintLearned(), 1u);
}

TEST(LeaseRouteHintRegression, HintClearedOnLeaderChangeRegression) {
    LeaseTrio t("lease-hint-lc");
    const auto topic = nonSeedLedLease(t);
    ASSERT_TRUE(topic.has_value()) << "a lease topic led by a node other than the seed";
    auto lt = t.open(*topic);
    rc::LeaseOptions o;
    o.ttlMs = 30'000;
    const rc::LeaseResult g = lt.grantAndWait(lt3::row("hint", 1), o);
    ASSERT_TRUE(g.ok());
    auto& c = t.client();
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(lt.keepAliveOnceAndWait(lt3::row("hint"), g.version).ok()) << i;
    ASSERT_GE(c.routeHintLearned(), 1u);
    const uint64_t cleared0 = c.routeHintCleared();
    (void)t.killLeader(*topic, 0);
    const int32_t newLeader = t.leaderOf(*topic, 0);
    ASSERT_GT(newLeader, 0);
    const auto t0 = Clk::now();
    bool ok = false;
    rc::LeaseOptions ko;
    ko.opTimeoutMs = 300;
    while (!ok && msSince(t0) < 1500) {
        try { ok = lt.keepAliveOnceAndWait(lt3::row("hint"), g.version, ko).ok(); } catch (const std::exception&) {}
        if (!ok) std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(ok) << "keepalives succeed again within 1.5 s of the new leader";
    EXPECT_GT(c.routeHintCleared(), cleared0) << "the hint naming the dead leader was dropped";
    if (newLeader != t.seedNode()) {   // a seed-led partition is served without a Redirect: nothing to remember
        const uint64_t hit0 = c.routeHintHits();
        for (int i = 0; i < 20; ++i) ASSERT_TRUE(lt.keepAliveOnceAndWait(lt3::row("hint"), g.version).ok()) << i;
        EXPECT_GE(c.routeHintHits() - hit0, 15u) << "the hint now names the new leader";
    }
}

TEST(LeaseRouteHintRegression, ViewReadGoesStraightToLeaderRegression) {
    LeaseTrio t("lease-hint-view");
    auto& c = t.client();
    rc::TopicOptions to;
    to.replicationFactor = 3;
    int32_t viewTopicId = -1;
    for (int i = 0; i < 6 && viewTopicId < 0; ++i) {
        const std::string name = "src" + std::to_string(i);
        const auto src = c.defineTopicAndWait(name, 1, "id", rune::node::test::charBlob(), rc::Persistence::Disk, to, {}, 90s);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        const auto view = c.createViewAndWait("v" + std::to_string(i), "id", 0, rune::node::test::viewPlanBlob("v" + std::to_string(i), src.topicId, 0), {}, 60s);
        ASSERT_EQ(view.errorCode, 0) << view.detail;
        const LeaseTrio::Topic st{src.topicId, 1, name};
        const int32_t l = t.leaderOf(st, 0);
        if (l > 0 && l != t.seedNode()) viewTopicId = view.viewTopicId;
    }
    ASSERT_GE(viewTopicId, 0) << "a view whose source partition is led by a node other than the seed";
    // The first read may meet a view that is still being created ("not ready"): bounded retries until it is served at all.
    auto read = [&] { return c.getViewRowAndWait(viewTopicId, 101, 0, 10s); };
    rc::ViewRowResult r0;
    for (int i = 0; i < 100; ++i) {
        try { r0 = read(); } catch (const std::exception&) {}
        if (r0.status != rc::ViewRowResult::kNotLeader && c.routeHintLearned() >= 1) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_GE(c.routeHintLearned(), 1u) << "the first read followed a Redirect to the leader";
    const uint64_t red0 = c.ctlRedirectsFollowed(), hit0 = c.routeHintHits();
    for (int i = 0; i < 20; ++i) (void)read();
    EXPECT_EQ(c.ctlRedirectsFollowed() - red0, 0u) << "no further Redirect";
    EXPECT_GE(c.routeHintHits() - hit0, 20u) << "every later read starts on the remembered leader";
}
