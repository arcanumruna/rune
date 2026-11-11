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
    const lt3::RouteCounters r0 = lt3::settledRouteCounters(c);
    for (int i = 0; i < 20; ++i) ASSERT_TRUE(lt.keepAliveOnceAndWait(lt3::row("hint"), g.version).ok()) << i;
    const lt3::RouteCounters r1 = lt3::settledRouteCounters(c);
    EXPECT_LE(r1.redirects - r0.redirects, 1u) << "only the first keepalive is redirected";
    // A produce Redirected before the client learnt the leader's address moves the seed itself to the leader (redirectBySeed_): then
    // every keepalive is served where it lands, with no Redirect and so nothing to remember.  Otherwise the first one is Redirected
    // and remembered, and every later one starts on the remembered leader.
    if (r1.redirects != r0.redirects) {
        EXPECT_GE(r1.hits - r0.hits, 19u) << "every later keepalive starts on the remembered leader";
        EXPECT_GE(r1.learned - r0.learned, 1u);
    }
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
    const uint64_t cleared0 = lt3::settledRouteCounters(c).cleared;
    ASSERT_GE(c.routeHintLearned(), 1u);
    (void)t.killLeader(*topic, 0);
    const int32_t newLeader = t.leaderOf(*topic, 0);
    ASSERT_GT(newLeader, 0);
    const auto t0 = Clk::now();
    bool ok = false;
    rc::LeaseOptions ko;
    ko.opTimeoutMs = 300;
    const int64_t budgetMs = lt3::kSanitized ? 15'000 : 1500;   // the 1.5 s budget in a plain build; under a sanitizer only "they recover"
    while (!ok && msSince(t0) < budgetMs) {
        try { ok = lt.keepAliveOnceAndWait(lt3::row("hint"), g.version, ko).ok(); } catch (const std::exception&) {}
        if (!ok) std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(ok) << "keepalives succeed again within 1.5 s of the new leader";
    EXPECT_GT(lt3::settledRouteCounters(c).cleared, cleared0) << "the hint naming the dead leader was dropped";
    if (newLeader != t.seedNode()) {   // a seed-led partition is served without a Redirect: nothing to remember
        const uint64_t hit0 = lt3::settledRouteCounters(c).hits;
        for (int i = 0; i < 20; ++i) ASSERT_TRUE(lt.keepAliveOnceAndWait(lt3::row("hint"), g.version).ok()) << i;
        EXPECT_GE(lt3::settledRouteCounters(c).hits - hit0, 15u) << "the hint now names the new leader";
    }
}

TEST(LeaseRouteHintRegression, ViewReadGoesStraightToLeaderRegression) {
    LeaseTrio t("lease-hint-view");
    auto& c = t.client();
    rc::TopicOptions to;
    to.replicationFactor = 3;
    int32_t viewTopicId = -1;
    LeaseTrio::Topic st{};
    for (int i = 0; i < 6 && viewTopicId < 0; ++i) {
        const std::string name = "src" + std::to_string(i);
        const auto src = c.defineTopicAndWait(name, 1, "id", rune::node::test::charBlob(), rc::Persistence::Disk, to, {}, 90s);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        const auto view = c.createViewAndWait("v" + std::to_string(i), "id", 0, rune::node::test::viewPlanBlob("v" + std::to_string(i), src.topicId, 0), {}, 60s);
        ASSERT_EQ(view.errorCode, 0) << view.detail;
        st = LeaseTrio::Topic{src.topicId, 1, name};
        const int32_t l = t.leaderOf(st, 0);
        if (l > 0 && l != t.seedNode()) viewTopicId = view.viewTopicId;
    }
    ASSERT_GE(viewTopicId, 0) << "a view whose source partition is led by a node other than the seed";
    auto read = [&] { return c.getViewRowAndWait(viewTopicId, 101, 0, 10s); };
    // Warm-up: reads until one is served through the remembered leader (a hit, no Redirect).  The first ones may meet a view that is
    // still being created, and a hint learnt then may be dropped again while the young cluster still moves leaders (bounded).
    bool warm = false;
    for (int i = 0; i < 100 && !warm; ++i) {
        const lt3::RouteCounters before = lt3::settledRouteCounters(c);
        rc::ViewRowResult r;
        try { r = read(); } catch (const std::exception&) {}
        const lt3::RouteCounters after = lt3::settledRouteCounters(c);
        warm = r.status != rc::ViewRowResult::kNotLeader && after.hits == before.hits + 1 && after.redirects == before.redirects;
        if (!warm) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(warm) << "a read served through the remembered leader";
    // The measured window: 20 reads, none Redirected.  A window across which the partition's leader moved (a controller change in the
    // just-booted cluster re-elects) proves nothing about the hint: it is measured again, at most 3 times.
    std::string trail;   // per read: the counters after it, so a failure names the read that missed the hint and why
    lt3::RouteCounters r1, r2;
    bool measured = false;
    for (int w = 0; w < 3 && !measured; ++w) {
        const int32_t leader = t.leaderOf(st, 0);
        r1 = lt3::settledRouteCounters(c);
        lt3::RouteCounters prev = r1;
        trail.clear();
        for (int i = 0; i < 20; ++i) {
            const rc::ViewRowResult r = read();
            const lt3::RouteCounters now = lt3::settledRouteCounters(c);
            if (now.hits - prev.hits != 1 || now.redirects != prev.redirects)
                trail += " read" + std::to_string(i) + "{status " + std::to_string(int(r.status)) + " hits+" + std::to_string(now.hits - prev.hits) +
                         " red+" + std::to_string(now.redirects - prev.redirects) + " learned+" + std::to_string(now.learned - prev.learned) +
                         " cleared+" + std::to_string(now.cleared - prev.cleared) + "}";
            prev = now;
        }
        r2 = prev;
        measured = r2.redirects == r1.redirects || t.leaderOf(st, 0) == leader;
    }
    ASSERT_TRUE(measured) << "the partition's leader moved in every window";
    EXPECT_EQ(r2.redirects - r1.redirects, 0u) << "no further Redirect;" << trail;
    EXPECT_GE(r2.hits - r1.hits, 20u) << "every later read starts on the remembered leader;" << trail;
}
