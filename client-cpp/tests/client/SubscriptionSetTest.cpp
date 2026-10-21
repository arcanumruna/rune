// tests/client/SubscriptionSetTest.cpp — Phase 2c Task 12: subscribe(topic, AllPartitions{}, ...) and
// SubscriptionSet.  A real Client in Invoker mode over the InvokerGateway fake; single-threaded, every wait a
// bounded pump loop.
#include "../support/GroupRig.h"

#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <stdexcept>
#include <vector>

namespace {
using E = rune::gen::PlayerEvent;

void noopRecords(const rc::Record&) {}

constexpr int32_t kParts = 4;
// The rig's topic "t" is declared with 2 partitions; the fake broker accepts any partition number.
rc::TopicHandle fourPartitions(rc::Client& c) {
    rc::TopicHandle t = c.topic("t");
    t.partitionCount  = kParts;
    return t;
}

struct SetRig : GroupRig {
    explicit SetRig(int32_t in) : GroupRig(in, in + 1) {}
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
};
} // namespace

TEST(SubscriptionSet, SubscribesEveryPartitionWithTheFilter) {
    SetRig r(4400);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, opts, &noopRecords);
    EXPECT_EQ(set.size(), static_cast<std::size_t>(kParts));
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    const auto want = E::Filter{.worldId = 7u}.encode();
    std::set<int32_t> parts;
    for (const auto& s : r.gw.subscribed) {
        parts.insert(s.partition);
        EXPECT_EQ(s.partitionKnown, 1);
        EXPECT_EQ(s.filter, std::vector<uint8_t>(want.span().begin(), want.span().end()));
    }
    EXPECT_EQ(parts, (std::set<int32_t>{0, 1, 2, 3}));
}

TEST(SubscriptionSet, HandlerSeesEachRecordsPartition) {
    SetRig r(4402);
    r.connect();
    std::vector<int32_t> seen;
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {},
                                  [&](const rc::Record& rec) { seen.push_back(rec.partition); });
    ASSERT_TRUE(r.drive([&] { return set.active() && r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    for (const auto& s : r.gw.subscribed) r.gw.pushRecords(s, 0, 1);
    ASSERT_TRUE(r.drive([&] { return seen.size() == static_cast<std::size_t>(kParts); }));
    std::sort(seen.begin(), seen.end());
    EXPECT_EQ(seen, (std::vector<int32_t>{0, 1, 2, 3}));
    for (int32_t p = 0; p < kParts; ++p) EXPECT_EQ(set.position(p), 1);
    EXPECT_GT(set.batchBytes(), 0u);
}

TEST(SubscriptionSet, UnsubscribeEndsEveryPartition) {
    SetRig r(4404);
    r.connect();
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    EXPECT_TRUE(set.active());
    EXPECT_FALSE(set.finished());
    set.unsubscribe();
    ASSERT_TRUE(r.drive([&] { return set.finished(); }));
    EXPECT_FALSE(set.active());
}

TEST(SubscriptionSetRegression, OnePartitionRefusedFinishesOnlyThatOneRegression) {
    SetRig r(4406);
    r.gw.subscribeErrorCode      = 39;   // InvalidRequest
    r.gw.subscribeErrorPartition = 2;
    r.connect();
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return set[2].finished(); }));
    EXPECT_EQ(set[2].failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    ASSERT_TRUE(r.drive([&] { return set[0].subscriptionId() != 0 && set[1].subscriptionId() != 0 &&
                                     set[3].subscriptionId() != 0; }));
    EXPECT_TRUE(set[0].active());
    EXPECT_TRUE(set[1].active());
    EXPECT_TRUE(set[3].active());
    EXPECT_TRUE(set.active()) << "any";
    EXPECT_FALSE(set.finished()) << "all";
}

TEST(SubscriptionSetRegression, ZeroPartitionTopicIsRejectedRegression) {
    SetRig r(4408);
    r.connect();
    rc::TopicHandle t = r.client.topic("t");
    t.partitionCount  = 0;
    EXPECT_THROW((void)r.client.subscribe(t, rc::AllPartitions{}, {}, &noopRecords), std::invalid_argument);
    r.passes(50);
    EXPECT_TRUE(r.gw.subscribeFrames.empty()) << "nothing submitted";
}
