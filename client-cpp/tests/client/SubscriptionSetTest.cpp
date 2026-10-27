// tests/client/SubscriptionSetTest.cpp — Phase 2c Task 12: subscribe(topic, AllPartitions{}, ...) and
// SubscriptionSet.  A real Client in Invoker mode over the InvokerGateway fake; single-threaded, every wait a
// bounded pump loop.
#include "../support/GroupRig.h"

#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>
#include <rune/protocol/RecordBatch.h>

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

namespace {
std::vector<uint8_t> batchOf(int64_t base, int records) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < records; ++i) b.append("k", std::string(20 + i, 'v'), 1000);
    return b.build();
}
} // namespace

TEST(SubscriptionSet, BatchBytesIsTheExactSumOverThePartitions) {
    SetRig r(4410);
    r.connect();
    std::size_t delivered = 0;
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {}, [&](const rc::Record&) { ++delivered; });
    ASSERT_TRUE(r.drive([&] { return set.active() && r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    uint64_t want = 0;
    std::size_t records = 0;
    for (const auto& s : r.gw.subscribed) {
        const int n = 1 + s.partition;   // a different size per partition
        const auto b = batchOf(0, n);
        want += b.size();
        records += static_cast<std::size_t>(n);
        r.gw.pushBatch(s, 0, b);
    }
    ASSERT_TRUE(r.drive([&] { return delivered == records; }));
    r.passes(20);
    EXPECT_EQ(set.batchBytes(), want);
    uint64_t sum = 0;
    for (int32_t p = 0; p < kParts; ++p) sum += set[p].batchBytes();
    EXPECT_EQ(sum, want);
}

TEST(SubscriptionSetRegression, BatchBytesOfAHandleThatDoesNotReadConvergesRegression) {
    // The status ring is small (about a dozen snapshots): a handle that never reads while many frames arrive makes the
    // duty side refuse publishes (dirty).  Phase 2c Task 17 fix round 1 (I1): the first read after that drained a
    // STALE snapshot (the scenario's bytes undercount).  One read frees the ring, the next duty pass republishes.
    SetRig r(4412);
    r.connect();
    std::size_t delivered = 0;
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {}, [&](const rc::Record&) { ++delivered; });
    ASSERT_TRUE(r.drive([&] { return set.active() && r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    const auto s0 = r.gw.subscribed[0];
    uint64_t want = 0;
    constexpr int kFrames = 200;
    for (int i = 0; i < kFrames; ++i) {
        const auto b = batchOf(i, 1);
        want += b.size();
        r.gw.pushBatch(s0, i, b);
        r.passes(1);            // no read of the handle in between
    }
    ASSERT_TRUE(r.drive([&] { return delivered == static_cast<std::size_t>(kFrames); }));
    (void)set[s0.partition].batchBytes();   // the drain frees the ring ...
    r.passes(20);                           // ... the duty side republishes the dirty snapshot
    EXPECT_EQ(set[s0.partition].batchBytes(), want);
}

// #473: the status reads of an app thread are eventually fresh, with a precise contract.  After a gap without reads
// the ring is full and the duty side refused newer snapshots (dirty): the FIRST read returns the snapshot from when
// the gap began; the dirty value is re-sent every duty cycle and fits once the first read drained the ring, so a
// read after ONE more duty cycle is current.  (Invoker mode: handles take the ring path on the owner thread too,
// so this read is the app-thread one - not the live duty-thread value.)
TEST(SubscriptionSetRegression, AReadAfterAGapIsCurrentAfterOneDutyCycleRegression) {
    SetRig r(4414);
    r.connect();
    std::size_t delivered = 0;
    auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {}, [&](const rc::Record&) { ++delivered; });
    ASSERT_TRUE(r.drive([&] { return set.active() && r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
    const auto s0 = r.gw.subscribed[0];
    uint64_t want = 0;
    constexpr int kFrames = 64;   // far more than the ring's ~12 snapshots
    for (int i = 0; i < kFrames; ++i) {
        const auto b = batchOf(i, 1);
        want += b.size();
        r.gw.pushBatch(s0, i, b);
        r.passes(1);              // no read of the handle in between
    }
    ASSERT_TRUE(r.drive([&] { return delivered == static_cast<std::size_t>(kFrames); }));
    rc::Subscription& sub = set[s0.partition];

    // First read after the gap: may be stale - and here it must be, or the setup never filled the ring.
    const uint64_t firstBytes = sub.batchBytes();
    const int64_t  firstPos   = sub.position();
    EXPECT_LE(firstBytes, want);
    EXPECT_LT(firstBytes, want) << "setup: the ring never filled, so the test proves nothing";
    EXPECT_LT(firstPos, kFrames) << "setup: the ring never filled, so the test proves nothing";

    // Exactly one duty cycle, nothing pushed: the next read is current.
    r.client.doWork();
    EXPECT_EQ(sub.batchBytes(), want);
    EXPECT_EQ(sub.position(), kFrames);
    EXPECT_EQ(set.batchBytes(), want);
}

// #473 adversarial (M31+ review): the gap ends with the subscription ENDED by the broker while the ring is full.
// An app thread that polls finished() must see the end — with its reason and the final position — after its first
// read plus ONE duty cycle, never only on some later event (the final snapshot takes the ring's reserve; mutation:
// publishing it with tryPublish turns the full-ring cells red).
// Repeated over gaps of several lengths, the read side never sees finished() go back to false.
TEST(SubscriptionSetRegression, AnEndThatLandsInAFullStatusRingIsSeenAfterOneDutyCycleAdversarialRegression) {
    int32_t port = 4460;
    for (const int kFrames : {12, 13, 64, 200}) {
        SCOPED_TRACE(kFrames);
        SetRig r(port);
        port += 2;
        r.connect();
        std::size_t delivered = 0;
        auto set = r.client.subscribe(fourPartitions(r.client), rc::AllPartitions{}, {},
                                      [&](const rc::Record&) { ++delivered; });
        ASSERT_TRUE(r.drive([&] { return set.active() && r.gw.subscribed.size() == static_cast<std::size_t>(kParts); }));
        const auto s0 = r.gw.subscribed[0];
        uint64_t want = 0;
        for (int i = 0; i < kFrames; ++i) {
            const auto b = batchOf(i, 1);
            want += b.size();
            r.gw.pushBatch(s0, i, b);
            r.passes(1);                                  // no read of the handle in between
        }
        ASSERT_TRUE(r.drive([&] { return delivered == static_cast<std::size_t>(kFrames); }));
        r.gw.sendError(s0.endpoint, 45, "not authorized: READ on TOPIC t", s0.sessionId, s0.corrId);
        r.passes(5);                                      // the end lands; still no read
        rc::Subscription& sub = set[s0.partition];
        (void)sub.finished();                             // the first read after the gap may be stale
        r.client.doWork();                                // exactly one duty cycle
        EXPECT_TRUE(sub.finished()) << "the end was lost in the full ring";
        EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::NotAuthorized);
        EXPECT_EQ(sub.position(), kFrames);
        EXPECT_EQ(sub.batchBytes(), want);
        for (int i = 0; i < 50; ++i) {
            r.client.doWork();
            ASSERT_TRUE(sub.finished()) << "finished() went back to false at read " << i;
        }
        for (int p = 1; p < kParts; ++p) EXPECT_FALSE(set[p].finished()) << "partition " << p << " ended too";
    }
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
