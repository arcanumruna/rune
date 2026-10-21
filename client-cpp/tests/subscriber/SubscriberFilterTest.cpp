// tests/subscriber/SubscriberFilterTest.cpp — Phase 2c Task 11: the client side of subscription filters.
// A real Client in Invoker mode over the InvokerGateway fake: the filter reaches the broker verbatim, the
// broker-version gate (SD3), refusal details, and the filtered position rules (SD11).  Single-threaded; every
// wait is a bounded pump loop.
#include "../support/GroupRig.h"

#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/codec/Fingerprint.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using E = rune::gen::PlayerEvent;
namespace rcc = rune::client::codec;

void noopRecords(const rc::Record&) {}

/// One filtered-or-not subscription on partition `p` of topic "t", with an onError log.
struct FilterRig : GroupRig {
    std::vector<std::string> errors;
    explicit FilterRig(int32_t in) : GroupRig(in, in + 1) {
        client.onError([this](const std::string& m) { errors.push_back(m); });
    }
    [[nodiscard]] const test_support::InvokerGateway::Subscribed* firstSub() const {
        return gw.subscribed.empty() ? nullptr : &gw.subscribed.front();
    }
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
};

/// A 2-record batch at `base` whose records sit at offsets base+0 and base+2 and whose lastOffsetDelta is 4 (a
/// filtered source batch rebuilt without its non-matching records): holes below and at its end.
std::vector<uint8_t> holedBatch(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("a", "1", 1000);
    b.append("b", "2", 1000);
    std::vector<uint8_t> v = b.build();
    const int rec1 = rune::protocol::batch::kRecordBatchOverhead + 9;   // record 0 is 9 bytes (1-byte varints)
    EXPECT_EQ(v[static_cast<std::size_t>(rec1 + 3)], 2u) << "record 1's offsetDelta (zigzag 1)";
    v[static_cast<std::size_t>(rec1 + 3)] = 4;                          // offsetDelta 2
    v[rune::protocol::batch::kLastOffsetDeltaOffset] = 4;               // lastOffsetDelta 4
    const uint32_t crc = rune::protocol::crc32c(v.data() + rune::protocol::batch::kAttributesOffset,
                                                v.size() - rune::protocol::batch::kAttributesOffset);
    std::memcpy(v.data() + rune::protocol::batch::kCrcOffset, &crc, 4);   // little-endian host (as the builder)
    return v;
}
} // namespace

TEST(SubscriberFilter, FilterBytesReachTheBrokerVerbatim) {
    FilterRig r(4300);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty(); }));
    const auto want = E::Filter{.worldId = 7u}.encode();
    ASSERT_FALSE(want.empty());
    EXPECT_EQ(r.gw.subscribed[0].filter, std::vector<uint8_t>(want.span().begin(), want.span().end()));
}

TEST(SubscriberFilterRegression, UnfilteredSubscribeFrameIsByteIdenticalRegression) {
    FilterRig r(4302);
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribeFrames.empty(); }));
    const auto& got = r.gw.subscribeFrames[0];
    const auto& s   = r.gw.subscribed[0];
    uint8_t want[rcc::kMaxSubscribeFrameBytes];
    const std::size_t n = rcc::encodeSubscribe(want, sizeof want, s.topicId, s.partition, /*partitionKnown=*/1,
                                               s.fromOffset, /*isolation=*/0, {}, s.sessionId, s.corrId);
    ASSERT_GT(n, 0u);
    EXPECT_EQ(got, std::vector<uint8_t>(want, want + n));
    EXPECT_EQ(got.size(), 24u + 18u + 2u) << "no filter trailer, no key";
}

TEST(SubscriberFilterRegression, OldBrokerFailsTheFilteredSubscriptionNothingSentRegression) {
    FilterRig r(4304);
    r.gw.advertisedBrokerVersion = 5;   // below kBrokerVersionFilter: it would ignore the trailer
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::BrokerTooOld);
    r.passes(200);
    EXPECT_TRUE(r.gw.subscribed.empty()) << "nothing may be sent to a broker that would ignore the filter";
    EXPECT_TRUE(r.gw.subscribeFrames.empty());
    // An unfiltered subscription on the same old broker still binds.
    auto plain = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return plain.subscriptionId() != 0; }));
}

TEST(SubscriberFilterRegression, MixedClusterChecksTheDestinationSessionRegression) {
    FilterRig r(4306);
    const auto [n1, port1] = r.addNode(4306);
    r.gw.moreBrokers = {{1, "127.0.0.1", port1}};
    r.gw.partitions  = {{1, 0, /*leader*/ 1, 1, 4306, ""}, {1, 1, /*leader*/ 0, 1, 4306, ""}};
    r.gw.subscribeLeader = {{0, n1}, {1, 0}};          // the seed redirects partition 0 to node 1
    r.gw.advertisedBrokerVersionAt[n1] = 5;            // node 1 is old; the seed is current
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto filtered = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    auto plain    = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return filtered.finished() && plain.subscriptionId() != 0; }))
        << "the filtered one fails at the re-send to node 1; the unfiltered one binds there";
    EXPECT_EQ(filtered.failReason(), rc::SubscriptionFailReason::BrokerTooOld);
    EXPECT_EQ(plain.failReason(), rc::SubscriptionFailReason::None);
    for (const auto& s : r.gw.subscribed) {
        EXPECT_TRUE(s.filter.empty()) << "no filtered Subscribe was accepted by any node";
    }
}

TEST(SubscriberFilterRegression, RefusedFilterSurfacesTheDetailNeverFallsBackRegression) {
    FilterRig r(4308);
    r.gw.subscribeErrorCode   = 39;   // InvalidRequest
    r.gw.subscribeErrorDetail = "filter needs a typed topic";
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), 39);
    ASSERT_TRUE(r.drive([&] { return !r.errors.empty(); }));
    EXPECT_NE(r.errors.back().find("filter needs a typed topic"), std::string::npos) << r.errors.back();
    EXPECT_NE(r.errors.back().find("InvalidRequest"), std::string::npos) << r.errors.back();
    r.passes(300);
    EXPECT_EQ(r.gw.subscribeFrames.size(), 1u) << "never an unfiltered fallback";
    EXPECT_TRUE(r.gw.subscribed.empty());
}

TEST(SubscriberFilterRegression, EncodeErrorThrowsBeforeAnythingIsSentRegression) {
    FilterRig r(4310);
    r.connect();
    rc::SubscribeOptions opts;
    EXPECT_THROW(opts.filter(E::Filter{.worldId = int64_t{-1}}), std::invalid_argument);
    EXPECT_TRUE(opts.filterBytes.empty());
    r.passes(50);
    EXPECT_TRUE(r.gw.subscribeFrames.empty());
}

TEST(SubscriberFilterRegression, PositionFollowsNotesOnAQuietFilterRegression) {
    FilterRig r(4312);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto s = *r.firstSub();
    r.gw.pushNote(s, 5000);
    ASSERT_TRUE(r.drive([&] { return sub.position() == 5000; }));
    r.gw.pushNote(s, 4000);   // older: monotonic
    r.passes(300);
    EXPECT_EQ(sub.position(), 5000);
}

TEST(SubscriberFilterRegression, PositionMovesToTheRebuiltBatchEndRegression) {
    FilterRig r(4314);
    r.connect();
    std::vector<int64_t> seen;
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, [&](const rc::Record& rec) { seen.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    r.gw.pushBatch(*r.firstSub(), 10, holedBatch(10));
    ASSERT_TRUE(r.drive([&] { return sub.position() == 15; }));
    EXPECT_EQ(seen, (std::vector<int64_t>{10, 12}));
}

TEST(SubscriberFilterRegression, UnfilteredPositionRulesAreUnchangedRegression) {
    FilterRig r(4316);
    r.connect();
    std::vector<int64_t> seen;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, {}, [&](const rc::Record& rec) { seen.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    r.gw.pushNote(*r.firstSub(), 5000);
    r.passes(300);
    EXPECT_EQ(sub.position(), 0) << "an unfiltered note is only a resumeHint";
    r.gw.pushBatch(*r.firstSub(), 10, holedBatch(10));
    ASSERT_TRUE(r.drive([&] { return sub.position() == 13; }));
    EXPECT_EQ(seen, (std::vector<int64_t>{10, 12}));
}

TEST(SubscriberFilterRegression, MovedSubscriptionResubscribesWithTheSameFilterBytesRegression) {
    FilterRig r(4318);
    const auto [n1, port1] = r.addNode(4318);
    r.gw.moreBrokers = {{1, "127.0.0.1", port1}};
    r.gw.partitions  = {{1, 0, /*leader*/ 0, 1, 4318, ""}, {1, 1, /*leader*/ 0, 1, 4318, ""}};
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto first = *r.firstSub();
    r.gw.pushRecords(first, 0, 3);
    ASSERT_TRUE(r.drive([&] { return sub.position() == 3; }));
    // The partition moved to node 1: the seed ends the subscription Redirect.
    r.gw.subscribeLeader = {{0, n1}, {1, 0}};
    r.gw.partitions      = {{1, 0, /*leader*/ n1, 1, 4318, ""}, {1, 1, /*leader*/ 0, 1, 4318, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(port1), first.sessionId, first.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr; })) << "re-subscribed on node 1";
    const auto& moved = *r.subAt(0, n1);
    EXPECT_EQ(moved.filter, first.filter);
    EXPECT_FALSE(moved.filter.empty());
    EXPECT_EQ(moved.fromOffset, 3);
    EXPECT_EQ(moved.fromOffset, sub.position());
}

TEST(SubscriberFilter, RecordCarriesItsPartition) {
    FilterRig r(4320);
    r.connect();
    std::vector<int32_t> parts;
    auto sub = r.client.subscribe(r.client.topic("t"), 1, {}, [&](const rc::Record& rec) { parts.push_back(rec.partition); });
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    r.gw.pushRecords(*r.firstSub(), 0, 2);
    ASSERT_TRUE(r.drive([&] { return parts.size() == 2; }));
    EXPECT_EQ(parts, (std::vector<int32_t>{1, 1}));
}

TEST(SubscriberFilter, BatchBytesCountsDeliveredBatches) {
    FilterRig r(4322);
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    EXPECT_EQ(sub.batchBytes(), 0u);
    const auto a = holedBatch(0);
    const auto b = holedBatch(10);
    r.gw.pushBatch(*r.firstSub(), 0, a);
    r.gw.pushBatch(*r.firstSub(), 10, b);
    ASSERT_TRUE(r.drive([&] { return sub.batchBytes() == a.size() + b.size(); }));
}

TEST(SubscriberFilterRegression, WorkerInvalidRequestFailsTheBoundSubscriptionRegression) {
    FilterRig r(4324);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto s = *r.firstSub();
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest), "filter rejected by worker",
                   s.sessionId, s.corrId);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), 39);
    ASSERT_FALSE(r.errors.empty());
    EXPECT_NE(r.errors.back().find("filter rejected by worker"), std::string::npos);
}

TEST(ClientFilterSchemaRegression, SharedDescriptorMatchesTheCodecToolRegression) {
    EXPECT_EQ(rune::codec::fingerprint(rune::examples::playerEventDescriptor()), E::kFingerprint);
}
