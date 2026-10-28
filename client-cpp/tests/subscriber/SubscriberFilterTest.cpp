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

// #428: the same assertions for a KEY subscription (the broker resolves its partition; the refusal detail reaches
// onError just the same, and nothing falls back to an unfiltered Subscribe).
TEST(SubscriberFilterRegression, KeySubscriptionRefusalCarriesTheDetailRegression) {
    FilterRig r(4309);
    r.gw.subscribeErrorCode   = 39;   // InvalidRequest
    r.gw.subscribeErrorDetail = "filter needs a typed topic";
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    const rc::Key key(uint64_t{7});
    auto sub = r.client.subscribe(r.client.topic("t"), key, opts, &noopRecords);
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

// ---- Fix round 1 ------------------------------------------------------------------------------------------------

// I1: the handler unsubscribes on the first record of a rebuilt batch: the position must not jump past the
// matches it never got, or the documented "re-subscribe from position()" loses them.
TEST(SubscriberFilterRegression, HandlerUnsubscribeMidBatchKeepsTheLastDeliveredPositionRegression) {
    FilterRig r(4326);
    r.connect();
    std::vector<int64_t> seen;
    rc::Subscription* self = nullptr;
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, [&](const rc::Record& rec) {
        seen.push_back(rec.offset);
        if (self != nullptr) self->unsubscribe();
    });
    self = &sub;
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    r.gw.pushBatch(*r.firstSub(), 10, holedBatch(10));
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(seen, (std::vector<int64_t>{10}));
    EXPECT_EQ(sub.position(), 11) << "last delivered + 1, not the batch end (15)";
    // Re-subscribe from position(): the broker re-sends the batch from 11 on; the record at 12 is delivered.
    seen.clear();
    rc::SubscribeOptions again;
    again.from = rc::From::offset(sub.position());
    again.filter(E::Filter{.worldId = 7u});
    auto sub2 = r.client.subscribe(r.client.topic("t"), 0, again, [&](const rc::Record& rec) { seen.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.size() == 2 && sub2.subscriptionId() != 0; }));
    EXPECT_EQ(r.gw.subscribed[1].fromOffset, 11);
    r.gw.pushBatch(r.gw.subscribed[1], 10, holedBatch(10));
    ASSERT_TRUE(r.drive([&] { return sub2.position() == 15; }));
    EXPECT_EQ(seen, (std::vector<int64_t>{10, 12}));   // nothing between 11 and 15 was lost (10 is the broker's batch)
}

namespace {
struct TwoNode : FilterRig {
    int n1 = 0;
    int port1 = 0;
    explicit TwoNode(int32_t in) : FilterRig(in) {
        const auto [e, p] = addNode(in);
        n1 = e; port1 = p;
        gw.moreBrokers = {{1, "127.0.0.1", port1}};
        gw.partitions  = {{1, 0, /*leader*/ 0, 1, in, ""}, {1, 1, /*leader*/ 0, 1, in, ""}};
    }
    void redirectTo1(const test_support::InvokerGateway::Subscribed& s) {
        gw.subscribeLeader = {{0, n1}, {1, n1}};
        gw.partitions      = {{1, 0, n1, 1, 0, ""}, {1, 1, n1, 1, 0, ""}};
        gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                     "127.0.0.1:" + std::to_string(port1), s.sessionId, s.corrId);
    }
};
}  // namespace

// I2: a node session that is reconnecting still remembers the version of its PREVIOUS connect.  The node was
// restarted on a newer broker: the filtered Subscribe is held until the reconnect, never failed BrokerTooOld.
TEST(SubscriberFilterRegression, ReconnectingNodeWithAStaleOldVersionHoldsTheFilteredSubscribeRegression) {
    TwoNode r(4328);
    r.gw.advertisedBrokerVersionAt[r.n1] = 5;
    r.connect();
    // A plain subscription connects node 1's session while it is still v5.
    r.gw.subscribeLeader = {{0, r.n1}, {1, 0}};
    auto plain = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return plain.subscriptionId() != 0; }));
    const auto plainSub = r.gw.subscribed.back();
    ASSERT_EQ(plainSub.endpoint, r.n1);
    // The filtered one is bound on the seed (partition 1).
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto filtered = r.client.subscribe(r.client.topic("t"), 1, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return filtered.subscriptionId() != 0; }));
    const auto fSub = r.gw.subscribed.back();
    ASSERT_EQ(fSub.endpoint, 0);
    // Node 1 restarts on v6 and its client session drops; before it reconnects, the partition moves there.
    r.gw.advertisedBrokerVersionAt[r.n1] = 6;
    r.gw.expireSession(plainSub.sessionId);
    ASSERT_TRUE(r.drive([&] { return r.client.testSessionStateAt(1) != rc::session::SessionState::Connected; }));
    r.redirectTo1(fSub);
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.back().endpoint == r.n1 && !r.gw.subscribed.back().filter.empty(); }))
        << "held until node 1 reconnected, then sent";
    EXPECT_NE(filtered.failReason(), rc::SubscriptionFailReason::BrokerTooOld);
    EXPECT_FALSE(filtered.finished());
}

// I3: the filter survives a lost node session (driveHomed re-arms the bound subscription) ...
TEST(SubscriberFilterRegression, FilterSurvivesALostNodeSessionRearmRegression) {
    TwoNode r(4330);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    r.gw.subscribeLeader = {{0, r.n1}, {1, 0}};
    r.gw.partitions      = {{1, 0, r.n1, 1, 4330, ""}, {1, 1, 0, 1, 4330, ""}};
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto first = r.gw.subscribed.back();
    ASSERT_EQ(first.endpoint, r.n1);
    ASSERT_FALSE(first.filter.empty());
    r.gw.expireSession(first.sessionId);
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.size() == 2; })) << "re-armed on the reconnected node session";
    EXPECT_EQ(r.gw.subscribed[1].filter, first.filter);
    for (const auto& f : r.gw.subscribeFrames) {
        const auto v = rcc::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_FALSE(v->filter.empty()) << "never an unfiltered fallback";
    }
}

// ... and a transient partition's data reset (restartFromLogStart).
TEST(SubscriberFilterRegression, FilterSurvivesADataResetRestartRegression) {
    FilterRig r(4332);
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto first = *r.firstSub();
    r.gw.sendLineageNote(first, 1);
    r.gw.pushRecords(first, 0, 3);
    ASSERT_TRUE(r.drive([&] { return sub.position() == 3; }));
    r.gw.partitionLineage[0] = 2;   // the partition's data was reset: the resume's claim (1) no longer matches
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), "", first.sessionId, first.corrId);
    ASSERT_TRUE(r.drive([&] { return r.gw.dataResetAnswers == 1 && r.gw.subscribed.size() == 2; }));
    EXPECT_EQ(sub.dataResets(), 1u);
    EXPECT_EQ(r.gw.subscribed[1].fromOffset, -2) << "restarted at the log start";
    EXPECT_EQ(r.gw.subscribed[1].filter, first.filter);
    EXPECT_FALSE(r.gw.subscribed[1].filter.empty());
    for (const auto& f : r.gw.subscribeFrames) {
        const auto v = rcc::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(v.has_value());
        EXPECT_FALSE(v->filter.empty());
    }
}

// M1: a refusal whose detail trailer is cut short still finishes the subscription with the broker's code.
TEST(SubscriberFilterRegression, TruncatedRefusalDetailStillSurfacesTheRefusalRegression) {
    FilterRig r(4334);
    r.gw.subscribeErrorCode = 39;
    r.gw.subscribeBadDetailTrailer = true;
    r.connect();
    rc::SubscribeOptions opts;
    opts.filter(E::Filter{.worldId = 7u});
    auto sub = r.client.subscribe(r.client.topic("t"), 0, opts, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }, 20'000)) << "a refusal, not a subscribe timeout";
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), 39);
    ASSERT_FALSE(r.errors.empty());
    EXPECT_NE(r.errors.back().find("refused"), std::string::npos) << r.errors.back();
}

// I4: an unfiltered subscription carries no filter storage.
TEST(SubscriberFilter, UnfilteredSubscriptionImplCarriesNoFilterStorage) {
    EXPECT_LT(sizeof(rc::SubscriptionImpl), 1024u);
    rc::SubscriptionImpl impl;
    EXPECT_FALSE(impl.filtered());
    EXPECT_EQ(impl.filterData.get(), nullptr);
    rc::SubscribeOptions o;
    o.filter(E::Filter{.worldId = 7u});
    impl.setFilter(o.filterBytes);
    EXPECT_TRUE(impl.filtered());
    EXPECT_EQ(std::vector<uint8_t>(impl.filterSpan().begin(), impl.filterSpan().end()),
              std::vector<uint8_t>(o.filterBytes.span().begin(), o.filterBytes.span().end()));
}

namespace {
std::vector<uint8_t> controlBatch(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("c", "m", 1000);
    std::vector<uint8_t> v = b.build();
    const uint16_t attrs = static_cast<uint16_t>(rune::protocol::batch::kAttrControl);
    v[rune::protocol::batch::kAttributesOffset]     = static_cast<uint8_t>(v[rune::protocol::batch::kAttributesOffset] | (attrs & 0xFF));
    v[rune::protocol::batch::kAttributesOffset + 1] = static_cast<uint8_t>(v[rune::protocol::batch::kAttributesOffset + 1] | (attrs >> 8));
    const uint32_t crc = rune::protocol::crc32c(v.data() + rune::protocol::batch::kAttributesOffset,
                                                v.size() - rune::protocol::batch::kAttributesOffset);
    std::memcpy(v.data() + rune::protocol::batch::kCrcOffset, &crc, 4);
    return v;
}
std::vector<uint8_t> plainBatch(int64_t base, int n) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < n; ++i) b.append("k", "v", 1000);
    return b.build();
}
} // namespace

// #428 decision: SubscriptionStatus::batchBytes counts only DELIVERED batches.  One delivered, one duplicate (a
// moved subscription's batch wholly below its resume floor), one control (its offsets consumed, never delivered)
// and one malformed batch: only the delivered one adds.  Mutation: count every received frame (the old
// `batchBytes += rb.size()` up front) -> red.
TEST(SubscriberStatusRegression, BatchBytesCountsOnlyDeliveredBatchesRegression) {
    FilterRig r(4326);
    const auto [n1, port1] = r.addNode(4326);
    r.gw.moreBrokers = {{1, "127.0.0.1", port1}};
    r.gw.partitions  = {{1, 0, /*leader*/ 0, 1, 4326, ""}, {1, 1, /*leader*/ 0, 1, 4326, ""}};
    r.connect();
    std::vector<int64_t> seen;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, {}, [&](const rc::Record& rec) { seen.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() != 0; }));
    const auto first = *r.firstSub();
    r.gw.pushRecords(first, 0, 3);   // offsets 0..2 delivered: the position is 3
    ASSERT_TRUE(r.drive([&] { return sub.position() == 3; }));
    const uint64_t before = sub.batchBytes();
    // The partition moves to node 1: the re-armed subscription carries a resume floor of 3.
    r.gw.subscribeLeader = {{0, n1}, {1, 0}};
    r.gw.partitions      = {{1, 0, /*leader*/ n1, 1, 4326, ""}, {1, 1, /*leader*/ 0, 1, 4326, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(port1), first.sessionId, first.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr; })) << "re-subscribed on node 1";
    const auto moved = *r.subAt(0, n1);

    const auto duplicate = plainBatch(0, 2);     // offsets 0..1: wholly below the floor
    const auto delivered = plainBatch(3, 2);     // offsets 3..4
    const auto control   = controlBatch(5);      // offset 5: consumed
    const std::vector<uint8_t> malformed(40, 0xEE);
    r.gw.pushBatch(moved, 0, duplicate);
    r.gw.pushBatch(moved, 3, delivered);
    r.gw.pushBatch(moved, 5, control);
    r.gw.pushBatch(moved, 6, malformed);
    ASSERT_TRUE(r.drive([&] { return sub.position() == 6; })) << "delivered + control consumed through offset 5";
    r.passes(300);   // the malformed frame is certainly processed too
    EXPECT_EQ(seen, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(sub.batchBytes() - before, delivered.size())
        << "only the delivered batch counts (duplicate " << duplicate.size() << ", control " << control.size()
        << ", malformed " << malformed.size() << ")";
}
