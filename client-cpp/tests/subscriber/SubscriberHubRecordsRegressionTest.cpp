// tests/subscriber/SubscriberHubRecordsRegressionTest.cpp — catalog history Task 13
// fix round 2: SubscriberHub::onRecords over the whole Records-frame class.
//   - several whole batches in one frame (the catalog read server coalesces), on
//     any topic: every record delivered once, the position at the frame's end;
//   - a resume floor across batches (wholly below, then straddling);
//   - a malformed tail (counted, what was decoded stands) / a malformed first batch;
//   - a control batch (consumed, never delivered);
//   - the catalog's 8-byte epoch note / caught-up marker, the lineage note, the
//     empty position note;
//   - zero allocation per frame (hard rule 4: no std::function on the data path).
// The test thread plays the duty thread (the hub is duty-owned).
#include "../support/AllocCounter.h"

#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <memory>
#include <vector>

namespace rc  = rune::client;
namespace rcd = rune::client::detail;
using rune::client::subscriber::SubscriberHub;

namespace {
constexpr int64_t kSession = 7;
constexpr int32_t kTopic   = 3;

struct Bound {
    SubscriberHub                        hub;
    std::unique_ptr<rc::SubscriptionImpl> impl = std::make_unique<rc::SubscriptionImpl>();
    std::vector<int64_t>                 delivered;
    std::vector<std::pair<int64_t, uint64_t>> markers;   // catalog: (hw, epoch)
    std::vector<uint64_t>                notes;           // catalog: epochs
    int64_t                              subId;

    explicit Bound(int32_t topic, int64_t corr, int64_t sid) : subId(sid) {
        impl->correlationId = corr;
        impl->topicId       = topic;
        impl->partition     = 0;
        // A plain pointer capture: invoking it never allocates.
        impl->handler = [v = &delivered](const rc::Record& r) { v->push_back(r.offset); };
        if (topic == rune::protocol::client::kCatalogTopicId) {
            impl->onCaughtUp     = [m = &markers](int64_t hw, uint64_t e) { m->emplace_back(hw, e); };
            impl->onHistoryEpoch = [n = &notes](uint64_t e) { n->push_back(e); };
        }
        delivered.reserve(4096);
        auto send = [](int32_t, const uint8_t*, std::size_t) { return true; };
        auto req = SubscriberHub::makeSubscribeRequest(impl.get(), kSession, 1, -2, 0, {},
                                                       std::chrono::steady_clock::now() + std::chrono::hours(1));
        rcd::HubCommand c;
        c.op     = rcd::HubCommand::Op::SubscribeAdd;
        c.handle = reinterpret_cast<uint64_t>(req.release());
        hub.applyCommand(c, 0, kSession, send);
        rc::codec::SubscribeResponseView r;
        r.sessionId = kSession;
        r.correlationId = corr;
        r.subscriptionId = sid;
        hub.onSubscribeResponse(r, 0, kSession, send);
    }
    rc::codec::RecordsView view(std::span<const uint8_t> batches, int64_t base) const {
        rc::codec::RecordsView rv;
        rv.sessionId      = kSession;
        rv.subscriptionId = subId;
        rv.topicId        = impl->topicId;
        rv.partition      = 0;
        rv.baseOffset     = base;
        rv.recordBatch    = batches;
        return rv;
    }
};

std::vector<uint8_t> batchAt(int64_t base, int32_t n, bool control = false) {
    rune::protocol::RecordBatchBuilder b(base, 1'000'000);
    if (control) b.setControl(true);
    for (int32_t i = 0; i < n; ++i) b.append("k", "v", 1'000'000 + i);
    return b.build();
}
std::vector<uint8_t> concat(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> all;
    for (const auto& p : parts) all.insert(all.end(), p.begin(), p.end());
    return all;
}
std::vector<int64_t> range(int64_t from, int64_t to) {
    std::vector<int64_t> v;
    for (int64_t i = from; i < to; ++i) v.push_back(i);
    return v;
}
}   // namespace

// RED (before fix round 1): only the first batch was decoded.
TEST(SubscriberHubRecordsRegression, EveryBatchOfAMultiBatchFrameIsDeliveredOnAnyTopicRegression) {
    Bound b(kTopic, 11, 101);
    const auto frame = concat({batchAt(0, 3), batchAt(3, 2), batchAt(5, 4)});
    b.hub.onRecords(b.view(frame, 0));
    EXPECT_EQ(b.delivered, range(0, 9));
    EXPECT_EQ(b.impl->nextOffset, 9) << "the position is the frame's end";
    EXPECT_EQ(b.hub.malformedBatches(), 0u);
}

// The resume floor across batches: a wholly-below batch is skipped, the floor
// stays until a batch at/above it — which delivers only from the floor.  RED
// (a `return` instead of a skip): nothing after the first batch is delivered.
TEST(SubscriberHubRecordsRegression, AResumeFloorHoldsAcrossTheBatchesOfOneFrameRegression) {
    Bound b(kTopic, 12, 102);
    b.impl->resumeFloor = 11;
    const auto frame = concat({batchAt(8, 2), batchAt(10, 3), batchAt(13, 2)});
    b.hub.onRecords(b.view(frame, 8));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{11, 12, 13, 14}));
    EXPECT_EQ(b.hub.movedDuplicateBatchesDropped(), 1u);
    EXPECT_EQ(b.impl->resumeFloor, -1) << "cleared by the straddling batch";
    EXPECT_EQ(b.impl->nextOffset, 15);
}

// RED (round 1): a malformed tail vanished uncounted.
TEST(SubscriberHubRecordsRegression, AMalformedTailIsCountedAndWhatWasDecodedStandsRegression) {
    Bound b(kTopic, 13, 103);
    auto bad = batchAt(2, 2);
    bad[bad.size() - 1] ^= 0xff;   // CRC mismatch
    const auto frame = concat({batchAt(0, 2), bad, batchAt(4, 1)});
    b.hub.onRecords(b.view(frame, 0));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(b.hub.malformedBatches(), 1u);
    const auto badFirst = concat({bad});
    b.hub.onRecords(b.view(badFirst, 2));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(b.hub.malformedBatches(), 2u) << "a malformed first batch is counted too";
}

TEST(SubscriberHubRecordsRegression, AControlBatchIsConsumedNeverDeliveredRegression) {
    Bound b(kTopic, 14, 104);
    const auto frame = concat({batchAt(0, 2), batchAt(2, 1, /*control=*/true), batchAt(3, 2)});
    b.hub.onRecords(b.view(frame, 0));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{0, 1, 3, 4}));
    EXPECT_EQ(b.hub.controlBatchesSkipped(), 1u);
    EXPECT_EQ(b.impl->nextOffset, 5);
    const auto onlyMarker = concat({batchAt(5, 1, /*control=*/true)});
    b.hub.onRecords(b.view(onlyMarker, 5));
    EXPECT_EQ(b.impl->nextOffset, 6) << "its offset is consumed";
}

// The catalog's epoch frames: the note (baseOffset -1) and the caught-up marker
// (baseOffset = HW), both 8 bytes; an empty catalog frame is NOT a marker.
TEST(SubscriberHubRecordsRegression, CatalogEpochNoteAndMarkerReachTheirCallbacksRegression) {
    Bound b(rune::protocol::client::kCatalogTopicId, 15, 105);
    const uint8_t e[8] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
    const uint64_t epoch = 0xefcdab8967452301ULL;
    b.hub.onRecords(b.view(std::span<const uint8_t>(e, 8), -1));
    const auto frame = concat({batchAt(0, 2), batchAt(2, 1)});
    b.hub.onRecords(b.view(frame, 0));
    b.hub.onRecords(b.view(std::span<const uint8_t>(e, 8), 3));
    EXPECT_EQ(b.notes, (std::vector<uint64_t>{epoch}));
    EXPECT_EQ(b.delivered, range(0, 3));
    EXPECT_EQ(b.markers, (std::vector<std::pair<int64_t, uint64_t>>{{3, epoch}}));
    b.hub.onRecords(b.view({}, 3));
    EXPECT_EQ(b.markers.size(), 1u) << "an empty catalog frame carries no epoch: never a marker";
    EXPECT_EQ(b.hub.malformedBatches(), 1u);
    EXPECT_EQ(b.impl->lineage, 0u) << "never a lineage note";
}

TEST(SubscriberHubRecordsRegression, OnADataTopicTheNotesAreUnchangedRegression) {
    Bound b(kTopic, 16, 106);
    const uint8_t l[8] = {9, 0, 0, 0, 0, 0, 0, 0};
    b.hub.onRecords(b.view(std::span<const uint8_t>(l, 8), -1));
    EXPECT_EQ(b.impl->lineage, 9u) << "the transient lineage note";
    b.hub.onRecords(b.view({}, 40));
    EXPECT_EQ(b.impl->resumeHint, 40) << "the position note";
    EXPECT_TRUE(b.delivered.empty());
}

// Hard rule 4: a multi-batch Records frame costs no allocation on the duty thread.
// RED (round 1's RecordBatchReader::forEach — a std::function per batch; with a
// visitor larger than the library's small buffer, as libstdc++'s 16 bytes for the
// round-1 lambda): one allocation per batch.
TEST(SubscriberHubRecordsRegression, AMultiBatchFrameAllocatesNothingRegression) {
    Bound b(kTopic, 17, 107);
    std::vector<std::vector<uint8_t>> frames;
    for (int i = 0; i < 64; ++i) frames.push_back(concat({batchAt(i * 6, 2), batchAt(i * 6 + 2, 3), batchAt(i * 6 + 5, 1)}));
    b.hub.onRecords(b.view(frames[0], 0));   // warm-up (first status publish)
    alloc_counter::startThisThread();
    for (int i = 1; i < 64; ++i) b.hub.onRecords(b.view(frames[static_cast<std::size_t>(i)], i * 6));
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "the per-batch visitor is a template, never a std::function";
    EXPECT_EQ(b.delivered.size(), 64u * 6u);
}

// Phase 2c Task 1: a holed (rebuilt) batch is delivered with its records' own offsets.  The plain
// subscription's position is the last DELIVERED record + 1 (today's per-record rule); Task 11 moves a
// FILTERED subscription's position to the batch end.
TEST(SubscriberHubRecordsRegression, AHoledBatchDeliversItsOwnOffsetsRegression) {
    Bound b(kTopic, 12, 102);
    const auto src = batchAt(10, 5);
    std::vector<rune::protocol::RecordSpan> s;
    ASSERT_TRUE(rune::protocol::forEachRecordSpan(src.data(), src.size(), [&](const auto& r) { s.push_back(r); }));
    const std::array<uint32_t, 2> keep{0, 2};
    std::vector<uint8_t> out(src.size());
    const std::size_t n = rune::protocol::rebuildBatch(src, s, keep, out);
    ASSERT_GT(n, 0u);
    b.hub.onRecords(b.view(std::span<const uint8_t>(out.data(), n), 10));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{10, 12}));
    EXPECT_EQ(b.impl->nextOffset, 13);
    EXPECT_EQ(b.hub.malformedBatches(), 0u);
}
