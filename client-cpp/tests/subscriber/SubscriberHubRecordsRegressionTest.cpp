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

// ---- Issue #180: a RecordBatch header recordCount that disagrees with its body is rejected ------------------
namespace {
// Set the header's recordCount and re-seal the CRC (the CRC covers the count: a hostile or corrupt broker re-seals).
std::vector<uint8_t> withCount(std::vector<uint8_t> b, uint32_t count) {
    namespace pb = rune::protocol::batch;
    for (int i = 0; i < 4; ++i) b[static_cast<std::size_t>(pb::kRecordCountOffset + i)] = static_cast<uint8_t>(count >> (8 * i));
    const uint32_t crc = rune::protocol::crc32c(b.data() + pb::kAttributesOffset, b.size() - pb::kAttributesOffset);
    for (int i = 0; i < 4; ++i) b[static_cast<std::size_t>(pb::kCrcOffset + i)] = static_cast<uint8_t>(crc >> (8 * i));
    return b;
}
std::size_t visited(const rc::codec::RecordsView& v) {
    std::size_t n = 0;
    v.forEachRecord([&](std::string_view, std::string_view, int64_t, int64_t) { ++n; });
    return n;
}
}   // namespace

// Table: header count in {actual-1, actual+1, 0, max int32, max uint32 (-1)} x body in {empty, 1 record, many}.
// Every disagreeing batch is invalid (the reader's ensureValid), a view over it visits nothing, and the subscriber
// counts it malformed and neither delivers nor moves the position.
TEST(RecordsViewRegression, HeaderCountDisagreeingWithBodyIsRejectedRegression) {
    int cases = 0;
    for (const int32_t actual : {0, 1, 2, 7, 40}) {
        const auto good = batchAt(0, actual);
        std::vector<uint32_t> counts{static_cast<uint32_t>(actual + 1), 0u, 0x7FFFFFFFu, 0xFFFFFFFFu, 0x80000000u};
        if (actual > 0) counts.push_back(static_cast<uint32_t>(actual - 1));
        for (const uint32_t cnt : counts) {
            if (cnt == static_cast<uint32_t>(actual)) continue;
            const auto bad = withCount(good, cnt);
            SCOPED_TRACE("actual=" + std::to_string(actual) + " headerCount=" + std::to_string(cnt));
            rune::protocol::RecordBatchReader reader(bad.data(), bad.size());
            EXPECT_FALSE(reader.ensureValid()) << "the CRC is valid; the count disagrees with the body";
            Bound b(kTopic, 100 + cases, 500 + cases);
            const auto view = b.view(bad, 0);
            EXPECT_EQ(visited(view), 0u) << "a view over a disagreeing batch visits nothing";
            b.hub.onRecords(view);
            EXPECT_TRUE(b.delivered.empty()) << "a partial or over-read batch is never delivered";
            EXPECT_EQ(b.hub.malformedBatches(), 1u);
            EXPECT_EQ(b.impl->nextOffset, 0) << "the position does not move";
            ++cases;
        }
    }
    EXPECT_GE(cases, 25) << "vacuity guard: the table ran";
}

TEST(RecordsViewRegression, HeaderCountMatchingBodyDecodesEveryRecordRegression) {
    int cases = 0;
    for (const int32_t actual : {0, 1, 2, 7, 40}) {
        const auto good = batchAt(0, actual);
        SCOPED_TRACE("actual=" + std::to_string(actual));
        rune::protocol::RecordBatchReader reader(good.data(), good.size());
        EXPECT_TRUE(reader.ensureValid());
        Bound b(kTopic, 200 + cases, 600 + cases);
        const auto view = b.view(good, 0);
        EXPECT_EQ(visited(view), static_cast<std::size_t>(actual));
        b.hub.onRecords(view);
        EXPECT_EQ(b.delivered, range(0, actual));
        EXPECT_EQ(b.hub.malformedBatches(), 0u);
        ++cases;
    }
    EXPECT_EQ(cases, 5);
}

// ---- C17 adversarial review (#180): a REAL end-transaction marker in a Records frame -------------------------
// The broker writes markers in the 75-byte layout of broker/libs/storage/include/rune/storage/ControlMarkerLayout.h:
// ONE record WITHOUT a record-length varint (the control batches above are protocol-builder batches, which do carry
// one, so they never exercised it).  A frame [data, marker, data] must deliver every data record, consume the
// marker's offset, and count nothing malformed.
//
// Guards #180's control-batch exemption: ensureValid walks the records of data batches only.
namespace {
std::vector<uint8_t> brokerLayoutMarker(int64_t base, int16_t controlType) {
    namespace pb = rune::protocol::batch;
    std::vector<uint8_t> m(75, 0);
    auto put = [&](std::size_t off, uint64_t v, int n) { for (int i = 0; i < n; ++i) m[off + static_cast<std::size_t>(i)] = uint8_t(v >> (8 * i)); };
    put(0, static_cast<uint64_t>(base), 8);
    put(pb::kLengthOffset, 75 - 12, 4);
    put(12, 0xFFFFFFFFu, 4);                 // partitionLeaderEpoch -1
    m[16] = 2;                               // magic
    put(pb::kAttributesOffset, 0x0030, 2);   // CONTROL | TRANSACTIONAL
    put(23, 0, 4);                           // lastOffsetDelta 0
    put(43, 42, 8);                          // producerId
    put(51, 0, 2);                           // producerEpoch
    put(53, 0xFFFFFFFFu, 4);                 // baseSequence -1
    put(pb::kRecordCountOffset, 1, 4);       // one record
    // record (no length varint): attrs 0, tsDelta 0, offsetDelta 0, keyLen zz(2), key, valueLen zz(6), value, headers 0
    m[64] = 0x04; put(65, static_cast<uint16_t>(controlType), 2);
    m[67] = 0x0C; put(68, 1, 2); put(70, 7, 4);
    m[74] = 0;
    put(pb::kCrcOffset, rune::protocol::crc32c(m.data() + pb::kAttributesOffset, m.size() - pb::kAttributesOffset), 4);
    return m;
}
}   // namespace

TEST(SubscriberHubAdversarialRegression, ABrokerLayoutMarkerMidFrameIsConsumedAndTheDataAfterItDeliveredRegression) {
    for (const int16_t type : {int16_t{0}, int16_t{1}}) {   // commit, abort
        SCOPED_TRACE("controlType=" + std::to_string(type));
        Bound b(kTopic, 900 + type, 950 + type);
        const auto frame = concat({batchAt(0, 3), brokerLayoutMarker(3, type), batchAt(4, 2)});
        b.hub.onRecords(b.view(frame, 0));
        EXPECT_EQ(b.delivered, (std::vector<int64_t>{0, 1, 2, 4, 5})) << "the data after the marker is delivered";
        EXPECT_EQ(b.impl->nextOffset, 6) << "the position passes the marker and the batch after it";
        EXPECT_EQ(b.hub.malformedBatches(), 0u) << "a broker marker is not malformed";
        EXPECT_EQ(b.hub.controlBatchesSkipped(), 1u);
    }
}

TEST(SubscriberHubAdversarialRegression, AFrameOpeningWithABrokerLayoutMarkerMovesThePositionRegression) {
    Bound b(kTopic, 960, 970);
    const auto frame = concat({brokerLayoutMarker(0, 0), batchAt(1, 2)});
    b.hub.onRecords(b.view(frame, 0));
    EXPECT_EQ(b.delivered, (std::vector<int64_t>{1, 2}));
    EXPECT_EQ(b.impl->nextOffset, 3) << "never stuck before the marker";
    EXPECT_EQ(b.hub.malformedBatches(), 0u);
}
