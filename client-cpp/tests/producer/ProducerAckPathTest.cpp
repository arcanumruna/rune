// tests/producer/ProducerAckPathTest.cpp — in-flight ring + ack path on a bare
// ProducerImpl (no Aeron, injected clock, bounded pumps).
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using namespace rune::client::producer;
namespace rcc = rune::client::codec;

namespace {

constexpr int64_t kNow = 1'000'000'000;

struct Frame {
    int32_t topicId;
    int32_t partition;
    int64_t corrId;
    int32_t records;
    int32_t baseSequence;
    int64_t producerId; // F5
};

struct Capture {
    std::vector<Frame> frames;
    bool accept = true;
    ProducerImpl::SendFn fn() {
        return [this](int32_t t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            if (!accept) return false;
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({t, p, v->correlationId, r.recordCount(), r.baseSequence(),
                              r.producerId()});
            return true;
        };
    }
};

ProducerConfig ackCfg(int32_t maxInFlight, int64_t lingerUs = 0) {
    ProducerConfig c;
    c.acks = Acks::Leader;
    c.lingerUs = lingerUs;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = maxInFlight;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

int pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int64_t now = kNow) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(now);
        flushed += impl.flushBatches(now, 42, sink);
        if (d == 0) break;
    }
    return flushed;
}

const std::vector<uint8_t> kVal(16, 0x11);

} // namespace

TEST(ProducerInFlight, BatchCorrIdsAreTaggedAndAddressDirectly) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(impl.submitSendRecord(3, 0, Key(uint64_t{1}), kVal, false, 0));
    ASSERT_TRUE(impl.submitSendRecord(3, 1, Key(uint64_t{2}), kVal, false, 0));
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    for (const Frame& f : cap.frames) {
        ASSERT_TRUE(batchcorr::isBatch(f.corrId));
        EXPECT_EQ(batchcorr::ordinal(f.corrId), 0u);
        EXPECT_EQ(batchcorr::seq(f.corrId), 0u);
    }
    EXPECT_NE(batchcorr::slot(cap.frames[0].corrId), batchcorr::slot(cap.frames[1].corrId));
    EXPECT_TRUE(impl.hasInFlight());
    impl.onProduceResponse(cap.frames[0].corrId, 10);
    impl.onProduceResponse(cap.frames[1].corrId, 20);
    EXPECT_FALSE(impl.hasInFlight());
}

TEST(ProducerInFlight, OutOfOrderAckNeverOverwritesALiveSlot) {
    ProducerImpl impl(ackCfg(2));
    Capture cap;
    const auto sink = cap.fn();
    for (uint64_t k = 0; k < 3; ++k) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(k), kVal, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(cap.frames.size(), 2u) << "third batch held by maxInFlight=2";
    impl.onProduceResponse(cap.frames[1].corrId, 11);   // B acks first
    pump(impl, sink);
    EXPECT_EQ(cap.frames.size(), 2u) << "window stays full until the oldest (A) acks";
    impl.onProduceResponse(cap.frames[0].corrId, 10);   // A acks
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(batchcorr::seq(cap.frames[2].corrId), 2u);
}

TEST(ProducerInFlight, HubRoutesAcksByOrdinal) {
    ProducerHub hub;
    auto a = std::make_shared<ProducerImpl>(ackCfg(4));
    auto b = std::make_shared<ProducerImpl>(ackCfg(4));
    hub.add(a);
    hub.add(b);
    EXPECT_EQ(a->ordinal(), 0u);
    EXPECT_EQ(b->ordinal(), 1u);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(b->submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    hub.drainAndFlush(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(batchcorr::ordinal(cap.frames[0].corrId), 1u);
    hub.onProduceResponse(cap.frames[0].corrId, 5);
    EXPECT_FALSE(b->hasInFlight());
}

// Regression (fixed in this task): a batch that passes its delivery timeout is
// failed immediately and its slot released; a LATE OK ProduceResponse for that
// id must not complete anything (the old path dropped the retry record without
// failing it, then resolved the wire corrId OK on a late ack).  Each record
// completes exactly once, with the failure.
TEST(ProducerInFlight, TimedOutBatchLateOkAckCompletesNothingRegression) {
    ProducerImpl impl(ackCfg(4, /*lingerUs=*/1'000'000));
    Capture cap;
    const auto sink = cap.fn();
    std::vector<int64_t> results[2];
    for (int r = 0; r < 2; ++r) {
        const int64_t id = impl.registerCallback([&results, r](int64_t off) {
            results[r].push_back(off);
        });
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(r)), kVal, true, id));
    }
    // One batch holding both ack-wanting records (linger holds, then flush).
    pump(impl, sink);
    impl.submitFlush();
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    ASSERT_EQ(cap.frames[0].records, 2);
    const int64_t wire = cap.frames[0].corrId;

    // Past deliveryTimeoutMs (10 s) on the injected clock: batch given up.
    const int64_t late = kNow + 10'000LL * 1'000'000LL;
    impl.retransmitStalled(late, sink);
    EXPECT_FALSE(impl.hasInFlight());
    for (int r = 0; r < 2; ++r) {
        ASSERT_EQ(results[r].size(), 1u) << "record " << r << " must fail exactly once";
        EXPECT_EQ(results[r][0], -1);
    }

    // The broker's late OK ack for the given-up batch arrives.
    impl.onProduceResponse(wire, 100);
    for (int r = 0; r < 2; ++r) {
        EXPECT_EQ(results[r].size(), 1u) << "late OK ack must not complete record " << r;
        EXPECT_EQ(results[r][0], -1);
    }
}

// Regression (fixed in this task): ackTail used to advance FIFO by the acked
// batch's count, so an out-of-order ack moved it into a LIVE slice.  It must
// stay at the oldest live batch's slice start, and the ack ring must never grow
// over many out-of-order cycles (> kAckRingCapacity total ack entries).
TEST(ProducerInFlight, OutOfOrderAcksKeepAckTailAtOldestLiveRegression) {
    ProducerImpl impl(ackCfg(2, /*lingerUs=*/1'000'000));
    Capture cap;
    const auto sink = cap.fn();
    long completed = 0;
    auto sendBatch = [&](int records) {
        for (int i = 0; i < records; ++i) {
            const int64_t id = impl.registerCallback([&completed](int64_t off) {
                if (off >= 0) ++completed;
            });
            ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), kVal, true, id));
        }
        pump(impl, sink);            // linger holds the records in one builder
        impl.submitFlush();
        pump(impl, sink);            // one frame for all of them
    };

    // Seed the partition so its state exists.
    sendBatch(1);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.frames[0].corrId, 0);
    const PartitionState* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    const std::size_t ringCap = ps->ackRing.size();
    ASSERT_EQ(ringCap, kAckRingCapacity);

    constexpr int kCycles = 6'000;   // 3 ack entries/cycle -> 18'000 > 16'384
    for (int c = 0; c < kCycles; ++c) {
        const uint64_t aStart = ps->ackHead;
        const std::size_t f0 = cap.frames.size();
        sendBatch(1);                // A: slice [aStart, aStart+1)
        sendBatch(2);                // B: slice [aStart+1, aStart+3)
        ASSERT_EQ(cap.frames.size(), f0 + 2) << "cycle " << c;
        ASSERT_EQ(cap.frames[f0 + 1].records, 2);
        ASSERT_EQ(ps->ackHead, aStart + 3);

        impl.onProduceResponse(cap.frames[f0 + 1].corrId, 10);   // B acks first
        ASSERT_EQ(ps->ackTail, aStart) << "ackTail must stay at live A's slice, cycle " << c;
        impl.onProduceResponse(cap.frames[f0].corrId, 5);        // then A
        ASSERT_EQ(ps->ackTail, ps->ackHead) << "cycle " << c;
        ASSERT_EQ(ps->ackRing.size(), ringCap) << "ack ring grew at cycle " << c;
    }
    EXPECT_EQ(completed, 1 + 3L * kCycles);
    EXPECT_FALSE(impl.hasInFlight());
}

namespace {
ProducerConfig noneCfg(int32_t maxInFlight) {
    ProducerConfig c = ackCfg(maxInFlight);
    c.acks = Acks::None;
    return c;
}
} // namespace

// Regression (carried): under acks=None an ack-wanting record wrote ack-ring
// entries nothing ever reclaimed (no in-flight batch -> ackTail never moved), so
// the ring doubled forever and the callbacks never fired.  acks=None semantics
// (Kafka acks=0): the record's ack completes when its batch is handed to the
// transport, with offset -1 ("sent, offset unknown"); nothing is retained.
TEST(ProducerAcks, AcksNoneRetainsNoAckRingEntriesRegression) {
    ProducerImpl impl(noneCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    long fired = 0;
    long nonMinusOne = 0;
    constexpr int kCycles = 6'000;  // 3 ack records/cycle -> 18'000 > kAckRingCapacity
    for (int c = 0; c < kCycles; ++c) {
        for (int i = 0; i < 3; ++i) {
            const int64_t h = impl.registerCallback([&](int64_t off) {
                ++fired;
                if (off != -1) ++nonMinusOne;
            });
            ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), kVal, true, h));
        }
        pump(impl, sink);
        const PartitionState* ps = impl.partitionState(1, 0);
        ASSERT_NE(ps, nullptr);
        ASSERT_EQ(ps->ackTail, ps->ackHead) << "acks=None retained ack entries, cycle " << c;
        ASSERT_EQ(ps->ackRing.size(), static_cast<std::size_t>(kAckRingCapacity))
            << "ack ring grew at cycle " << c;
    }
    EXPECT_EQ(fired, 3L * kCycles);
    EXPECT_EQ(nonMinusOne, 0);
    EXPECT_FALSE(impl.hasInFlight());
    for (const Frame& f : cap.frames) EXPECT_EQ(f.corrId, 0) << "acks=None frames carry no batch id";
}

// Regression (carried): a Tier-2 batch always got a batch id (its future handle
// is never 0), so under acks=None it occupied an in-flight slot until a response
// / the delivery timeout, capping Tier-2 at maxInFlight per round trip and
// retransmitting.  Under acks=None Tier-2 behaves like Tier-1: wire id 0, no
// slot, the future resolves -1 once the frame is handed to the transport.
TEST(ProducerAcks, AcksNoneTier2BatchesNeverOccupyInFlightSlotsRegression) {
    auto impl = std::make_shared<ProducerImpl>(noneCfg(/*maxInFlight=*/2));
    Capture cap;
    const auto sink = cap.fn();
    constexpr int kBatches = 200;
    std::vector<std::future<int64_t>> futs;
    futs.reserve(kBatches);
    for (int i = 0; i < kBatches; ++i) {
        rune::client::BatchBuilder bb(impl, 1, 0);
        bb.add(kVal);
        bb.add(kVal);
        futs.push_back(bb.send());
    }
    pump(*impl, sink);
    ASSERT_EQ(cap.frames.size(), static_cast<std::size_t>(kBatches))
        << "Tier-2 under acks=None must not be capped by maxInFlight";
    EXPECT_FALSE(impl->hasInFlight());
    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(cap.frames[i].corrId, 0);
        // F5: acks=None batches are non-idempotent — baseSequence is always the
        // "unset" sentinel -1, never a real (tracked) partition sequence.
        EXPECT_EQ(cap.frames[i].baseSequence, -1);
        ASSERT_EQ(futs[i].wait_for(std::chrono::seconds(0)), std::future_status::ready) << i;
        EXPECT_EQ(futs[i].get(), -1);
    }
    // Far past produce + delivery timeouts: nothing is resent or failed.
    const int64_t late = kNow + 60'000LL * 1'000'000LL;
    EXPECT_EQ(impl->retransmitStalled(late, sink), 0);
    EXPECT_EQ(cap.frames.size(), static_cast<std::size_t>(kBatches)) << "no resends";
    const PartitionState* ps = impl->partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->ackTail, ps->ackHead);
}

// ---------------------------------------------------------------------------
// F5 — producerId uniqueness + acks=None non-idempotence.
//
// Bug: producerId_ was one hardcoded magic-stub constant shared by EVERY
// ProducerImpl instance.  The broker dedups idempotent batches by
// (producerId, partition, baseSequence), so two producers writing the same
// partition with the same producerId silently deduplicated each other's
// records.  acks=None batches also carried a stamped-but-untracked sequence,
// so a lost frame opened a permanent gap nothing could ever detect or heal.
// ---------------------------------------------------------------------------

// (a) Two ProducerImpl instances must draw different, non-negative producerIds
// — neither may be the old shared magic-stub value.
TEST(ProducerSequence, TwoInstancesGetUniqueProducerIdsRegression) {
    ProducerImpl p1(ackCfg(8));
    ProducerImpl p2(ackCfg(8));

    constexpr int64_t kLegacyStubProducerId =
        static_cast<int64_t>(0x4352554E'4543422DULL); // old "RUNECB-" magic stub

    EXPECT_GE(p1.producerId(), 0) << "producerId must be non-negative (idempotent)";
    EXPECT_GE(p2.producerId(), 0) << "producerId must be non-negative (idempotent)";
    EXPECT_NE(p1.producerId(), kLegacyStubProducerId)
        << "must never reissue the old shared stub";
    EXPECT_NE(p2.producerId(), kLegacyStubProducerId)
        << "must never reissue the old shared stub";
    EXPECT_NE(p1.producerId(), p2.producerId())
        << "two live instances must not share a producerId (broker dedup collision)";
}

// (b) Epoch overflow (INT16_MAX) — see below, after the SeqCapture/sendOne
// helpers are declared (TwoInstancesGetUniqueProducerIdsRegression is (a);
// the overflow test EpochOverflowRotatesToFreshProducerIdRegression follows
// CloseWithInFlightBatchesBumpsEpochAndRestartsSequenceRegression et al.).

// (c) acks=None: every batch is encoded producerId=-1 (non-idempotent) and the
// partition's real idempotent sequence is never touched — it stays exactly
// where it was across many batches, so nothing is ever stamped/tracked that a
// lost frame could turn into an undetectable gap.
TEST(ProducerAcks, AcksNoneAlwaysNonIdempotentSequenceNeverAdvancesRegression) {
    ProducerImpl impl(noneCfg(8));
    Capture cap;
    const auto sink = cap.fn();

    constexpr int kBatches = 500;
    for (int i = 0; i < kBatches; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(cap.frames.size(), static_cast<std::size_t>(kBatches));

    const PartitionState* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->nextBaseSequence, 0)
        << "acks=None must never advance the partition's idempotent sequence";

    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(cap.frames[i].producerId, -1)
            << "acks=None batch " << i << " must carry the non-idempotent producerId sentinel";
        EXPECT_EQ(cap.frames[i].baseSequence, -1)
            << "acks=None batch " << i << " must carry the non-idempotent baseSequence sentinel";
    }
}

// (d) Guard: acks=Leader/All must still stamp THIS instance's real producerId
// and advance the partition's idempotent sequence normally (i.e. the acks=None
// fix above must not regress the idempotent path).
TEST(ProducerAcks, AcksLeaderStampsInstanceIdAndAdvancesSequenceGuard) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();

    constexpr int kBatches = 5;
    for (int i = 0; i < kBatches; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(cap.frames.size(), static_cast<std::size_t>(kBatches));

    const PartitionState* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->nextBaseSequence, kBatches)
        << "acks=Leader must advance the idempotent sequence by 1 per single-record batch";

    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(cap.frames[i].producerId, impl.producerId())
            << "acks=Leader batch " << i << " must carry this instance's real producerId";
        EXPECT_GE(cap.frames[i].producerId, 0);
        EXPECT_EQ(cap.frames[i].baseSequence, i)
            << "acks=Leader batch " << i << " must carry the real, advancing sequence";
    }
}

namespace {
void drainAll(ProducerImpl& impl) {
    for (int i = 0; i < 100'000 && impl.drainRing(kNow) > 0; ++i) {}
}
/// Drains every queued record WITHOUT flushing, then flushes them as one batch.
void flushAsOneBatch(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    drainAll(impl);
    impl.submitFlush();
    drainAll(impl);
    impl.flushBatches(kNow, 42, sink);
}
} // namespace

// Regression (Review Focus 3): the ack ring stored only ack-wanting records, so a
// batch [void, cb, void, cb] resolved the callbacks with base+0 and base+1.
TEST(ProducerAcks, MixedVoidAndCallbackRecordsGetTheirOwnOffsetsRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/1'000'000'000));
    Capture cap;
    const auto sink = cap.fn();
    std::vector<int64_t> got;
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));           // record 0
    const int64_t h1 = impl.registerCallback([&](int64_t off) { got.push_back(off); });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h1));           // record 1
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));           // record 2
    const int64_t h3 = impl.registerCallback([&](int64_t off) { got.push_back(off); });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h3));           // record 3
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    ASSERT_EQ(cap.frames[0].records, 4);
    impl.onProduceResponse(cap.frames[0].corrId, 100);
    EXPECT_EQ(got, (std::vector<int64_t>{101, 103}));
}

// Review Focus 4: a duplicate ack (retransmit dedup / second publisher) fires once.
TEST(ProducerAcks, DuplicateBatchAckFiresCallbacksOnceRegression) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    int fired = 0;
    const int64_t h = impl.registerCallback([&](int64_t) { ++fired; });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.frames[0].corrId, 7);
    impl.onProduceResponse(cap.frames[0].corrId, 7);
    EXPECT_EQ(fired, 1);
}

TEST(ProducerAcks, BatchAckReportsPartitionSequenceAndCount) {
    ProducerImpl impl(ackCfg(8, 1'000'000'000));
    std::vector<rune::client::BatchAck> acks;
    impl.onBatchAck([&](const rune::client::BatchAck& a) { acks.push_back(a); });
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    for (int i = 0; i < 2; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    impl.onProduceResponse(cap.frames[0].corrId, 500);
    impl.onProduceResponse(cap.frames[1].corrId, 503);
    ASSERT_EQ(acks.size(), 2u);
    EXPECT_EQ(acks[0].topicId, 4);
    EXPECT_EQ(acks[0].partition, 2);
    EXPECT_EQ(acks[0].baseOffset, 500);
    EXPECT_EQ(acks[0].baseSequence, 0);
    EXPECT_EQ(acks[0].recordCount, 3);
    EXPECT_EQ(acks[0].errorCode, 0);
    EXPECT_EQ(acks[1].baseSequence, 3);
    EXPECT_EQ(acks[1].recordCount, 2);
}

TEST(ProducerAcks, BrokerErrorFailsEveryRecordAndReportsTheCode) {
    ProducerImpl impl(ackCfg(8, 1'000'000'000));
    std::vector<rune::client::BatchAck> acks;
    impl.onBatchAck([&](const rune::client::BatchAck& a) { acks.push_back(a); });
    Capture cap;
    const auto sink = cap.fn();
    std::vector<int64_t> got;
    for (int i = 0; i < 2; ++i) {
        const int64_t h = impl.registerCallback([&](int64_t off) { got.push_back(off); });
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    }
    flushAsOneBatch(impl, sink);
    impl.onProduceResponse(cap.frames[0].corrId, /*-MinIsrViolation*/ -10);
    EXPECT_EQ(got, (std::vector<int64_t>{-1, -1}));
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].errorCode, 10);
    EXPECT_EQ(acks[0].baseOffset, -1);
}

TEST(ProducerAcks, CloseFailsRecordsStillInTheRing) {
    ProducerImpl impl(ackCfg(8));
    auto [h, fut] = impl.registerFuture();
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));  // never drained
    impl.failAllPending("Client closed");
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_THROW(fut.get(), std::runtime_error);
}

TEST(ProducerAcks, DeliveryTimeoutFailsNeverSentRecordsAndDropsThem) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    int64_t got = 0;
    const int64_t h = impl.registerCallback([&](int64_t off) { got = off; });
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, h));
    drainAll(impl);
    impl.flushBatches(kNow, /*sessionId=*/0, sink);  // no session: nothing is sent
    ASSERT_TRUE(cap.frames.empty());
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{kNow + 10'000'000'000LL}});  // deliveryTimeoutMs = 10 s
    EXPECT_EQ(got, -1);
    impl.flushBatches(kNow + 10'000'000'001LL, 42, sink);
    EXPECT_TRUE(cap.frames.empty()) << "expired records are dropped, never sent after failing";
}

// Tier-2 counterpart: a Tier-2 batch that never left the partition queue (no
// session) past the delivery deadline is failed AND dropped (the per-record map
// deadline that used to time it out no longer exists).
TEST(ProducerAcks, DeliveryTimeoutFailsNeverSentTier2BatchesAndDropsThemRegression) {
    auto impl = std::make_shared<ProducerImpl>(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    rune::client::BatchBuilder bb(impl, 1, 0);
    bb.add(kVal);
    auto fut = bb.send();
    drainAll(*impl);
    impl->flushBatches(kNow, /*sessionId=*/0, sink);
    ASSERT_TRUE(cap.frames.empty());
    impl->sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{kNow + 10'000'000'000LL}});
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_THROW(fut.get(), std::runtime_error);
    impl->flushBatches(kNow + 10'000'000'001LL, 42, sink);
    EXPECT_TRUE(cap.frames.empty());
}

// ---------------------------------------------------------------------------
// Unresolved sequences (Kafka semantics): failing a SEQUENCED batch to the user
// while its broker outcome is unknown (or known-rejected) leaves a same-epoch
// sequence gap the broker rejects forever (ProducerStateManager: same epoch ->
// baseSequence must equal lastSequence+1).  The producer bumps its epoch and
// restarts every partition at baseSequence 0 (a higher epoch must start at 0 and
// resets the broker's state).
// ---------------------------------------------------------------------------
namespace {
struct SeqFrame {
    int64_t corrId;
    int16_t epoch;
    int32_t baseSequence;
    int32_t partition;
};
struct SeqCapture {
    std::vector<SeqFrame> frames;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({v->correlationId, r.producerEpoch(), r.baseSequence(), p});
            return true;
        };
    }
};
void sendOne(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int32_t partition,
             int64_t now = kNow) {
    ASSERT_TRUE(impl.submitSendRecord(1, partition, Key(uint64_t{1}), kVal, false, 0));
    pump(impl, sink, now);
}
} // namespace

// (b) Epoch overflow (INT16_MAX) must rotate to a FRESH unique producerId —
// never producerId_+1 — and restart epoch at 0 with every partition's sequence
// back at 0.  Uses the debugSetEpochForTest() seam to reach INT16_MAX without
// actually sending 32767 batches, then drives the SAME delivery-timeout path
// as DeliveryTimeoutBumpsEpochAndRestartsSequenceRegression below.
TEST(ProducerSequence, EpochOverflowRotatesToFreshProducerIdRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();

    const int64_t oldId = impl.producerId();
    impl.debugSetEpochForTest(INT16_MAX);

    sendOne(impl, sink, 0); // batch is stamped with the current (INT16_MAX) epoch
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(cap.frames[0].epoch, INT16_MAX);

    // Past deliveryTimeoutMs (10s in ackCfg): retransmitStalled fails the
    // unresolved batch, triggering the epoch-overflow rotation.
    const int64_t late = kNow + 10'000LL * 1'000'000LL;
    impl.retransmitStalled(late, sink);
    EXPECT_FALSE(impl.hasInFlight());

    EXPECT_EQ(impl.producerEpoch(), 0) << "epoch must restart at 0 after overflow";
    EXPECT_GE(impl.producerId(), 0) << "the rotated id must still be non-negative";
    EXPECT_NE(impl.producerId(), oldId) << "overflow must draw a NEW id";
    EXPECT_NE(impl.producerId(), oldId + 1)
        << "must NOT rotate via +1 (as likely to collide as the original id)";

    // The next batch on this (now-reset) partition must start at sequence 0.
    sendOne(impl, sink, 0, late);
    ASSERT_EQ(cap.frames.size(), 2u);
    EXPECT_EQ(cap.frames[1].epoch, 0);
    EXPECT_EQ(cap.frames[1].baseSequence, 0);
}

TEST(ProducerSequence, CloseWithInFlightBatchesBumpsEpochAndRestartsSequenceRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 1);
    ASSERT_EQ(cap.frames.size(), 3u);
    const int16_t e0 = cap.frames[0].epoch;
    EXPECT_EQ(cap.frames[1].baseSequence, 1);
    impl.failAllPending("Redirect reconnect failed");  // releases 3 in-flight batches
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 1);
    ASSERT_EQ(cap.frames.size(), 5u);
    EXPECT_EQ(cap.frames[3].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[3].baseSequence, 0);
    EXPECT_EQ(cap.frames[4].epoch, static_cast<int16_t>(e0 + 1)) << "one bump for the whole close";
    EXPECT_EQ(cap.frames[4].baseSequence, 0);
}

TEST(ProducerSequence, DeliveryTimeoutBumpsEpochAndRestartsSequenceRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 0);   // two batches, both time out
    sendOne(impl, sink, 1);   // other partition, acked fine
    ASSERT_EQ(cap.frames.size(), 3u);
    const int16_t e0 = cap.frames[0].epoch;
    impl.onProduceResponse(cap.frames[2].corrId, 7);
    const int64_t late = kNow + 10'000LL * 1'000'000LL;  // deliveryTimeoutMs = 10 s
    impl.retransmitStalled(late, sink);
    EXPECT_FALSE(impl.hasInFlight());
    sendOne(impl, sink, 0, late);
    sendOne(impl, sink, 1, late);
    ASSERT_EQ(cap.frames.size(), 5u);
    EXPECT_EQ(cap.frames[3].epoch, static_cast<int16_t>(e0 + 1)) << "two timeouts, one bump";
    EXPECT_EQ(cap.frames[3].baseSequence, 0);
    EXPECT_EQ(cap.frames[4].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[4].baseSequence, 0) << "every partition restarts at 0";
}

TEST(ProducerSequence, AckedBatchesKeepEpochAndSequenceRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 4; ++i) {
        sendOne(impl, sink, 0);
        impl.onProduceResponse(cap.frames.back().corrId, i);
        impl.onProduceResponse(cap.frames.back().corrId, i);  // duplicate ack: ignored
    }
    ASSERT_EQ(cap.frames.size(), 4u);
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(cap.frames[i].epoch, cap.frames[0].epoch);
        EXPECT_EQ(cap.frames[i].baseSequence, i);
    }
}

// A broker-rejected batch also consumed sequences it never wrote: bump once;
// the later same-epoch batches it strands (rejected out-of-sequence) must not
// bump again.
TEST(ProducerSequence, BrokerRejectionBumpsEpochOnceRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 0);
    const int16_t e0 = cap.frames[0].epoch;
    impl.onProduceResponse(cap.frames[0].corrId, /*-MinIsrViolation*/ -10);
    impl.onProduceResponse(cap.frames[1].corrId, /*-OutOfOrderSequence (any)*/ -9);
    sendOne(impl, sink, 0);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(cap.frames[2].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[2].baseSequence, 0);
}

// ---------------------------------------------------------------------------
// F8c C0: flush() reaches the duty thread only through the producer ring (the
// cross-thread std::atomic<bool> globalFlushPending_ latch is gone).  A flush
// requested while the ring is FULL must still flush every record accepted
// before it — never be silently lost.
// ---------------------------------------------------------------------------
namespace {
constexpr int32_t kTinyRing = 4 * 1024;
/// Fills the producer ring (no drain) with void records; returns how many fit.
int fillRing(ProducerImpl& impl) {
    int n = 0;
    while (impl.submitSendRecord(1, 0, Key(uint64_t{7}), kVal, false, 0)) ++n;
    return n;
}
} // namespace

// App thread (Thread mode: not the duty thread): flush() with a full ring waits
// for ring room instead of dropping the request.  Deterministic: the flusher
// either has written its token or is still retrying when the duty side drains;
// join() then orders it, and the next pass must emit ONE batch of every record.
TEST(ProducerFlushRing, AppThreadFlushOnFullRingIsNotLostRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/1'000'000'000), kTinyRing);  // linger never expires
    Capture cap;
    const auto sink = cap.fn();
    const int n = fillRing(impl);
    ASSERT_GT(n, 4);
    std::thread flusher([&] { impl.submitFlush(); });  // ring full: cannot write yet
    drainAll(impl);                                    // duty side makes room
    flusher.join();                                    // token is in the ring now
    drainAll(impl);
    impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u) << "flush() on a full ring was lost";
    EXPECT_EQ(cap.frames[0].records, n);
}

// Duty thread (a flush() from an ack/batch-ack callback) with a full ring: it
// cannot wait for room (it IS the consumer), so the request is kept in
// duty-owned state and the next flushBatches() pass flushes the builders.
TEST(ProducerFlushRing, DutyThreadFlushOnFullRingIsNotLostRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/1'000'000'000), kTinyRing);
    impl.setPumpFn(nullptr, nullptr, std::this_thread::get_id());  // this thread = duty thread
    Capture cap;
    const auto sink = cap.fn();
    const int n = fillRing(impl);
    ASSERT_GT(n, 4);
    impl.submitFlush();  // ring full, on the duty thread: must neither block nor be lost
    drainAll(impl);
    impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u) << "duty-thread flush() on a full ring was lost";
    EXPECT_EQ(cap.frames[0].records, n);
    // Consumed by that one pass: a later record lingers again (no stuck flag).
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{7}), kVal, false, 0));
    drainAll(impl);
    impl.flushBatches(kNow, 42, sink);
    EXPECT_EQ(cap.frames.size(), 1u) << "the duty-thread flush request must be consumed once";
}
