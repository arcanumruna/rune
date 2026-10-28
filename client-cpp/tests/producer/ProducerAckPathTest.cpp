// tests/producer/ProducerAckPathTest.cpp — in-flight ring + ack path on a bare
// ProducerImpl (no Aeron, injected clock, bounded pumps).
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

using rune::client::Acks;
using rune::client::AckToken;
using rune::client::Key;
using rune::client::OpResult;
using rune::client::OpStatus;
using test_support::AckLedger;
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

// A producer that only occupies a hub ordinal (never builds a Tier-2 batch):
// the default pool preallocates tier2Slots(64) x 2 x 64 KiB = 8 MiB per
// producer, so the 4096-producer ordinal-exhaustion tests reserved 32 GiB and
// TSan's allocator aborted (16 GiB cap per size class).  One small slot keeps
// the filler cheap; the producers under test keep the full config.
ProducerConfig fillerCfg(int32_t maxInFlight) {
    ProducerConfig c = ackCfg(maxInFlight);
    c.tier2Slots     = 1;
    c.tier2SlotBytes = 4 * 1024;
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
    impl.onProduceResponse(cap.frames[0].corrId, 10, 0);
    impl.onProduceResponse(cap.frames[1].corrId, 20, 0);
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
    impl.onProduceResponse(cap.frames[1].corrId, 11, 0);   // B acks first
    pump(impl, sink);
    EXPECT_EQ(cap.frames.size(), 2u) << "window stays full until the oldest (A) acks";
    impl.onProduceResponse(cap.frames[0].corrId, 10, 0);   // A acks
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(batchcorr::seq(cap.frames[2].corrId), 2u);
}

// F8c C7: a released producer's ordinal (its slot in the hub, encoded into its
// batch ids) is a tombstone — a late ProduceResponse for it is dropped, never
// routed to another producer — and it is reused only once every fresh ordinal
// is taken, oldest first.
TEST(ProducerInFlight, ReleasedOrdinalReusedOnlyAfterFreshOnesRegression) {
    ProducerHub hub;
    std::vector<std::unique_ptr<ProducerImpl>> impls;
    for (uint32_t i = 0; i <= batchcorr::kMaxOrdinal; ++i) {
        impls.push_back(std::make_unique<ProducerImpl>(fillerCfg(1), 1024));
        hub.add(impls.back().get());
        ASSERT_EQ(impls.back()->ordinal(), i);
    }
    EXPECT_THROW(hub.add(impls.front().get()), std::length_error) << "every ordinal taken";
    hub.remove(impls[7].get());
    hub.remove(impls[3].get());
    EXPECT_EQ(hub.size(), static_cast<std::size_t>(batchcorr::kMaxOrdinal) + 1 - 2);
    hub.onProduceResponse(batchcorr::make(7, 0, 0), 100, 0);  // a late ack for the released one: dropped
    auto a = std::make_unique<ProducerImpl>(fillerCfg(1), 1024);
    auto b = std::make_unique<ProducerImpl>(fillerCfg(1), 1024);
    hub.add(a.get());
    hub.add(b.get());
    EXPECT_EQ(a->ordinal(), 7u) << "the oldest released ordinal first";
    EXPECT_EQ(b->ordinal(), 3u);
}

// F8c C7 fix round 1 (I-1): an ordinal reused by a new producer must never
// match a late ProduceResponse of the producer that held it before: the
// ordinal's generation is folded into the sequence, so the old corrId is
// dropped and the new producer's record completes only from its own response
// (with its own offset).  RED (pre-fix): both producers' first batch on slot 0
// was batchcorr::make(ord, 0, 0) — the stale response completed the new
// producer's record Ok with the old producer's offset.
TEST(ProducerInFlight, ReusedOrdinalDropsStaleResponseOfPreviousProducerRegression) {
    ProducerHub hub;
    std::vector<std::unique_ptr<ProducerImpl>> impls;
    for (uint32_t i = 0; i <= batchcorr::kMaxOrdinal; ++i) {  // [9] sends below: full config
        impls.push_back(std::make_unique<ProducerImpl>(i == 9 ? ackCfg(4) : fillerCfg(4), 1024));
        hub.add(impls.back().get());
    }
    // One batch of `p` on slot 0, acked by its own response unless `ack` is false.
    rune::client::Token next = 1;
    auto sendOne = [&](ProducerImpl& p, AckLedger& L, bool ack, rune::client::Token& tok) -> int64_t {
        tok = next++;
        Capture cap;
        EXPECT_TRUE(p.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(tok)));
        pump(p, cap.fn());
        EXPECT_EQ(cap.frames.size(), 1u);
        if (cap.frames.empty()) return 0;
        (void)L;
        return cap.frames[0].corrId;
    };

    // (i) the previous producer was long-lived: its sequences are already past
    //     2^26 (the round-1 generation window) when it is released.
    ProducerImpl& old = *impls[9];
    old.testRebaseSequences((uint64_t{1} << 26) + 5);
    AckLedger Lold;
    Lold.attach(old);
    rune::client::Token oldTok = 0;
    const int64_t staleCorr = sendOne(old, Lold, false, oldTok);  // its response is still "in the network"
    hub.remove(&old);

    std::unique_ptr<ProducerImpl> cur = std::make_unique<ProducerImpl>(ackCfg(4), 1024);
    AckLedger L;
    L.attach(*cur);
    hub.add(cur.get());
    ASSERT_EQ(cur->ordinal(), 9u) << "the released ordinal is reused";
    for (int b = 0; b < 8; ++b) {  // round 1: batch 5 of the next generation equalled the stale id
        rune::client::Token t = 0;
        const int64_t c = sendOne(*cur, L, true, t);
        EXPECT_NE(c, staleCorr) << "batch " << b;
        hub.onProduceResponse(staleCorr, 111, 0);  // the previous producer's late answer
        EXPECT_EQ(L.times(t), 0) << "a stale response completed another producer's record (batch " << b << ")";
        hub.onProduceResponse(c, 222 + b, 0);
        ASSERT_EQ(L.times(t), 1);
        EXPECT_EQ(L.status(t), OpStatus::Ok);
        EXPECT_EQ(L.value(t), 222 + b) << "its own offset";
    }

    // (ii) the same ordinal reused 70 times (> the round-1 64-generation wrap)
    //      while the very first stale response is still outstanding.
    std::vector<std::unique_ptr<ProducerImpl>> gone;
    hub.remove(cur.get());
    gone.push_back(std::move(cur));
    cur = std::make_unique<ProducerImpl>(ackCfg(4), 1024);  // X: one batch, never answered
    AckLedger Lx;
    Lx.attach(*cur);
    hub.add(cur.get());
    rune::client::Token xTok = 0;
    const int64_t stale2 = sendOne(*cur, Lx, false, xTok);
    std::vector<std::unique_ptr<AckLedger>> ledgers;  // outlive their producers' last handler call
    for (int r = 0; r < 70; ++r) {  // ONE reuse per iteration
        hub.remove(cur.get());
        gone.push_back(std::move(cur));
        cur = std::make_unique<ProducerImpl>(ackCfg(4), 1024);
        ledgers.push_back(std::make_unique<AckLedger>());
        AckLedger& Lr = *ledgers.back();
        Lr.attach(*cur);
        hub.add(cur.get());
        ASSERT_EQ(cur->ordinal(), 9u);
        rune::client::Token t = 0;
        const int64_t c = sendOne(*cur, Lr, true, t);
        EXPECT_NE(c, staleCorr) << "reuse " << r;
        EXPECT_NE(c, stale2) << "reuse " << r;
        hub.onProduceResponse(staleCorr, 111, 0);
        hub.onProduceResponse(stale2, 333, 0);  // X's late answer
        EXPECT_EQ(Lr.times(t), 0) << "reuse " << r << ": a stale response completed a new record";
        hub.onProduceResponse(c, 7, 0);
        EXPECT_EQ(Lr.times(t), 1);
    }
    EXPECT_EQ(Lold.times(oldTok), 0) << "the original record was never completed by a stranger";
    EXPECT_EQ(Lx.times(xTok), 0);
}

TEST(ProducerInFlight, HubRoutesAcksByOrdinal) {
    ProducerHub hub;
    auto a = std::make_unique<ProducerImpl>(ackCfg(4));  // F8c C7: the bare hub holds raw pointers
    auto b = std::make_unique<ProducerImpl>(ackCfg(4));
    hub.add(a.get());
    hub.add(b.get());
    EXPECT_EQ(a->ordinal(), 0u);
    EXPECT_EQ(b->ordinal(), 1u);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(b->submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    hub.drainAndFlush(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(batchcorr::ordinal(cap.frames[0].corrId), 1u);
    hub.onProduceResponse(cap.frames[0].corrId, 5, 0);
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
    AckLedger L;
    L.attach(impl);
    for (int r = 0; r < 2; ++r) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(r)), kVal, true, AckLedger::hi(r + 1)));
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
        ASSERT_EQ(L.times(r + 1), 1) << "record " << r << " must fail exactly once";
        EXPECT_EQ(L.value(r + 1), -1);
        EXPECT_EQ(L.status(r + 1), OpStatus::DeliveryFailed);
    }

    // The broker's late OK ack for the given-up batch arrives.
    impl.onProduceResponse(wire, 100, 0);
    for (int r = 0; r < 2; ++r) {
        EXPECT_EQ(L.times(r + 1), 1) << "late OK ack must not complete record " << r;
        EXPECT_EQ(L.value(r + 1), -1);
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
    AckLedger L;
    L.attach(impl);
    rune::client::Token next = 1;
    auto sendBatch = [&](int records) {
        for (int i = 0; i < records; ++i) {
            ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), kVal, true, AckLedger::hi(next++)));
        }
        pump(impl, sink);            // linger holds the records in one builder
        impl.submitFlush();
        pump(impl, sink);            // one frame for all of them
    };

    // Seed the partition so its state exists.
    sendBatch(1);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.frames[0].corrId, 0, 0);
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

        impl.onProduceResponse(cap.frames[f0 + 1].corrId, 10, 0);   // B acks first
        ASSERT_EQ(ps->ackTail, aStart) << "ackTail must stay at live A's slice, cycle " << c;
        impl.onProduceResponse(cap.frames[f0].corrId, 5, 0);        // then A
        ASSERT_EQ(ps->ackTail, ps->ackHead) << "cycle " << c;
        ASSERT_EQ(ps->ackRing.size(), ringCap) << "ack ring grew at cycle " << c;
    }
    long completed = 0;
    for (const auto& r : L.recs) completed += r.value >= 0 ? 1 : 0;
    EXPECT_EQ(completed, 1 + 3L * kCycles);
    EXPECT_TRUE(L.noneDuplicated());
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
    AckLedger L;
    L.attach(impl);
    rune::client::Token next = 1;
    constexpr int kCycles = 6'000;  // 3 ack records/cycle -> 18'000 > kAckRingCapacity
    for (int c = 0; c < kCycles; ++c) {
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), kVal, true, AckLedger::hi(next++)));
        }
        pump(impl, sink);
        const PartitionState* ps = impl.partitionState(1, 0);
        ASSERT_NE(ps, nullptr);
        ASSERT_EQ(ps->ackTail, ps->ackHead) << "acks=None retained ack entries, cycle " << c;
        ASSERT_EQ(ps->ackRing.size(), static_cast<std::size_t>(kAckRingCapacity))
            << "ack ring grew at cycle " << c;
    }
    long nonMinusOne = 0;
    for (const auto& r : L.recs) nonMinusOne += (r.value != -1 || r.status != OpStatus::Ok) ? 1 : 0;
    EXPECT_EQ(static_cast<long>(L.recs.size()), 3L * kCycles);
    EXPECT_TRUE(L.noneDuplicated());
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
    constexpr int kBatches = 200;
    ProducerConfig cfg = noneCfg(/*maxInFlight=*/2);
    cfg.tier2Slots = kBatches;  // all 200 queued unsent at once: the pool bounds that (Task 3)
    auto impl = std::make_unique<ProducerImpl>(cfg);
    Capture cap;
    const auto sink = cap.fn();
    AckLedger L;
    L.attach(*impl);
    for (int i = 0; i < kBatches; ++i) {
        rune::client::BatchBuilder bb(impl.get(), 1, 0);
        bb.add(kVal);
        bb.add(kVal);
        ASSERT_EQ(bb.send(AckToken{static_cast<rune::client::Token>(i + 1)}), OpResult::Accepted);
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
        ASSERT_EQ(L.times(i + 1), 1) << i;
        EXPECT_EQ(L.value(i + 1), -1);
        EXPECT_EQ(L.status(i + 1), OpStatus::Ok);
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

// (a) Phase 2b F (FD4): the client no longer draws ids.  A bare rig (no coordinator link) starts with the
// fixed rig id - never the old shared magic stub; distinct live producers get distinct BROKER ids
// (ProducerId.* / ProducerIdBrokerRegression).
TEST(ProducerSequence, BareRigsUseTheRigIdNeverTheLegacyStubRegression) {
    ProducerImpl p1(ackCfg(8));
    ProducerImpl p2(ackCfg(8));

    constexpr int64_t kLegacyStubProducerId =
        static_cast<int64_t>(0x4352554E'4543422DULL); // old "RUNECB-" magic stub

    EXPECT_EQ(p1.producerId(), kRigProducerId);
    EXPECT_GE(p1.producerId(), 0) << "producerId must be non-negative (idempotent)";
    EXPECT_NE(p1.producerId(), kLegacyStubProducerId) << "must never reissue the old shared stub";
    EXPECT_EQ(p2.producerId(), kRigProducerId) << "rigs share the rig id: no client-drawn randomness";
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
    AckLedger L;
    L.attach(impl);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));                  // record 0
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));    // record 1
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));                  // record 2
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(3)));    // record 3
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    ASSERT_EQ(cap.frames[0].records, 4);
    impl.onProduceResponse(cap.frames[0].corrId, 100, 0);
    ASSERT_EQ(L.recs.size(), 2u);
    EXPECT_EQ(L.value(1), 101);
    EXPECT_EQ(L.value(3), 103);
}

// Review Focus 4: a duplicate ack (retransmit dedup / second publisher) fires once.
TEST(ProducerAcks, DuplicateBatchAckFiresCallbacksOnceRegression) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    AckLedger L;
    L.attach(impl);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.frames[0].corrId, 7, 0);
    impl.onProduceResponse(cap.frames[0].corrId, 7, 0);
    EXPECT_EQ(L.times(1), 1);
}

TEST(ProducerAcks, BatchAckReportsPartitionSequenceAndCount) {
    ProducerImpl impl(ackCfg(8, 1'000'000'000));
    AckLedger L;
    L.attachBatches(impl);
    const auto& acks = L.batchAcks;
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    for (int i = 0; i < 2; ++i) ASSERT_TRUE(impl.submitSendRecord(4, 2, Key(uint64_t{9}), kVal, false, 0));
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    impl.onProduceResponse(cap.frames[0].corrId, 500, 0);
    impl.onProduceResponse(cap.frames[1].corrId, 503, 0);
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
    AckLedger L;
    L.attach(impl);
    L.attachBatches(impl);
    const auto& acks = L.batchAcks;
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(i + 1)));
    }
    flushAsOneBatch(impl, sink);
    impl.onProduceResponse(cap.frames[0].corrId, -1, /*StorageError (outcome unknown; MinIsrViolation is retriable since re-review 1 N-e)*/ 26);
    ASSERT_EQ(L.recs.size(), 2u);
    for (int i = 1; i <= 2; ++i) {
        EXPECT_EQ(L.value(i), -1);
        EXPECT_EQ(L.status(i), OpStatus::BrokerError);
        EXPECT_EQ(L.last.at(i).ec, 26);
    }
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].errorCode, 26);
    EXPECT_EQ(acks[0].baseOffset, -1);
}

// No-hang wave: the broker answers a Produce it could not decode with
// ProduceResponse{InvalidRequest (39), partition -1}.  Every record of the
// batch completes BrokerError with that code at once (never left pending, never
// re-sent: the same bytes fail the same way).
TEST(ProducerAcksRegression, InvalidRequestAnswerFailsTheBatchAtOnceRegression) {
    ProducerImpl impl(ackCfg(8, 1'000'000'000));
    AckLedger L;
    L.attach(impl);
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(i + 1)));
    }
    flushAsOneBatch(impl, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    constexpr int16_t kInvalidRequest = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    impl.onProduceResponse(cap.frames[0].corrId, 0, kInvalidRequest);
    ASSERT_EQ(L.recs.size(), 2u);
    for (int i = 1; i <= 2; ++i) {
        EXPECT_EQ(L.times(i), 1);
        EXPECT_EQ(L.status(i), OpStatus::BrokerError);
        EXPECT_EQ(L.last.at(i).ec, kInvalidRequest);
    }
    pump(impl, sink);
    EXPECT_EQ(cap.frames.size(), 1u) << "a batch the broker could not decode was re-sent";
}

TEST(ProducerAcks, CloseFailsRecordsStillInTheRing) {
    ProducerImpl impl(ackCfg(8));
    AckLedger L;
    L.attach(impl);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));  // never drained
    impl.failAllPending(OpStatus::Closed);
    ASSERT_EQ(L.times(1), 1);
    EXPECT_EQ(L.status(1), OpStatus::Closed);
}

TEST(ProducerAcks, DeliveryTimeoutFailsNeverSentRecordsAndDropsThem) {
    ProducerImpl impl(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    AckLedger L;
    L.attach(impl);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    drainAll(impl);
    impl.flushBatches(kNow, /*sessionId=*/0, sink);  // no session: nothing is sent
    ASSERT_TRUE(cap.frames.empty());
    impl.sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{kNow + 10'000'000'000LL}});  // deliveryTimeoutMs = 10 s
    ASSERT_EQ(L.times(1), 1);
    EXPECT_EQ(L.value(1), -1);
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
    impl.flushBatches(kNow + 10'000'000'001LL, 42, sink);
    EXPECT_TRUE(cap.frames.empty()) << "expired records are dropped, never sent after failing";
}

// Tier-2 counterpart: a Tier-2 batch that never left the partition queue (no
// session) past the delivery deadline is failed AND dropped (the per-record map
// deadline that used to time it out no longer exists).
TEST(ProducerAcks, DeliveryTimeoutFailsNeverSentTier2BatchesAndDropsThemRegression) {
    auto impl = std::make_unique<ProducerImpl>(ackCfg(8));
    Capture cap;
    const auto sink = cap.fn();
    AckLedger L;
    L.attach(*impl);
    rune::client::BatchBuilder bb(impl.get(), 1, 0);
    bb.add(kVal);
    ASSERT_EQ(bb.send(AckToken{1}), OpResult::Accepted);
    drainAll(*impl);
    impl->flushBatches(kNow, /*sessionId=*/0, sink);
    ASSERT_TRUE(cap.frames.empty());
    impl->sweepTimedOut(std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{kNow + 10'000'000'000LL}});
    ASSERT_EQ(L.times(1), 1);
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
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
    int64_t producerId = -1;
    std::vector<uint8_t> bytes;  ///< the whole Produce frame as offered
};
// EOS-DUP review I3: every captured frame's FRAME identity (what the broker
// dedups on) must equal its CRC-covered batch header's — asserted on EVERY frame,
// so the epoch-bump / rotation / retransmit tests below pin frame == header
// across every path that changes the identity.
struct SeqCapture {
    std::vector<SeqFrame> frames;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            EXPECT_EQ(v->producerId, r.producerId()) << "frame producerId != header (frame " << frames.size() << ")";
            EXPECT_EQ(v->producerEpoch, r.producerEpoch()) << "frame epoch != header (frame " << frames.size() << ")";
            EXPECT_EQ(v->baseSequence, r.baseSequence()) << "frame baseSequence != header (frame " << frames.size() << ")";
            frames.push_back({v->correlationId, r.producerEpoch(), r.baseSequence(), p, r.producerId(),
                              std::vector<uint8_t>(b, b + n)});
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

// (b) Epoch overflow (INT16_MAX) must rotate to a NEW producerId and restart epoch at 0 with every partition's sequence
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
    EXPECT_NE(impl.producerId(), oldId) << "overflow must move to a NEW id";
    // (A bare rig rotates deterministically; a producer wired to a broker asks it for the new id instead -
    // ProducerId.EpochOverflowRequestsANewId.  Nothing here is client-drawn.)

    // The next batch on this (now-reset) partition must start at sequence 0.
    sendOne(impl, sink, 0, late);
    ASSERT_EQ(cap.frames.size(), 2u);
    EXPECT_EQ(cap.frames[1].epoch, 0);
    EXPECT_EQ(cap.frames[1].baseSequence, 0);
}

// EOS follow-ups review: BatchAck::producerId names the identity the batch
// was STAMPED with — across an INT16_MAX rotation the failed batch reports the
// old id and the next batch the fresh one (never the producer's current id
// read at completion time).
TEST(ProducerSequence, BatchAckProducerIdIsTheStampedIdentityAcrossRotationRegression) {
    ProducerImpl impl(ackCfg(8));
    AckLedger L;
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    const int64_t oldId = impl.producerId();
    impl.debugSetEpochForTest(INT16_MAX);
    sendOne(impl, sink, 0);
    ASSERT_EQ(cap.frames.size(), 1u);
    const int64_t late = kNow + 10'000LL * 1'000'000LL;  // past deliveryTimeoutMs: fails + rotates
    impl.retransmitStalled(late, sink);
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].producerId, oldId) << "the failed batch was stamped with the old id";
    EXPECT_EQ(L.batchAcks[0].producerEpoch, INT16_MAX);
    const int64_t newId = impl.producerId();
    ASSERT_NE(newId, oldId);
    sendOne(impl, sink, 0, late);
    ASSERT_EQ(cap.frames.size(), 2u);
    impl.onProduceResponse(cap.frames[1].corrId, 77, 0);
    ASSERT_EQ(L.batchAcks.size(), 2u);
    EXPECT_EQ(L.batchAcks[1].errorCode, 0);
    EXPECT_EQ(L.batchAcks[1].producerId, newId);
    EXPECT_EQ(L.batchAcks[1].producerId, cap.frames[1].producerId) << "== the frame's stamped id";
    EXPECT_EQ(L.batchAcks[1].producerEpoch, 0);
}

// Sequence overflow (broker-agent concern): the per-partition int32 sequence
// counter never overflows.  The client chooses not to wrap (the broker accepts
// the Kafka wrap since 37564ba), so the producer moves the partition to a new epoch at sequence 0
// BEFORE the counter would pass INT32_MAX — and only once the partition's
// in-flight batches drained (a bump with A in flight would fence A's stored
// retransmit, EOS I-4).  A completes Ok, B and C go out at 0 / 1 under
// epoch + 1, every record completes Ok exactly once, no sequence is negative.
// RED (pre-fix): B went out at INT32_MAX and C at a wrapped negative sequence.
TEST(ProducerSequence, SequenceCounterNeverOverflowsInt32Regression) {
    ProducerImpl impl(ackCfg(8));
    AckLedger L;
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    impl.debugSetNextSequenceForTest(1, 0, INT32_MAX - 1);
    const int16_t e0 = impl.producerEpoch();
    sendOne(impl, sink, 0);  // A: INT32_MAX - 1 (the counter then reaches INT32_MAX)
    sendOne(impl, sink, 0);  // B: no room — held while A is in flight
    sendOne(impl, sink, 0);  // C
    for (const auto& f : cap.frames) {
        EXPECT_GE(f.baseSequence, 0) << "a wrapped sequence went out";
    }
    ASSERT_EQ(cap.frames.size(), 1u) << "B/C must wait for A (no bump with A in flight)";
    EXPECT_EQ(cap.frames[0].baseSequence, INT32_MAX - 1);
    EXPECT_EQ(cap.frames[0].epoch, e0);
    EXPECT_EQ(impl.producerEpoch(), e0) << "no bump while A is in flight";

    impl.onProduceResponse(cap.frames[0].corrId, 900, 0);  // A stored + acked
    pump(impl, sink);
    ASSERT_GE(cap.frames.size(), 2u);
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1)) << "one bump, before the overflow";
    int32_t next = 0;
    for (std::size_t i = 1; i < cap.frames.size(); ++i) {
        EXPECT_EQ(cap.frames[i].epoch, static_cast<int16_t>(e0 + 1));
        EXPECT_EQ(cap.frames[i].baseSequence, next) << "restarts at 0, contiguous";
        next = cap.frames[i].baseSequence + 1;
    }
    for (std::size_t i = 1; i < cap.frames.size(); ++i) impl.onProduceResponse(cap.frames[i].corrId, 1000, 0);
    int okRecords = 0;
    for (const auto& a : L.batchAcks) {
        EXPECT_EQ(a.errorCode, 0) << "a record failed across the sequence rollover";
        okRecords += a.recordCount;
    }
    EXPECT_EQ(okRecords, 3) << "every record completes exactly once";
    EXPECT_FALSE(impl.hasInFlight());
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
    impl.failAllPending(OpStatus::SessionLost);  // releases 3 in-flight batches
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
    impl.onProduceResponse(cap.frames[2].corrId, 7, 0);
    const int64_t late = kNow + 10'000LL * 1'000'000LL;  // deliveryTimeoutMs = 10 s
    impl.retransmitStalled(late, sink);
    EXPECT_FALSE(impl.hasInFlight());
    sendOne(impl, sink, 0, late);
    sendOne(impl, sink, 1, late);
    ASSERT_EQ(cap.frames.size(), 5u);
    EXPECT_EQ(cap.frames[3].epoch, static_cast<int16_t>(e0 + 1)) << "two timeouts, one bump";
    EXPECT_EQ(cap.frames[3].baseSequence, 0);
    // EOS follow-ups I-4 (Kafka): the bump moves only the partition with the
    // unresolved sequence; partition 1 keeps its identity and sequence.
    EXPECT_EQ(cap.frames[4].epoch, e0) << "an unaffected partition is not moved";
    EXPECT_EQ(cap.frames[4].baseSequence, 1);
}

TEST(ProducerSequence, AckedBatchesKeepEpochAndSequenceRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 4; ++i) {
        sendOne(impl, sink, 0);
        impl.onProduceResponse(cap.frames.back().corrId, i, 0);
        impl.onProduceResponse(cap.frames.back().corrId, i, 0);  // duplicate ack: ignored
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
    impl.onProduceResponse(cap.frames[0].corrId, -1, /*StorageError (outcome unknown; MinIsrViolation is retriable since re-review 1 N-e)*/ 26);
    impl.onProduceResponse(cap.frames[1].corrId, -1, /*NotPartitionLeader (any error)*/ 9);
    sendOne(impl, sink, 0);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(cap.frames[2].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[2].baseSequence, 0);
}

// ---------------------------------------------------------------------------
// EOS-DUP review I3 / I4: the frame identity equals the header's at a NON-ZERO
// epoch (a fresh producer's epoch 0 equals the Produce default, so an epoch-0
// check cannot catch a missing frame epoch), for Tier-1 AND the Tier-2
// pre-built path, acks=Leader and acks=All.  The epoch is driven up by a real
// broker rejection first.  (Mutation-checked: dropping the frame's
// producerEpoch assignment in either encoder turns this RED.)
// ---------------------------------------------------------------------------
TEST(ProducerSequence, FrameIdentityEqualsHeaderAtNonZeroEpochTier1AndTier2Regression) {
    for (const Acks acks : {Acks::Leader, Acks::All}) {
        SCOPED_TRACE(acks == Acks::All ? "acks=All" : "acks=Leader");
        ProducerConfig cfg = ackCfg(8);
        cfg.acks = acks;
        auto impl = std::make_unique<ProducerImpl>(cfg);
        SeqCapture cap;
        const auto sink = cap.fn();
        sendOne(*impl, sink, 0);
        ASSERT_EQ(cap.frames.size(), 1u);
        impl->onProduceResponse(cap.frames[0].corrId, -1, /*NotPartitionLeader*/ 9);  // -> epoch bump
        ASSERT_EQ(impl->producerEpoch(), 1);
        sendOne(*impl, sink, 0);  // Tier-1 at epoch 1
        {
            rune::client::BatchBuilder bb(impl.get(), 1, 0);  // Tier-2 at epoch 1
            bb.add(kVal);
            bb.add(kVal);
            ASSERT_EQ(bb.send(), OpResult::Accepted);
        }
        pump(*impl, sink);
        ASSERT_EQ(cap.frames.size(), 3u);
        for (std::size_t i = 1; i < 3; ++i) {
            SCOPED_TRACE(i == 1 ? "tier-1" : "tier-2");
            const auto v = rcc::decodeProduce(cap.frames[i].bytes.data(), cap.frames[i].bytes.size());
            ASSERT_TRUE(v.has_value());
            EXPECT_EQ(v->producerEpoch, 1) << "non-zero epoch on the FRAME";
            EXPECT_EQ(cap.frames[i].epoch, 1) << "non-zero epoch in the header";
            EXPECT_EQ(v->producerId, impl->producerId());
        }
        EXPECT_EQ(cap.frames[1].baseSequence, 0);
        EXPECT_EQ(cap.frames[2].baseSequence, 1);  // after the 1-record Tier-1 batch
    }
}

namespace {
constexpr int16_t kOos = static_cast<int16_t>(rune::protocol::client::ErrorCode::OutOfOrderSequence);
/// Two offers of one stored frame: identical but for the attempt tag in the
/// correlationId (EOS follow-ups I-2: every re-send is a new attempt).
::testing::AssertionResult sameFrameNewAttempt(const SeqFrame& a, const SeqFrame& b) {
    if (a.bytes.size() != b.bytes.size()) return ::testing::AssertionFailure() << "sizes differ";
    for (std::size_t i = 0; i < a.bytes.size(); ++i) {
        if (i >= 16 && i < 24) continue;  // MessageHeader.correlationId
        if (a.bytes[i] != b.bytes[i]) return ::testing::AssertionFailure() << "byte " << i << " differs";
    }
    if (batchcorr::base(a.corrId) != batchcorr::base(b.corrId)) return ::testing::AssertionFailure() << "other batch";
    if (batchcorr::attempt(a.corrId) == batchcorr::attempt(b.corrId)) {
        return ::testing::AssertionFailure() << "same attempt tag";
    }
    return ::testing::AssertionSuccess();
}
}  // namespace

// EOS-DUP review M1: a retransmit re-sends the STORED frame — byte for byte but
// for its attempt tag (I-2) — even across another partition's epoch bump in
// between (the in-flight batch keeps the identity it was sent with, on the
// frame and in the header alike; I-4: its partition is not moved at all).
TEST(ProducerSequence, RetransmitResendsTheStoredFrameAcrossAnEpochBumpRegression) {
    ProducerImpl impl(ackCfg(8));
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);  // A: stays in flight
    sendOne(impl, sink, 1);  // B: rejected -> partition 1 unresolved -> its new epoch
    ASSERT_EQ(cap.frames.size(), 2u);
    const int16_t e0 = cap.frames[0].epoch;
    impl.onProduceResponse(cap.frames[1].corrId, -1, /*StorageError (outcome unknown; MinIsrViolation is retriable since re-review 1 N-e)*/ 26);
    ASSERT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1));
    // Past produceTimeoutMs (1 s): A is retransmitted.
    const int64_t t1 = kNow + 1'000LL * 1'000'000LL;
    ASSERT_EQ(impl.retransmitStalled(t1, sink), 1);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_TRUE(sameFrameNewAttempt(cap.frames[0], cap.frames[2])) << "the stored frame";
    EXPECT_EQ(cap.frames[2].epoch, e0);
    EXPECT_EQ(cap.frames[2].baseSequence, 0);
    sendOne(impl, sink, 0, t1);  // partition 0 keeps its identity
    sendOne(impl, sink, 1, t1);  // partition 1 starts its new one at 0
    ASSERT_EQ(cap.frames.size(), 5u);
    EXPECT_EQ(cap.frames[3].epoch, e0);
    EXPECT_EQ(cap.frames[3].baseSequence, 1);
    EXPECT_EQ(cap.frames[4].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[4].baseSequence, 0);
}

// ---------------------------------------------------------------------------
// EOS-OOS (unit level; the end-to-end fault-injection runs are in
// ProducerEosTest.cpp).
// ---------------------------------------------------------------------------


// Kafka rule (C4): OutOfOrderSequence on the next-in-line batch (oldest, and
// nothing acked after its send) fails THAT batch; the batches behind it are
// re-sequenced under a new epoch from 0 and re-sent — not failed.  The
// superseded attempt's own late OOS is ignored.
TEST(ProducerSequence, OutOfOrderOnTheNextInLineBatchFailsItAndResequencesTheRestRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    AckLedger L;
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 0);
    const int16_t e0 = cap.frames[0].epoch;
    impl.onProduceResponse(cap.frames[0].corrId, -1, kOos);
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].errorCode, kOos);
    EXPECT_EQ(L.batchAcks[0].producerEpoch, e0);
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1));
    impl.flushBatches(kNow, 42, sink);  // the follower, re-stamped
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(batchcorr::base(cap.frames[2].corrId), batchcorr::base(cap.frames[1].corrId));
    EXPECT_EQ(cap.frames[2].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[2].baseSequence, 0) << "re-sequenced from 0";
    impl.onProduceResponse(cap.frames[1].corrId, -1, kOos);  // the superseded attempt's late OOS
    EXPECT_EQ(L.batchAcks.size(), 1u) << "ignored";
    impl.onProduceResponse(cap.frames[2].corrId, 500, 0);
    ASSERT_EQ(L.batchAcks.size(), 2u);
    EXPECT_EQ(L.batchAcks[1].errorCode, 0);
    EXPECT_EQ(L.batchAcks[1].producerEpoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(L.batchAcks[1].baseSequence, 0);
    EXPECT_EQ(L.batchAcks[1].producerId, impl.producerId()) << "N-1: the identity names the batch";
    EXPECT_EQ(L.batchAcks[0].producerId, impl.producerId());
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1)) << "one bump";
    EXPECT_FALSE(impl.hasInFlight());
}

// Issue #222: an auth refusal (44..47) is answered before the append: on a batch's ONLY attempt it is a definite
// non-append - no epoch bump, and the sequence is given back.
TEST(ProducerSequence, NotAuthorizedOnAFirstAttemptIsNotAppendedAndKeepsTheSequenceRegression) {
    for (const int16_t code : {int16_t{44}, int16_t{45}, int16_t{46}, int16_t{47}}) {
        SCOPED_TRACE(code);
        ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
        AckLedger L;
        L.attachBatches(impl);
        SeqCapture cap;
        const auto sink = cap.fn();
        sendOne(impl, sink, 0);
        ASSERT_EQ(cap.frames.size(), 1u);
        const int16_t e0 = cap.frames[0].epoch;
        EXPECT_EQ(cap.frames[0].baseSequence, 0);
        impl.onProduceResponse(cap.frames[0].corrId, -1, code);
        ASSERT_EQ(L.batchAcks.size(), 1u);
        EXPECT_EQ(L.batchAcks[0].errorCode, code);
        EXPECT_EQ(L.batchAcks[0].producerEpoch, e0);
        EXPECT_EQ(impl.producerEpoch(), e0) << "no bump";
        EXPECT_FALSE(impl.hasInFlight());
        sendOne(impl, sink, 0);
        ASSERT_EQ(cap.frames.size(), 2u);
        EXPECT_EQ(cap.frames[1].epoch, e0);
        EXPECT_EQ(cap.frames[1].baseSequence, 0) << "the sequence continues, nothing consumed";
        impl.onProduceResponse(cap.frames[1].corrId, 7, 0);
        ASSERT_EQ(L.batchAcks.size(), 2u);
        EXPECT_EQ(L.batchAcks[1].errorCode, 0);
        EXPECT_EQ(L.batchAcks[1].producerEpoch, e0);
        EXPECT_EQ(L.batchAcks[1].baseSequence, 0);
    }
}

// Issue #222: the refused batch is not the newest sequence holder's last: the later in-flight batch meets the gap
// (OutOfOrderSequence) and C3 re-sequences it.
TEST(ProducerSequence, NotAuthorizedAheadOfInFlightBatchesLetsTheGapResequenceThemRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    AckLedger L;
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    sendOne(impl, sink, 0);
    ASSERT_EQ(cap.frames.size(), 2u);
    const int16_t e0 = cap.frames[0].epoch;
    EXPECT_EQ(cap.frames[1].baseSequence, 1);
    impl.onProduceResponse(cap.frames[0].corrId, -1, 45);
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].errorCode, 45);
    EXPECT_EQ(impl.producerEpoch(), e0) << "no bump from the refusal itself";
    impl.onProduceResponse(cap.frames[1].corrId, -1, kOos);
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1));
    impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(cap.frames[2].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames[2].baseSequence, 0);
    impl.onProduceResponse(cap.frames[2].corrId, 9, 0);
    ASSERT_EQ(L.batchAcks.size(), 2u);
    EXPECT_EQ(L.batchAcks[1].errorCode, 0);
    EXPECT_FALSE(impl.hasInFlight());
}

// Issue #222: a refusal of a RETRANSMIT proves nothing about the earlier attempt: outcome unknown, re-identified.
TEST(ProducerSequence, NotAuthorizedOnARetransmitStaysOutcomeUnknownRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    AckLedger L;
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    sendOne(impl, sink, 0);
    const int16_t e0 = cap.frames[0].epoch;
    const int64_t t1 = kNow + 1'000LL * 1'000'000LL;  // past produceTimeoutMs
    ASSERT_EQ(impl.retransmitStalled(t1, sink), 1);
    ASSERT_EQ(cap.frames.size(), 2u);
    impl.onProduceResponse(cap.frames[1].corrId, -1, 45);
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].errorCode, 45);
    EXPECT_FALSE(impl.hasInFlight());
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1));
}

// OOS behind an unresolved older batch: parked (no completion, no bump); the
// older batch is fast-retransmitted ONCE (the gap's frame, not every older
// one); when it resolves the parked batches go out again, in sequence order,
// BEFORE any newer batch of the partition; then everything acks exactly once.
TEST(ProducerSequence, OutOfOrderBehindAnOlderBatchIsParkedAndResentInOrderRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    AckLedger L;
    L.attach(impl);
    L.attachBatches(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) {  // A(seq 0) B(seq 1) C(seq 2), one record each
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(i + 1)));
        pump(impl, sink);
    }
    ASSERT_EQ(cap.frames.size(), 3u);
    const int16_t e0 = cap.frames[0].epoch;
    const int64_t a = cap.frames[0].corrId, b = cap.frames[1].corrId, c = cap.frames[2].corrId;
    impl.onProduceResponse(b, -1, kOos);  // A was lost: B and C meet the gap
    impl.onProduceResponse(c, -1, kOos);
    EXPECT_TRUE(L.recs.empty()) << "nothing completes while parked";
    EXPECT_TRUE(L.batchAcks.empty());
    EXPECT_EQ(impl.producerEpoch(), e0) << "no bump for a non-oldest OOS";
    const PartitionState* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->parkedCount, 2u);
    // Fast retransmit: A (the frame before the gap) is due NOW — not B or C.
    ASSERT_EQ(impl.retransmitStalled(kNow, sink), 1);
    ASSERT_EQ(cap.frames.size(), 4u);
    EXPECT_TRUE(sameFrameNewAttempt(cap.frames[0], cap.frames[3]));
    EXPECT_EQ(impl.retransmitStalled(kNow, sink), 0) << "one fast retransmit, not one per OOS answer";
    // A new record D waits in the builder; then A resolves.
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(4)));
    drainAll(impl);
    impl.onProduceResponse(a, 100, 0);
    EXPECT_TRUE(ps->resendParked);
    impl.flushBatches(kNow, 42, sink);
    ASSERT_EQ(cap.frames.size(), 7u);
    EXPECT_EQ(batchcorr::base(cap.frames[4].corrId), b) << "parked batches first, in sequence order";
    EXPECT_EQ(batchcorr::base(cap.frames[5].corrId), c);
    EXPECT_TRUE(sameFrameNewAttempt(cap.frames[1], cap.frames[4])) << "the stored frame";
    EXPECT_EQ(cap.frames[6].baseSequence, 3) << "then the new batch";
    EXPECT_FALSE(ps->resendParked);
    EXPECT_EQ(ps->parkedCount, 0u);
    impl.onProduceResponse(b, 101, 0);
    impl.onProduceResponse(c, 102, 0);
    impl.onProduceResponse(cap.frames[6].corrId, 103, 0);
    EXPECT_TRUE(L.exactlyOnce(1, 5));
    for (int i = 1; i <= 4; ++i) {
        EXPECT_EQ(L.status(i), OpStatus::Ok);
        EXPECT_EQ(L.value(i), 99 + i);
    }
    ASSERT_EQ(L.batchAcks.size(), 4u);
    for (const auto& ba : L.batchAcks) {
        EXPECT_EQ(ba.errorCode, 0);
        EXPECT_EQ(ba.producerEpoch, e0);
    }
    EXPECT_EQ(impl.producerEpoch(), e0);
    EXPECT_FALSE(impl.hasInFlight());
}

// I-4: a parked batch whose older batch then FAILS by its delivery timeout
// (outcome unknown -> the partition is unresolved; no bump yet: B is in
// flight) is re-sent once it is the oldest; the broker's OOS on it then proves
// the gap (C3): B is re-sequenced under a new epoch and written — not failed.
TEST(ProducerSequence, ParkedBatchBehindATimedOutHeadIsResequencedNotFailedRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    AckLedger L;
    L.attach(impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    pump(impl, sink, kNow);
    const int64_t tB = kNow + 2'000LL * 1'000'000LL;
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(2)));
    pump(impl, sink, tB);
    ASSERT_EQ(cap.frames.size(), 2u);
    const int16_t e0 = cap.frames[0].epoch;
    impl.onProduceResponse(cap.frames[1].corrId, -1, kOos);  // parked behind A
    EXPECT_EQ(L.recs.size(), 0u);
    // A's delivery timeout (10 s) passes; B's does not.
    const int64_t late = kNow + 10'000LL * 1'000'000LL;
    const std::size_t before = cap.frames.size();
    impl.retransmitStalled(late, sink);
    ASSERT_EQ(L.times(1), 1);
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
    EXPECT_EQ(impl.producerEpoch(), e0) << "unresolved: no bump while B is in flight (Kafka)";
    impl.flushBatches(late, 42, sink);  // B is the oldest now: re-sent (stored, old-epoch frame)
    ASSERT_EQ(cap.frames.size(), before + 1) << "B re-sent exactly once (timer or parked resend, not both)";
    EXPECT_EQ(batchcorr::base(cap.frames.back().corrId), batchcorr::base(cap.frames[1].corrId));
    EXPECT_EQ(cap.frames.back().epoch, e0);
    impl.onProduceResponse(cap.frames.back().corrId, -1, kOos);  // the gap is proven
    EXPECT_EQ(L.times(2), 0) << "re-sequenced, not failed";
    EXPECT_EQ(impl.producerEpoch(), static_cast<int16_t>(e0 + 1));
    impl.flushBatches(late, 42, sink);
    ASSERT_EQ(cap.frames.size(), before + 2);
    EXPECT_EQ(cap.frames.back().epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(cap.frames.back().baseSequence, 0);
    impl.onProduceResponse(cap.frames.back().corrId, 300, 0);
    ASSERT_EQ(L.times(2), 1);
    EXPECT_EQ(L.status(2), OpStatus::Ok);
    EXPECT_EQ(L.value(2), 300);
    EXPECT_FALSE(impl.hasInFlight());
}

// EOS-OOS item 2 (unit level): OK with baseOffset -1 = known-old duplicate.
// Tier-1 records (every record of the batch -1, not -1 + i), a Tier-2 handle,
// and the BatchAck (errorCode 0, baseOffset -1, the batch's epoch) — each
// exactly once, no epoch bump, the next batch continues the sequence.
TEST(ProducerAcks, KnownOldDuplicateCompletesOkWithUnknownOffsetRegression) {
    auto impl = std::make_unique<ProducerImpl>(ackCfg(8, 1'000'000'000));
    AckLedger L;
    L.attach(*impl);
    L.attachBatches(*impl);
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(impl->submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(i + 1)));
    }
    flushAsOneBatch(*impl, sink);
    {
        rune::client::BatchBuilder bb(impl.get(), 1, 0);
        bb.add(kVal);
        ASSERT_EQ(bb.send(AckToken{9}), OpResult::Accepted);
    }
    flushAsOneBatch(*impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    const int16_t e0 = cap.frames[0].epoch;
    impl->onProduceResponse(cap.frames[0].corrId, -1, 0);
    impl->onProduceResponse(cap.frames[1].corrId, -1, 0);
    for (const rune::client::Token t : {1, 2, 3, 9}) {
        ASSERT_EQ(L.times(t), 1) << t;
        EXPECT_EQ(L.status(t), OpStatus::Ok) << t;
        EXPECT_EQ(L.value(t), -1) << t;
    }
    ASSERT_EQ(L.batchAcks.size(), 2u);
    for (const auto& ba : L.batchAcks) {
        EXPECT_EQ(ba.errorCode, 0);
        EXPECT_EQ(ba.baseOffset, -1);
        EXPECT_EQ(ba.producerEpoch, e0);
    }
    EXPECT_EQ(impl->producerEpoch(), e0);
    ASSERT_TRUE(impl->submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    flushAsOneBatch(*impl, sink);  // long linger: flushed explicitly
    ASSERT_EQ(cap.frames.size(), 3u);
    EXPECT_EQ(cap.frames[2].epoch, e0);
    EXPECT_EQ(cap.frames[2].baseSequence, 4);
}

// M-1: while a partition has parked batches, NO new batch of it goes out (each
// would only meet the same gap: a wasted OutOfOrderSequence round trip, Kafka
// stops draining a partition with a retry outstanding), and a later OOS behind
// a parked batch never re-sends the gap frame again (the gap is upstream of the
// parked batch, already being retried).  RED (pre-fix): the new batch was sent
// at once and A was fast-retransmitted a second time.
TEST(ProducerSequence, ParkedPartitionHoldsNewBatchesAndRetransmitsTheGapOnceRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) sendOne(impl, sink, 0);  // A B C
    ASSERT_EQ(cap.frames.size(), 3u);
    impl.onProduceResponse(cap.frames[1].corrId, -1, kOos);  // B parks: A is the gap
    ASSERT_EQ(impl.retransmitStalled(kNow, sink), 1);        // A fast-retransmitted
    ASSERT_EQ(cap.frames.size(), 4u);
    sendOne(impl, sink, 0);  // D: held while B is parked
    EXPECT_EQ(cap.frames.size(), 4u) << "no new batch while the partition has parked batches";
    impl.onProduceResponse(cap.frames[2].corrId, -1, kOos);  // C parks behind parked B
    EXPECT_EQ(impl.retransmitStalled(kNow, sink), 0) << "A is not re-sent again for C's OOS";
    EXPECT_EQ(cap.frames.size(), 4u);
}

// N-3: a close (failAllPending) leaves no resend armed for a partition with no
// parked batch left.  RED (pre-fix): resendParked stayed armed.
TEST(ProducerSequence, FailAllPendingDisarmsTheParkedResendRegression) {
    ProducerImpl impl(ackCfg(8, /*lingerUs=*/0));
    SeqCapture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 3; ++i) sendOne(impl, sink, 0);
    impl.onProduceResponse(cap.frames[1].corrId, -1, kOos);
    impl.onProduceResponse(cap.frames[2].corrId, -1, kOos);
    impl.failAllPending(OpStatus::Closed);
    const PartitionState* ps = impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->parkedCount, 0u);
    EXPECT_FALSE(ps->resendParked) << "nothing parked: nothing to re-send";
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
