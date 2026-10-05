// tests/producer/ProducerRouteParkTest.cpp — Task 16 fix round 1: a
// ProduceResponse{NotLeader} is retriable (C1) and a moved route re-offers the
// stored batches in order (M3).  No Aeron: ProducerImpl is driven directly.
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::ProducerImpl;
namespace batchcorr = rune::client::producer::batchcorr;

namespace {
constexpr int64_t kNow = 1'000'000'000;
constexpr int16_t kNotLeader = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);

ProducerConfig ackedCfg() {
    ProducerConfig c;
    c.acks        = Acks::All;
    c.lingerUs    = 0;
    c.batchBytes  = 64 * 1024;
    c.maxInFlight = 8;
    return c;
}

struct Frame {
    int64_t corrId;
    int16_t epoch;
    int32_t baseSequence;
};

// Records every ACCEPTED offer; refuses the offers whose 0-based call ordinal is
// listed in `refuse`.
struct Wire {
    std::vector<Frame> frames;
    std::vector<int>   refuse;
    int                calls = 0;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const int call = calls++;
            for (int r : refuse) if (r == call) return false;
            const auto pv = rune::client::codec::decodeProduce(b, n);
            if (!pv) return false;
            frames.push_back(Frame{pv->correlationId, pv->producerEpoch, pv->baseSequence});
            return true;
        };
    }
};

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int passes = 4) {
    for (int i = 0; i < passes; ++i) {
        (void)impl.drainRing(kNow);
        (void)impl.flushBatches(kNow, /*sessionId=*/42, sink);
    }
}

// Three batches of one partition in flight (one record each).
void sendThree(ProducerImpl& impl, Wire& w, const ProducerImpl::SendFn& sink) {
    const std::vector<uint8_t> v(8, 1);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(w.frames.size(), 3u);
}
} // namespace

// C1: the old owner answers NotLeader — the batch is NOT failed and the epoch is
// NOT bumped; the partition holds new batches; once the route moves every stored
// batch goes out again unchanged (same corrId, epoch, sequence) and a later batch
// still carries the original epoch.
TEST(ProducerRoutePark, NotLeaderIsRetriableWithoutEpochBumpRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    sendThree(impl, w, sink);
    const std::vector<Frame> first = w.frames;

    impl.onProduceResponse(first[0].corrId, -1, kNotLeader);
    EXPECT_TRUE(impl.hasInFlight()) << "NotLeader failed the batch";
    EXPECT_TRUE(impl.routeParked(1, 0));

    // A new record while parked: held (never sent to the old owner).
    const std::vector<uint8_t> v(8, 2);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    EXPECT_EQ(w.frames.size(), 3u) << "a new batch went to the old owner while parked";

    // Metadata named the new owner: re-offer, oldest first, same identity.
    EXPECT_EQ(impl.resendRouteParked(), 3);
    pump(impl, sink);
    ASSERT_GE(w.frames.size(), 7u);
    for (int i = 0; i < 3; ++i) {
        // The same batch, a NEW attempt (EOS I-2: the old owner's late answers
        // to the earlier attempt are ignored).
        EXPECT_EQ(batchcorr::base(w.frames[3 + i].corrId), batchcorr::base(first[i].corrId));
        EXPECT_NE(batchcorr::attempt(w.frames[3 + i].corrId), batchcorr::attempt(first[i].corrId));
        EXPECT_EQ(w.frames[3 + i].epoch, first[i].epoch);
        EXPECT_EQ(w.frames[3 + i].baseSequence, first[i].baseSequence);
    }
    EXPECT_FALSE(impl.routeParked(1, 0));
    // The held batch follows the re-offers, under the SAME epoch (no bump).
    EXPECT_EQ(w.frames[6].epoch, first[0].epoch);
    EXPECT_EQ(w.frames[6].baseSequence, first[2].baseSequence + 1);

    for (const Frame& f : w.frames) impl.onProduceResponse(f.corrId, 100, 0);
    EXPECT_FALSE(impl.hasInFlight());
}

// M3: the route re-offer stops at the first refused offer (keeps order) and
// resumes with that batch on the next pass — never a younger batch first.
TEST(ProducerRoutePark, RouteResendKeepsOrderOnRefusalRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    sendThree(impl, w, sink);
    const std::vector<Frame> first = w.frames;

    w.refuse = {w.calls};  // the first re-offer is refused (publication resolving)
    EXPECT_EQ(impl.resendPartitionViaRoute(1, 0), 3);
    pump(impl, sink, 1);
    EXPECT_EQ(w.frames.size(), 3u) << "a younger batch overtook the refused one";
    pump(impl, sink, 1);
    ASSERT_EQ(w.frames.size(), 6u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(batchcorr::base(w.frames[3 + i].corrId), batchcorr::base(first[i].corrId)) << i;
    }
}

// The parked partition un-parks by itself once nothing is in flight any more
// (every stored batch resolved some other way): new batches flow again.
TEST(ProducerRoutePark, ParkClearsWhenNothingIsInFlight) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    sendThree(impl, w, sink);
    impl.onProduceResponse(w.frames[0].corrId, -1, kNotLeader);
    for (int i = 0; i < 3; ++i) impl.onProduceResponse(w.frames[static_cast<std::size_t>(i)].corrId, 100 + i, 0);
    const std::vector<uint8_t> v(8, 3);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    EXPECT_EQ(w.frames.size(), 4u);
    EXPECT_FALSE(impl.routeParked(1, 0));
}

// C1 x EOS I-2: after the re-offer to the new owner, the OLD owner's late
// answers to the superseded attempts (another NotLeader, an OutOfOrderSequence
// from a stale route) are ignored — they neither re-park the partition nor fail
// or re-send anything; the new attempts' OKs complete every record exactly once
// with no epoch bump.  RED (pre-fix: one corrId for every attempt): the late
// NotLeader re-parked the partition and the late OOS failed the head batch and
// bumped the epoch.
TEST(ProducerRoutePark, OldOwnersLateAnswersToSupersededAttemptsAreIgnoredRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    int okRecords = 0, failed = 0;
    std::pair<int*, int*> ctx{&okRecords, &failed};
    impl.setOnBatchAck(
        [](void* c, const rune::client::BatchAck& a) noexcept {
            auto* n = static_cast<std::pair<int*, int*>*>(c);
            if (a.errorCode == 0) *n->first += a.recordCount; else ++*n->second;
        },
        &ctx);
    sendThree(impl, w, sink);
    const std::vector<Frame> first = w.frames;
    const int16_t e0 = impl.producerEpoch();
    for (const Frame& f : first) impl.onProduceResponse(f.corrId, -1, kNotLeader);
    EXPECT_EQ(impl.resendRouteParked(), 3);
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 6u);
    EXPECT_FALSE(impl.routeParked(1, 0));

    impl.onProduceResponse(first[1].corrId, -1, kNotLeader);  // the superseded attempt
    impl.onProduceResponse(first[0].corrId, -1,
                           static_cast<int16_t>(rune::protocol::client::ErrorCode::OutOfOrderSequence));
    EXPECT_FALSE(impl.routeParked(1, 0)) << "a superseded attempt's NotLeader re-parked the partition";
    pump(impl, sink);
    EXPECT_EQ(w.frames.size(), 6u) << "a superseded attempt's answer caused a re-send";
    EXPECT_EQ(failed, 0) << "a superseded attempt's answer failed a batch";

    for (int i = 3; i < 6; ++i) impl.onProduceResponse(w.frames[static_cast<std::size_t>(i)].corrId, 100 + i, 0);
    EXPECT_EQ(okRecords, 3);
    EXPECT_EQ(failed, 0);
    EXPECT_EQ(impl.producerEpoch(), e0) << "epoch bumped";
    EXPECT_FALSE(impl.hasInFlight());
}

// ===========================================================================
// Re-review 1 NEW-4(b): an OutOfOrderSequence on the oldest batch is proof that
// nothing of the identity is stored only when no earlier attempt can still
// land.  Attempts on the SAME lane (one publication, FIFO to the broker) were
// processed before the answered one; an attempt on ANOTHER lane (the route
// moved: node relay vs direct worker, old vs new owner) may land later.  So
// while any live batch of the identity has an unanswered attempt on a previous
// lane the OOS is not conclusive: the batch waits (no failure, no re-sequence)
// for those answers — an OK of any attempt completes it — and is re-sent once
// they are all in; that re-send's answer decides.
// ===========================================================================
namespace {
constexpr int16_t kOos = static_cast<int16_t>(rune::protocol::client::ErrorCode::OutOfOrderSequence);
struct BatchTally {
    int okRecords = 0, failed = 0;
    static void on(void* c, const rune::client::BatchAck& a) noexcept {
        auto* t = static_cast<BatchTally*>(c);
        if (a.errorCode == 0) t->okRecords += a.recordCount; else ++t->failed;
    }
};
} // namespace

// The old lane's attempt of the head lands after the new lane's OOS.  RED
// (pre-fix): the head failed BrokerError(20) and the followers were re-sequenced
// under a new epoch — while the head was in fact stored (old lane OK).
TEST(ProducerRoutePark, OosWhileAnOldLaneAttemptIsOutstandingIsNotConclusiveRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    BatchTally t;
    impl.setOnBatchAck(&BatchTally::on, &t);
    sendThree(impl, w, sink);              // A, B, C on lane 0 (e.g. the node relay)
    const int16_t e0 = impl.producerEpoch();
    ASSERT_EQ(impl.resendPartitionViaRoute(1, 0), 3);  // the route moved: lane 1
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 6u);
    impl.onProduceResponse(w.frames[3].corrId, -1, kOos);  // A's lane-1 attempt: OOS
    pump(impl, sink);
    EXPECT_EQ(t.failed, 0) << "an OOS was taken as proof while A's lane-0 attempt may still land";
    EXPECT_EQ(impl.producerEpoch(), e0) << "re-sequenced while an old-lane attempt was outstanding";
    EXPECT_EQ(w.frames.size(), 6u) << "nothing re-sent / re-sequenced yet";
    impl.onProduceResponse(w.frames[0].corrId, 100, 0);  // A's lane-0 attempt was stored
    impl.onProduceResponse(w.frames[4].corrId, 101, 0);
    impl.onProduceResponse(w.frames[5].corrId, 102, 0);
    EXPECT_EQ(t.okRecords, 3);
    EXPECT_EQ(t.failed, 0);
    EXPECT_EQ(impl.producerEpoch(), e0);
    EXPECT_FALSE(impl.hasInFlight());
}

// Once every old-lane attempt is answered (here: the old owner's NotLeader for
// each — errors of superseded attempts, ignored but counted), the waiting head
// is re-sent on the current lane; an OOS on THAT attempt is conclusive (C4: the
// head fails, the followers are re-sequenced).  RED (pre-fix): the first OOS
// already failed the head.
TEST(ProducerRoutePark, WaitingHeadIsResentOnceTheOldLaneIsAnsweredRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    BatchTally t;
    impl.setOnBatchAck(&BatchTally::on, &t);
    sendThree(impl, w, sink);
    const int16_t e0 = impl.producerEpoch();
    ASSERT_EQ(impl.resendPartitionViaRoute(1, 0), 3);
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 6u);
    impl.onProduceResponse(w.frames[3].corrId, -1, kOos);
    EXPECT_EQ(t.failed, 0);
    for (int i = 0; i < 3; ++i) impl.onProduceResponse(w.frames[static_cast<std::size_t>(i)].corrId, -1, kNotLeader);
    EXPECT_FALSE(impl.routeParked(1, 0)) << "a superseded attempt's NotLeader parked the partition";
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 7u) << "the waiting head is re-sent once the old lane answered";
    EXPECT_EQ(batchcorr::base(w.frames[6].corrId), batchcorr::base(w.frames[0].corrId));
    EXPECT_EQ(w.frames[6].epoch, e0);
    impl.onProduceResponse(w.frames[6].corrId, -1, kOos);  // conclusive now
    pump(impl, sink);
    EXPECT_EQ(t.failed, 1) << "the rejected head fails";
    ASSERT_GE(w.frames.size(), 9u);
    EXPECT_EQ(w.frames[7].epoch, static_cast<int16_t>(e0 + 1)) << "the followers are re-sequenced";
    EXPECT_EQ(w.frames[7].baseSequence, 0);
    EXPECT_EQ(w.frames[8].baseSequence, 1);
}

// Re-review 1 NEW-4(a), the Kafka-faithful contract: DuplicateSequence (Kafka
// DUPLICATE_SEQUENCE_NUMBER) means "already stored, offset unknown" — the batch
// completes Ok with offset -1 exactly once (like the broker's OK -1 known-old
// answer); no failure, no epoch bump, the sequence continues.  RED (pre-fix):
// it was a generic error (D: failed, outcome unknown, the partition reset).
TEST(ProducerRoutePark, DuplicateSequenceIsAStoredBatchWithUnknownOffsetRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    std::vector<rune::client::BatchAck> acks;
    impl.setOnBatchAck(
        [](void* c, const rune::client::BatchAck& a) noexcept {
            static_cast<std::vector<rune::client::BatchAck>*>(c)->push_back(a);
        },
        &acks);
    sendThree(impl, w, sink);
    const int16_t e0 = impl.producerEpoch();
    impl.onProduceResponse(w.frames[0].corrId, 100,
                           static_cast<int16_t>(rune::protocol::client::ErrorCode::DuplicateSequence));
    impl.onProduceResponse(w.frames[1].corrId, 101, 0);
    impl.onProduceResponse(w.frames[2].corrId, 102, 0);
    ASSERT_EQ(acks.size(), 3u);
    EXPECT_EQ(acks[0].errorCode, 0) << "a DuplicateSequence batch is stored: never a failure";
    EXPECT_EQ(acks[0].baseOffset, -1) << "offset unknown";
    const std::vector<uint8_t> v(8, 4);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 4u);
    EXPECT_EQ(w.frames[3].epoch, e0) << "no epoch bump";
    EXPECT_EQ(w.frames[3].baseSequence, 3) << "the sequence continues";
}

// ===========================================================================
// Re-review 1 nits
// ===========================================================================

// N-a: resequence_ never sends a frame it could not re-stamp.  A stored frame
// that no longer decodes fails its batch loudly (DeliveryFailed: an internal
// error) instead of keeping the old frame identity under a new header identity
// (the broker would reject it SchemaValidationFailed; here: offered forever).
// The batches behind it are re-stamped from the next sequence.  RED (pre-fix):
// the mangled batch stayed live (not failed) and C took sequence 2.
TEST(ProducerRoutePark, ResequenceFailsABatchWhoseFrameDoesNotDecodeRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    std::vector<rune::client::BatchAck> acks;
    impl.setOnBatchAck(
        [](void* c, const rune::client::BatchAck& a) noexcept {
            static_cast<std::vector<rune::client::BatchAck>*>(c)->push_back(a);
        },
        &acks);
    sendThree(impl, w, sink);
    const int16_t e0 = impl.producerEpoch();
    impl.debugMangleStoredFrameForTest(1, 0, 1);  // B's stored frame
    impl.onProduceResponse(w.frames[0].corrId, -1, kOos);  // C4: A fails, B and C re-sequenced
    pump(impl, sink);
    ASSERT_EQ(acks.size(), 2u) << "A (rejected) and B (undecodable) must both fail";
    EXPECT_NE(acks[1].errorCode, 0);
    EXPECT_EQ(acks[1].baseSequence, 1) << "B is the failed one";
    ASSERT_EQ(w.frames.size(), 4u) << "only C is re-sent";
    EXPECT_EQ(w.frames[3].epoch, static_cast<int16_t>(e0 + 1));
    EXPECT_EQ(w.frames[3].baseSequence, 0) << "C re-stamped from 0 (B's sequence not consumed)";
}

// N-b: a NotLeader for an already re-offered batch, arriving while the route
// re-offer is still draining (a later offer refused), parks the partition
// again — the drain's completion must not clear that new park.  RED (pre-fix):
// routeParked was cleared and a new batch went to the refusing owner.
TEST(ProducerRoutePark, NotLeaderDuringTheReofferDrainIsNotLostRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    sendThree(impl, w, sink);
    for (int i = 0; i < 3; ++i) impl.onProduceResponse(w.frames[static_cast<std::size_t>(i)].corrId, -1, kNotLeader);
    ASSERT_EQ(impl.resendRouteParked(), 3);
    w.refuse = {w.calls + 1};  // B's re-offer is refused: the drain stops after A
    pump(impl, sink, 1);
    ASSERT_EQ(w.frames.size(), 4u);
    impl.onProduceResponse(w.frames[3].corrId, -1, kNotLeader);  // A's re-offer: NotLeader again
    pump(impl, sink, 1);  // the drain completes (B, C)
    ASSERT_EQ(w.frames.size(), 6u);
    EXPECT_TRUE(impl.routeParked(1, 0)) << "the NotLeader that arrived during the drain was lost";
    const std::vector<uint8_t> v(8, 9);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    EXPECT_EQ(w.frames.size(), 6u) << "a new batch went to the owner that just refused";
}

// N-e: MinIsrViolation is a definite non-append (Kafka NOT_ENOUGH_REPLICAS,
// retriable): the batch is re-sent with the SAME identity after a backoff, not
// failed.  RED (pre-fix): D — failed with an unknown outcome.
TEST(ProducerRoutePark, MinIsrViolationIsRetriedNotFailedRegression) {
    ProducerImpl impl(ackedCfg());
    Wire w;
    const auto sink = w.fn();
    BatchTally t;
    impl.setOnBatchAck(&BatchTally::on, &t);
    const std::vector<uint8_t> v(8, 1);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 1u);
    impl.onProduceResponse(w.frames[0].corrId, -1,
                           static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation));
    EXPECT_EQ(t.failed, 0) << "MinIsrViolation (nothing appended) failed the batch";
    EXPECT_TRUE(impl.hasInFlight());
    impl.retransmitStalled(kNow + 1'000'000LL, sink);  // inside the backoff: not yet
    EXPECT_EQ(w.frames.size(), 1u) << "re-sent without a backoff";
    impl.retransmitStalled(kNow + 200'000'000LL, sink);  // past the backoff
    ASSERT_EQ(w.frames.size(), 2u);
    EXPECT_EQ(batchcorr::base(w.frames[1].corrId), batchcorr::base(w.frames[0].corrId));
    EXPECT_EQ(w.frames[1].epoch, w.frames[0].epoch);
    EXPECT_EQ(w.frames[1].baseSequence, w.frames[0].baseSequence);
    impl.onProduceResponse(w.frames[1].corrId, 100, 0);
    EXPECT_EQ(t.okRecords, 1);
    EXPECT_EQ(t.failed, 0);
}

// DR-2 (2026-10-05): a Redirect re-offer refused because the leader's data
// publication is not connected yet (session already Connected, so no connect
// event follows) must stay queued and go out on a later pass — never wait for
// the delivery timeout.  Before the fix the refused batch was re-sent by
// nothing (only the attempt timer) and failed DeliveryFailed after 30 s.
TEST(ProducerRoutePark, RefusedRedirectReofferGoesOutOnALaterPassRegression) {
    ProducerConfig cfg = ackedCfg();
    cfg.produceTimeoutMs  = 30'000;   // the attempt timer never fires within this test
    cfg.deliveryTimeoutMs = 30'000;
    ProducerImpl impl(cfg);
    Wire w;
    const auto sink = w.fn();
    const std::vector<uint8_t> v(8, 1);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 1u);
    const Frame first = w.frames[0];

    // The seed answered Redirect: the re-offer to the leader route is refused
    // (its publication is still connecting).
    Wire refusing;
    refusing.refuse = {0};
    auto refuseFn = refusing.fn();
    (void)impl.retryRedirectedProduce(first.corrId, refuseFn);
    EXPECT_TRUE(refusing.frames.empty());
    EXPECT_TRUE(impl.hasInFlight()) << "a refused re-offer must not fail the batch";

    // The publication connects: the next duty passes re-offer the same batch.
    pump(impl, sink);
    ASSERT_EQ(w.frames.size(), 2u) << "the refused redirect re-offer was never re-driven";
    EXPECT_EQ(batchcorr::base(w.frames[1].corrId), batchcorr::base(first.corrId));
    EXPECT_EQ(w.frames[1].baseSequence, first.baseSequence);
    EXPECT_EQ(w.frames[1].epoch, first.epoch);

    impl.onProduceResponse(w.frames[1].corrId, 100, 0);
    EXPECT_FALSE(impl.hasInFlight());
}

// Review MEDIUM-1: while a refused redirect re-offer is queued, a YOUNGER
// batch's Redirect must not overtake it — it joins the per-pass re-offer, which
// sends oldest first.
TEST(ProducerRoutePark, YoungerRedirectNeverOvertakesAQueuedRefusedOneRegression) {
    ProducerConfig cfg = ackedCfg();
    cfg.produceTimeoutMs  = 30'000;
    cfg.deliveryTimeoutMs = 30'000;
    ProducerImpl impl(cfg);
    Wire w;
    const auto sink = w.fn();
    const std::vector<uint8_t> v(8, 1);
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), v, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(w.frames.size(), 2u);
    const Frame a = w.frames[0], b = w.frames[1];

    Wire refusing;
    refusing.refuse = {0};
    auto refuseFn = refusing.fn();
    (void)impl.retryRedirectedProduce(a.corrId, refuseFn);   // A refused: queued
    Wire direct;
    auto directFn = direct.fn();
    (void)impl.retryRedirectedProduce(b.corrId, directFn);   // B: must NOT go out ahead of A
    EXPECT_TRUE(direct.frames.empty()) << "a younger redirected batch overtook the queued older one";

    pump(impl, sink);
    ASSERT_GE(w.frames.size(), 4u);
    EXPECT_EQ(batchcorr::base(w.frames[2].corrId), batchcorr::base(a.corrId));
    EXPECT_EQ(batchcorr::base(w.frames[3].corrId), batchcorr::base(b.corrId));
    EXPECT_NE(batchcorr::attempt(w.frames[2].corrId), batchcorr::attempt(a.corrId)) << "a new attempt";
}

// Review MEDIUM-2 / LOW-3: the single-node resend-after-reconnect stops at the
// first refused frame; the rest follow it in order on a later pass.
TEST(ProducerRoutePark, ResendAllRedirectedStopsAtTheFirstRefusalRegression) {
    ProducerConfig cfg = ackedCfg();
    cfg.produceTimeoutMs  = 30'000;
    cfg.deliveryTimeoutMs = 30'000;
    ProducerImpl impl(cfg);
    Wire w;
    const auto sink = w.fn();
    sendThree(impl, w, sink);
    const std::vector<Frame> first = w.frames;

    Wire refusing;
    refusing.refuse = {0};   // the oldest is refused, the younger would be accepted
    auto refuseFn = refusing.fn();
    EXPECT_EQ(impl.resendAllRedirected(/*newSessionId=*/77, refuseFn), 0);
    EXPECT_TRUE(refusing.frames.empty()) << "a younger frame went out ahead of the refused oldest";

    pump(impl, sink);
    ASSERT_GE(w.frames.size(), 6u);
    for (int i = 0; i < 3; ++i)
        EXPECT_EQ(batchcorr::base(w.frames[3 + i].corrId), batchcorr::base(first[i].corrId));
}
