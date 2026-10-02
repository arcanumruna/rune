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
