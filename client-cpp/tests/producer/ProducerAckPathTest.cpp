// tests/producer/ProducerAckPathTest.cpp — in-flight ring + ack path on a bare
// ProducerImpl (no Aeron, injected clock, bounded pumps).
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
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
            frames.push_back({t, p, v->correlationId, r.recordCount(), r.baseSequence()});
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
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(8), corr);
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
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(ackCfg(2), corr);
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
    std::atomic<int64_t> corr{1};
    ProducerHub hub;
    auto a = std::make_shared<ProducerImpl>(ackCfg(4), corr);
    auto b = std::make_shared<ProducerImpl>(ackCfg(4), corr);
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
