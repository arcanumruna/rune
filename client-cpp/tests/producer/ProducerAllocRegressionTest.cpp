// tests/producer/ProducerAllocRegressionTest.cpp
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::ProducerImpl;
namespace rcc = rune::client::codec;

namespace {
constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

ProducerConfig ackCfg(int32_t maxInFlight) {
    ProducerConfig c;
    c.acks = Acks::Leader;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = maxInFlight;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

int pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        flushed += impl.flushBatches(kNow, 42, sink);
        if (d == 0) break;
    }
    return flushed;
}
} // namespace

// Regression: every acked batch allocated a std::map node + a std::vector frame
// copy (BatchInfo.wireFrame).  Steady state must allocate nothing.
TEST(ProducerAllocRegression, SteadyStateAckedFlushAllocatesNothingRegression) {
    ProducerImpl impl(ackCfg(4));
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&lastCorr](int32_t, int32_t, int32_t, const uint8_t* b,
                                                  std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        lastCorr = v ? v->correlationId : 0;
        return true;
    };
    auto cycle = [&](uint64_t k) {
        (void)impl.submitSendRecord(1, 0, Key(k), kVal, false, 0);
        pump(impl, sink);
        impl.onProduceResponse(lastCorr, static_cast<int64_t>(k), 0);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);   // warm-up: builder, ring, frame capacity
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "acked produce steady state must not allocate";
    EXPECT_FALSE(impl.hasInFlight());
}

// Regression: the in-flight slot kept its wire frame with vector::assign, whose
// capacity is the EXACT size of the first frame; a frame a few bytes larger (a
// wider timestamp-delta varint as the wall clock moves, or a slightly longer
// value) reallocated in steady state — the intermittent 1-allocation failure of
// SteadyStateAckedFlushAllocatesNothingRegression.  Deterministic here: sizes jitter by 2 bytes.
TEST(ProducerAllocRegression, FrameSizeJitterDoesNotReallocateSlotRegression) {
    ProducerImpl impl(ackCfg(1));  // one slot: every frame reuses it
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&lastCorr](int32_t, int32_t, int32_t, const uint8_t* b,
                                                  std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        lastCorr = v ? v->correlationId : 0;
        return true;
    };
    const std::vector<uint8_t> small(16, 0x11), large(18, 0x22);
    auto cycle = [&](uint64_t k, const std::vector<uint8_t>& v) {
        (void)impl.submitSendRecord(1, 0, Key(k), v, false, 0);
        pump(impl, sink);
        impl.onProduceResponse(lastCorr, static_cast<int64_t>(k), 0);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k, small);  // warm-up at the SMALL size only
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k, (k & 1) ? large : small);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "a slightly larger frame must not reallocate the slot's frame";
    EXPECT_FALSE(impl.hasInFlight());
}

// Regression guard: acks=None void sends (no ack handle, no in-flight slot) must
// not allocate in steady state either.
TEST(ProducerAllocRegression, AcksNoneVoidSteadyStateAllocatesNothingRegression) {
    ProducerConfig cfg = ackCfg(4);
    cfg.acks = Acks::None;
    ProducerImpl impl(cfg);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        (void)impl.submitSendRecord(1, 0, Key(k), kVal, false, 0);
        pump(impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "acks=None void steady state must not allocate";
    EXPECT_FALSE(impl.hasInFlight());
}

// Phase 2b F Task 13 (seeded txn stress): a claim reserves room for its record first
// (ProduceFrameBuilder::reserveExtra) - an EXACT vector::reserve, so while a builder grew past its high-water
// mark (a partition held for its AddPartitionsToTxn, or a session down) every claim reallocated and copied the
// whole builder: O(records) allocations instead of amortized geometric growth.  1000 claim-style appends into one
// builder must now allocate O(log n) times.
TEST(ProduceFrameBuilderAllocRegression, ReserveExtraGrowsGeometricallyNotOncePerClaimRegression) {
    rune::client::producer::ProduceFrameBuilder b(0, kNow);
    const std::string key = "k";
    constexpr int kRecords = 1000;
    alloc_counter::startThisThread();
    for (int i = 0; i < kRecords; ++i) {
        b.reserveExtra(key.size() + 24 + rune::client::producer::ProduceFrameBuilder::kRecordOverheadMax);
        (void)b.reserveRecord(key, 24, kNow);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(b.recordCount(), kRecords);
    EXPECT_LE(allocs, 16) << "reserveExtra reallocated " << allocs << " times for " << kRecords << " claims";
}
