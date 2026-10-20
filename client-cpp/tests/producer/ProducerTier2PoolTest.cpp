// tests/producer/ProducerTier2PoolTest.cpp - Phase 2b F Task 3: Tier-2 batches live in a
// preallocated slot pool (no per-batch heap, no deque), returned over an SPSC free ring.
// Bare ProducerImpl rigs (no Aeron, injected clock, bounded pumps, no sleeps / atomics).
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using rune::client::Acks;
using rune::client::AckToken;
using rune::client::BatchBuilder;
using rune::client::OpResult;
using rune::client::OpStatus;
using rune::client::ProducerConfig;
using test_support::AckLedger;
using namespace rune::client::producer;
namespace rcc = rune::client::codec;

namespace {

constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

ProducerConfig cfgOf(Acks acks, int32_t slots = 64) {
    ProducerConfig c;
    c.acks = acks;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = 4;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    c.tier2Slots = slots;
    return c;
}

struct Cap {
    int64_t lastCorr = 0;
    long    frames   = 0;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            lastCorr = v->correlationId;
            ++frames;
            return true;
        };
    }
};

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink, int64_t now = kNow) {
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(now, 1024);
        impl.flushBatches(now, 42, sink);
        if (d == 0) break;
    }
}

OpResult sendBatch(ProducerImpl& impl, rune::client::Token tok, int records = 8) {
    BatchBuilder bb(&impl, 1, 0);
    for (int i = 0; i < records; ++i) {
        if (bb.add(kVal) != OpResult::Accepted) return OpResult::BackPressured;
    }
    return bb.send(AckToken{tok});
}

} // namespace

// Regression (fix-on-sight): every Tier-2 batch heap-allocated a Tier2Entry, a
// RecordBatchBuilder + its record vector, and the batch bytes; the partition queue was a
// std::deque.  Steady state (after warm-up) must allocate nothing - sends and the duty
// side's drain / encode / ack, acked and unacked.
TEST(ProducerTier2Regression, BatchSendAllocatesNothingInSteadyStateRegression) {
    for (const Acks acks : {Acks::None, Acks::Leader}) {
        ProducerImpl impl(cfgOf(acks));
        Cap cap;
        const auto sink = cap.fn();
        struct Count {
            long n = 0;
            static void on(void* c, rune::client::Token, int64_t, OpStatus st, uint16_t) noexcept {
                if (st == OpStatus::Ok) ++static_cast<Count*>(c)->n;
            }
        } count;
        impl.setOnAck(&Count::on, &count);  // (AckLedger's std::map would allocate per token)
        auto cycle = [&](rune::client::Token t) {
            ASSERT_EQ(sendBatch(impl, t), OpResult::Accepted);
            pump(impl, sink);
            if (acks != Acks::None) impl.onProduceResponse(cap.lastCorr, static_cast<int64_t>(t) * 8, 0);
        };
        for (rune::client::Token t = 1; t <= 200; ++t) cycle(t);  // warm-up: > one lap of the pool
        alloc_counter::start();
        for (rune::client::Token t = 201; t <= 1200; ++t) cycle(t);
        const long allocs = alloc_counter::stop();
        EXPECT_EQ(allocs, 0L) << "Tier-2 batch send must not allocate (acks=" << static_cast<int>(acks) << ")";
        EXPECT_EQ(count.n, 1200L);
        EXPECT_EQ(impl.tier2FreeCount(), impl.tier2SlotCount());
    }
}

// The same through a Thread-mode style split: the app thread builds and sends, the duty thread
// drains / sends (acks=None: the slot returns at send).  Both threads allocate nothing in
// the measured window.
TEST(ProducerTier2Regression, BatchSendAllocatesNothingOnBothThreadsRegression) {
    constexpr long kWarm = 400, kMeasured = 1000, kTotal = kWarm + kMeasured;
    ProducerImpl impl(cfgOf(Acks::None, 8));
    long sent = 0;                         // duty thread only
    const ProducerImpl::SendFn sink = [&sent](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        ++sent;
        return true;
    };
    std::thread duty([&] {
        while (sent < kTotal) {
            impl.drainRing(kNow, 1024);
            impl.flushBatches(kNow, 42, sink);
        }
    });
    auto sendOne = [&](long i) {
        for (;;) {
            BatchBuilder bb(&impl, 1, 0);
            bool full = false;
            for (int r = 0; r < 8 && !full; ++r) full = bb.add(kVal) != OpResult::Accepted;
            if (!full && bb.send() == OpResult::Accepted) return;
            std::this_thread::yield();  // the pool is exhausted until the duty side returns slots
        }
        (void)i;
    };
    for (long i = 0; i < kWarm; ++i) sendOne(i);
    alloc_counter::start();
    for (long i = kWarm; i < kTotal; ++i) sendOne(i);
    const long allocs = alloc_counter::stop();
    duty.join();
    EXPECT_EQ(allocs, 0L) << "app + duty thread allocated in steady state";
    EXPECT_EQ(sent, kTotal);
    EXPECT_EQ(impl.tier2FreeCount(), impl.tier2SlotCount());
}

// Every way a batch can end gives its slot back, exactly once.
TEST(ProducerTier2Pool, SlotsReturnAfterAckAndAfterFailure) {
    {   // acked
        ProducerImpl impl(cfgOf(Acks::Leader, 4));
        Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
        ASSERT_EQ(impl.tier2FreeCount(), 4u);
        ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
        EXPECT_EQ(impl.tier2FreeCount(), 3u) << "queued unsent: the slot is the duty side's";
        pump(impl, sink);
        EXPECT_EQ(impl.tier2FreeCount(), 4u) << "sent: the frame is copied into the in-flight slot";
        impl.onProduceResponse(cap.lastCorr, 10, 0);
        EXPECT_EQ(L.status(1), OpStatus::Ok);
        EXPECT_EQ(impl.tier2FreeCount(), 4u);
    }
    {   // broker error
        ProducerImpl impl(cfgOf(Acks::Leader, 4));
        Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
        ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
        pump(impl, sink);
        impl.onProduceResponse(cap.lastCorr, -1, /*errorCode=*/3);
        EXPECT_EQ(L.times(1), 1);
        EXPECT_NE(L.status(1), OpStatus::Ok);
        EXPECT_EQ(impl.tier2FreeCount(), 4u);
    }
    {   // delivery timeout of a batch that never left the queue (no session)
        ProducerImpl impl(cfgOf(Acks::Leader, 4));
        Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
        ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
        ASSERT_EQ(sendBatch(impl, 2), OpResult::Accepted);
        impl.drainRing(kNow, 16);
        impl.flushBatches(kNow, /*sessionId=*/0, sink);
        EXPECT_EQ(impl.tier2FreeCount(), 2u);
        impl.sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{kNow + 10'000'000'000LL}});
        EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
        EXPECT_EQ(L.status(2), OpStatus::DeliveryFailed);
        EXPECT_EQ(impl.tier2FreeCount(), 4u);
    }
    {   // close / fail-all: batches still in the ring, and queued on the partition
        ProducerImpl impl(cfgOf(Acks::Leader, 4));
        Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
        cap.frames = 0;
        ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);   // queued on the partition (no session)
        impl.drainRing(kNow, 16);
        impl.flushBatches(kNow, 0, sink);
        ASSERT_EQ(sendBatch(impl, 2), OpResult::Accepted);   // still in the ring
        EXPECT_EQ(impl.tier2FreeCount(), 2u);
        impl.failAllPending(OpStatus::Closed);
        EXPECT_EQ(L.times(1), 1);
        EXPECT_EQ(L.times(2), 1);
        EXPECT_EQ(impl.tier2FreeCount(), 4u);
    }
    {   // a builder dropped without send, and one that is moved around
        ProducerImpl impl(cfgOf(Acks::None, 4));
        {
            BatchBuilder a(&impl, 1, 0);
            ASSERT_EQ(a.add(kVal), OpResult::Accepted);
            EXPECT_EQ(impl.tier2FreeCount(), 3u);
            BatchBuilder b(std::move(a));
            BatchBuilder c(&impl, 1, 0);
            ASSERT_EQ(c.add(kVal), OpResult::Accepted);
            c = std::move(b);  // c's slot is released, b's taken over
            EXPECT_EQ(impl.tier2FreeCount(), 3u);
        }
        EXPECT_EQ(impl.tier2FreeCount(), 4u);
        // a recycled slot starts empty
        Cap cap; const auto sink = cap.fn();
        ASSERT_EQ(sendBatch(impl, 9, 3), OpResult::Accepted);
        pump(impl, sink);
        EXPECT_EQ(cap.frames, 1);
    }
}

TEST(ProducerTier2Pool, ExhaustedPoolIsBackPressuredNotBlockedForever) {
    ProducerImpl impl(cfgOf(Acks::Leader, 2));
    Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
    // Two batches stay queued (no session): they hold both slots.
    ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
    ASSERT_EQ(sendBatch(impl, 2), OpResult::Accepted);
    impl.drainRing(kNow, 16);
    impl.flushBatches(kNow, 0, sink);
    ASSERT_EQ(cap.frames, 0);
    {
        BatchBuilder third(&impl, 1, 0);
        EXPECT_EQ(third.add(kVal), OpResult::BackPressured);
        EXPECT_EQ(third.send(), OpResult::BackPressured) << "no slot: nothing was added, nothing dropped silently";
    }
    // The session arrives, both are sent: slots come back, the next batch is accepted.
    impl.flushBatches(kNow, 42, sink);
    EXPECT_EQ(cap.frames, 2);
    EXPECT_EQ(sendBatch(impl, 3), OpResult::Accepted);
    pump(impl, sink);
    impl.onProduceResponse(cap.lastCorr, 99, 0);
    EXPECT_EQ(L.times(3), 1);
}

// A rejected send() keeps the batch (and its slot) intact for a retry.
TEST(ProducerTier2Pool, RejectedSendKeepsTheBatchAndItsSlot) {
    ProducerImpl impl(cfgOf(Acks::None, 2));
    BatchBuilder bb(&impl, 1, 0);
    ASSERT_EQ(bb.add(kVal), OpResult::Accepted);
    impl.markRingClosed();
    EXPECT_EQ(bb.send(), OpResult::Closed);
    EXPECT_EQ(impl.tier2FreeCount(), 1u) << "the slot is still the builder's";
    EXPECT_THROW((void)bb.add(kVal), std::logic_error) << "sealed after the first send attempt";
}

// Ordering with Tier-1 is unchanged by the pool: Tier-1 records queued before a Tier-2 batch
// leave first, and a Tier-1 record after it waits behind it.
TEST(ProducerTier2Pool, OrderingWithTier1IsUnchanged) {
    ProducerImpl impl(cfgOf(Acks::Leader, 4));
    std::vector<int32_t> counts;   // record count per frame, in send order
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        if (!v) return false;
        rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
        counts.push_back(r.recordCount());
        return true;
    };
    ASSERT_TRUE(impl.submitSendRecord(1, 0, rune::client::Key(uint64_t{1}), kVal, false, 0));
    ASSERT_TRUE(impl.submitSendRecord(1, 0, rune::client::Key(uint64_t{1}), kVal, false, 0));
    ASSERT_EQ(sendBatch(impl, 1, 5), OpResult::Accepted);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, rune::client::Key(uint64_t{1}), kVal, false, 0));
    pump(impl, sink);
    ASSERT_GE(counts.size(), 2u);
    EXPECT_EQ(counts[0], 2) << "Tier-1 before the Tier-2 batch";
    EXPECT_EQ(counts[1], 5) << "the Tier-2 batch is its own RecordBatch";
}

// Regression: a Tier-2 batch is bounded by its slot (tier2SlotBytes): a record that would take
// it past that is refused with std::length_error (MessageTooLarge, non-retryable), the batch
// stays intact and sendable, and a batch that fits is accepted up to the limit.
TEST(ProducerTier2PoolRegression, OversizeBatchIsMessageTooLargeRegression) {
    ProducerConfig c = cfgOf(Acks::None, 2);
    c.tier2SlotBytes = 2048;
    ProducerImpl impl(c);
    Cap cap; const auto sink = cap.fn();
    BatchBuilder bb(&impl, 1, 0);
    const std::vector<uint8_t> v(500, 0x33);
    int added = 0;
    EXPECT_THROW(
        for (int i = 0; i < 10; ++i) { ASSERT_EQ(bb.add(v), OpResult::Accepted); ++added; },
        std::length_error);
    EXPECT_GE(added, 3);
    EXPECT_LT(added, 5);
    EXPECT_EQ(bb.send(), OpResult::Accepted) << "the refused record did not poison the batch";
    pump(impl, sink);
    EXPECT_EQ(cap.frames, 1);
    EXPECT_EQ(impl.tier2FreeCount(), 2u);
}

// Direct (Invoker) ingress: a Tier-2 batch sent from inside the producer's own ack handler is
// deferred (arena), applied after the duty work, sent - and its slot returns, exactly once.
TEST(ProducerTier2Pool, DirectDeferredBatchReturnsItsSlot) {
    ProducerImpl impl(cfgOf(Acks::None, 4));
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    Cap cap; const auto sink = cap.fn();
    struct Ctx { ProducerImpl* impl; int fired = 0; OpResult inner = OpResult::Closed; uint32_t deferred = 0; } ctx{&impl};
    impl.setOnAck(
        [](void* c, rune::client::Token, int64_t, OpStatus, uint16_t) noexcept {
            auto* x = static_cast<Ctx*>(c);
            if (x->fired++ != 0) return;
            BatchBuilder bb(x->impl, 1, 0);
            (void)bb.add(kVal);
            x->inner = bb.send(AckToken{2});
            x->deferred = x->impl->deferredCount();
        },
        &ctx);
    ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
    pump(impl, sink);   // batch 1 is sent: its ack fires inside the flush and sends batch 2
    EXPECT_EQ(ctx.inner, OpResult::Accepted);
    EXPECT_EQ(ctx.deferred, 1u) << "sent from inside the duty work: deferred";
    pump(impl, sink);
    EXPECT_EQ(cap.frames, 2) << "the deferred batch was applied and sent";
    EXPECT_EQ(ctx.fired, 2);
    EXPECT_EQ(impl.deferredCount(), 0u);
    EXPECT_EQ(impl.tier2FreeCount(), impl.tier2SlotCount());
}
