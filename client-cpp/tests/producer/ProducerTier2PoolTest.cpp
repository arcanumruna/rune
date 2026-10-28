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
#include <optional>
#include <random>
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
    for (int i = 0; i < records; ++i) bb.add(kVal);
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
            for (int r = 0; r < 8; ++r) bb.add(kVal);
            if (bb.send() == OpResult::Accepted) return;
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
            a.add(kVal);
            EXPECT_EQ(impl.tier2FreeCount(), 3u);
            BatchBuilder b(std::move(a));
            BatchBuilder c(&impl, 1, 0);
            c.add(kVal);
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
        third.add(kVal);  // refused builder: stores nothing
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
    bb.add(kVal);
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
        for (int i = 0; i < 10; ++i) { bb.add(v); ++added; },
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
            bb.add(kVal);
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

// Regression (review I2): the slot is taken at batch().  A builder refused for an empty pool is
// STICKY: even when a slot comes back between its add() calls, it stores nothing and send() is
// BackPressured - a batch can never silently miss a record.
TEST(ProducerTier2PoolRegression, RefusedBuilderNeverSendsAPartialBatchRegression) {
    ProducerImpl impl(cfgOf(Acks::Leader, 1));
    Cap cap; const auto sink = cap.fn(); AckLedger L; L.attach(impl);
    ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);   // holds the only slot (queued, no session)
    impl.drainRing(kNow, 16);
    impl.flushBatches(kNow, 0, sink);
    BatchBuilder bb(&impl, 1, 0);                        // pool empty: refused
    bb.add(kVal);                                        // r1: stored nowhere
    impl.flushBatches(kNow, 42, sink);                   // the slot comes back
    ASSERT_EQ(impl.tier2FreeCount(), 1u);
    bb.add(kVal);                                        // r2: must not start a batch
    EXPECT_EQ(bb.send(), OpResult::BackPressured) << "never Accepted for a batch missing r1";
    EXPECT_EQ(impl.tier2FreeCount(), 1u) << "the refused builder holds no slot";
    EXPECT_EQ(sendBatch(impl, 2), OpResult::Accepted) << "a fresh builder rebuilds the whole batch";
    pump(impl, sink);
    EXPECT_EQ(cap.frames, 2);
}

// Regression (review I1): Tier-2 is single-thread per producer.  batch() off the bound thread
// is refused (send() = WrongThread) and never touches the pool; bindBatchThread() moves it.
TEST(ProducerTier2PoolRegression, OffThreadBatchIsWrongThreadAndPoolUntouchedRegression) {
    ProducerImpl impl(cfgOf(Acks::None, 4));
    Cap cap; const auto sink = cap.fn();
    ASSERT_EQ(impl.tier2FreeCount(), 4u);
    OpResult off = OpResult::Accepted, offAdd = OpResult::Accepted;
    std::thread other([&] {
        BatchBuilder bb(&impl, 1, 0);
        bb.add(kVal);
        off = bb.send();
        offAdd = bb.send();
    });
    other.join();
    EXPECT_EQ(off, OpResult::WrongThread);
    EXPECT_EQ(offAdd, OpResult::WrongThread);
    EXPECT_EQ(impl.tier2FreeCount(), 4u) << "the off-thread builder never touched the pool";
    // A builder made on the bound thread but used off it: add() throws, send() is WrongThread,
    // and the pool is only touched by the owner afterwards.
    {
        BatchBuilder bb(&impl, 1, 0);
        bb.add(kVal);
        OpResult s2 = OpResult::Accepted; bool threw = false;
        std::thread t2([&] {
            try { bb.add(kVal); } catch (const std::logic_error&) { threw = true; }
            s2 = bb.send();
        });
        t2.join();
        EXPECT_TRUE(threw);
        EXPECT_EQ(s2, OpResult::WrongThread);
        EXPECT_EQ(bb.send(), OpResult::Accepted) << "intact: only the one record from the owner thread";
    }
    pump(impl, sink);
    EXPECT_EQ(cap.frames, 1);
    // Rebinding: the other thread becomes the batch thread.
    OpResult bound = OpResult::WrongThread;
    std::thread t3([&] {
        impl.bindBatchThread();
        BatchBuilder bb(&impl, 1, 0);
        bb.add(kVal);
        bound = bb.send();
    });
    t3.join();
    EXPECT_EQ(bound, OpResult::Accepted);
    {
        BatchBuilder bb(&impl, 1, 0);   // now the ORIGINAL thread is the foreign one
        bb.add(kVal);
        EXPECT_EQ(bb.send(), OpResult::WrongThread);
    }
    impl.bindBatchThread();
    EXPECT_EQ(sendBatch(impl, 5), OpResult::Accepted);
}

// ---------------------------------------------------------------------------
// #341 / #342: a builder that dies off the batch thread hands its slot back through the producer ring.
// ---------------------------------------------------------------------------
namespace {
// Destroy `bb` on another thread.
void destroyOffThread(BatchBuilder&& bb) {
    std::thread t([p = std::make_unique<BatchBuilder>(std::move(bb))]() mutable { p.reset(); });
    t.join();
}
// Drive the duty side until the batch thread sees `want` free slots; bounded, fails on the cap.
bool driveUntilFree(ProducerImpl& impl, std::size_t want) {
    for (int i = 0; i < 1000; ++i) {
        if (impl.tier2FreeCount() == want) return true;
        impl.drainRing(kNow, 16);
    }
    return impl.tier2FreeCount() == want;
}
}  // namespace

TEST(Tier2SlotRegression, BuilderDestroyedOffTheBatchThreadReturnsItsSlotRegression) {
    constexpr int32_t kSlots = 4;
    ProducerImpl impl(cfgOf(Acks::None, kSlots));
    Cap cap; const auto sink = cap.fn();
    const std::size_t free0 = impl.tier2FreeCount();
    ASSERT_EQ(free0, static_cast<std::size_t>(kSlots));
    for (int round = 0; round < 2 * kSlots; ++round) {
        SCOPED_TRACE(round);
        BatchBuilder b(&impl, 1, 0);
        b.add(kVal);
        ASSERT_EQ(impl.tier2FreeCount(), free0 - 1) << "the builder holds a slot";
        destroyOffThread(std::move(b));
        ASSERT_TRUE(driveUntilFree(impl, free0)) << "the slot never came back";
    }
    EXPECT_EQ(sendBatch(impl, 1), OpResult::Accepted) << "no slot was lost";
    pump(impl, sink);
}

TEST(Tier2SlotRegression, OffThreadDiscardInADiscardingProducerStillReturnsTheSlotRegression) {
    constexpr int32_t kSlots = 4;
    ProducerImpl impl(cfgOf(Acks::None, kSlots));
    const std::size_t free0 = impl.tier2FreeCount();
    BatchBuilder b(&impl, 1, 0);
    b.add(kVal);
    ASSERT_EQ(impl.tier2FreeCount(), free0 - 1);
    destroyOffThread(std::move(b));
    ASSERT_EQ(impl.closeImmediately(), OpResult::Accepted);  // discardRing_ reads the message
    ASSERT_TRUE(driveUntilFree(impl, free0)) << "the slot never came back";
}

TEST(Tier2SlotRegression, FreeCountOffTheBatchThreadReadsNothingRegression) {
    ProducerImpl impl(cfgOf(Acks::None, 4));
    std::size_t off = 99;
    std::thread t([&] { off = impl.tier2FreeCount(); });
    t.join();
    EXPECT_EQ(off, 0u) << "off the batch thread the free ring is not ours to consume";
    EXPECT_EQ(impl.tier2FreeCount(), 4u);
}

// C1 adversarial review (#341): a seeded mix of on-thread drops, off-thread destructions, off-thread move-assign
// over a live builder, accepted batches and duty passes never loses or double-returns a slot.
TEST(Tier2SlotRegression, MixedBuilderDeathsKeepThePoolWholeAdversarialRegression) {
    constexpr int32_t kSlots = 4;
    ProducerImpl impl(cfgOf(Acks::None, kSlots));
    Cap cap;
    const auto sink = cap.fn();
    std::mt19937 rng(0xC1C1u);
    long accepted = 0;
    for (int step = 0; step < 400; ++step) {
        SCOPED_TRACE(step);
        switch (rng() % 5) {
            case 0: {  // dies off the batch thread
                BatchBuilder b(&impl, 1, 0);
                b.add(kVal);
                destroyOffThread(std::move(b));
                break;
            }
            case 1: {  // off-thread move-assign: the target's live slot goes back, the source's moves
                BatchBuilder a(&impl, 1, 0);
                BatchBuilder b(&impl, 1, 0);
                a.add(kVal);
                b.add(kVal);
                std::thread t([&] { a = std::move(b); });
                t.join();
                break;  // a (holding b's slot) dies here, on the batch thread
            }
            case 2: {  // dropped on the batch thread
                BatchBuilder b(&impl, 1, 0);
                b.add(kVal);
                break;
            }
            case 3: {
                const OpResult r = sendBatch(impl, static_cast<rune::client::Token>(step + 1), 2);
                ASSERT_TRUE(r == OpResult::Accepted || r == OpResult::BackPressured) << static_cast<int>(r);
                if (r == OpResult::Accepted) ++accepted;
                break;
            }
            default: pump(impl, sink); break;
        }
    }
    pump(impl, sink);
    ASSERT_TRUE(driveUntilFree(impl, static_cast<std::size_t>(kSlots))) << "a slot was lost or returned twice";
    EXPECT_GT(accepted, 0);
    EXPECT_EQ(impl.ringForeignMessages(), 0u) << "every discard token is understood by the drain";
    // Every slot is usable at once: kSlots builders open together.
    std::vector<BatchBuilder> all;
    all.reserve(kSlots);
    for (int i = 0; i < kSlots; ++i) all.emplace_back(&impl, 1, 0).add(kVal);
    EXPECT_EQ(impl.tier2FreeCount(), 0u);
    EXPECT_EQ(sendBatch(impl, 9999, 1), OpResult::BackPressured) << "no phantom slot either";
}

// C1 review: after a builder died off the batch thread, rebinding to a new thread is allowed (no live builder),
// and the new thread collects the returned slot.
TEST(Tier2SlotRegression, RebindAfterOffThreadBuilderDeathAdversarialRegression) {
    constexpr int32_t kSlots = 4;
    ProducerImpl impl(cfgOf(Acks::None, kSlots));
    Cap cap;
    {
        BatchBuilder b(&impl, 1, 0);
        b.add(kVal);
        destroyOffThread(std::move(b));
    }
    pump(impl, cap.fn());  // the duty side hands the slot back (free-ring type 2)
    std::size_t freeOnNew = 0;
    OpResult sent = OpResult::Closed;
    std::thread t([&] {
        impl.bindBatchThread();  // no live builder: must be allowed
        freeOnNew = impl.tier2FreeCount();
        sent = sendBatch(impl, 1);
    });
    t.join();
    EXPECT_EQ(freeOnNew, static_cast<std::size_t>(kSlots));
    EXPECT_EQ(sent, OpResult::Accepted);
}

// C1 review: a builder still live when the batch thread is rebound is refused on its old thread, and its slot
// comes back through the producer ring when it dies there (#341): the new thread ends with the whole pool.
TEST(Tier2SlotRegression, BuilderLiveAcrossARebindReturnsItsSlotToTheNewThreadRegression) {
    constexpr int32_t kSlots = 4;
    ProducerImpl impl(cfgOf(Acks::None, kSlots));
    Cap cap;
    std::optional<BatchBuilder> b;
    b.emplace(&impl, 1, 0);
    b->add(kVal);
    std::size_t freeOnNew = 0;
    std::thread t([&] {
        impl.bindBatchThread();  // the old thread's builder is still live
        freeOnNew = impl.tier2FreeCount();
    });
    t.join();
    EXPECT_EQ(freeOnNew, static_cast<std::size_t>(kSlots - 1)) << "the live builder still holds its slot";
    EXPECT_EQ(b->send(), OpResult::WrongThread) << "the old thread is no longer the batch thread";
    b.reset();     // dies on the old thread, now off the batch thread: handed back through the ring
    pump(impl, cap.fn());
    std::size_t freeAfter = 0;
    OpResult sent = OpResult::Closed;
    std::thread t3([&] {
        impl.bindBatchThread();  // another thread: nothing live
        freeAfter = impl.tier2FreeCount();
        sent = sendBatch(impl, 1);
    });
    t3.join();
    EXPECT_EQ(freeAfter, static_cast<std::size_t>(kSlots));
    EXPECT_EQ(sent, OpResult::Accepted);
}

// ---------------------------------------------------------------------------
// #393: the Tier-2 slot pool (tier2Slots x 2 x tier2SlotBytes, ~8 MiB by default) is allocated by the first
// batch() / bindBatchThread() of the producer's batch thread - a producer that never uses Tier-2 reserves nothing
// (thousands of per-shard producers no longer reserve gigabytes), and steady state still allocates nothing.
// ---------------------------------------------------------------------------

TEST(ProducerTier2Regression, AProducerThatNeverBatchesReservesNoTier2PoolRegression) {
    ProducerImpl impl(cfgOf(Acks::Leader));
    EXPECT_EQ(impl.tier2ReservedBytes(), 0u) << "construction reserves no pool";
    // Tier-1 traffic, flushes and acks do not allocate it either.
    Cap cap;
    const auto sink = cap.fn();
    for (uint64_t k = 0; k < 32; ++k) {
        ASSERT_TRUE(impl.submitSendRecord(1, 0, rune::client::Key(k), kVal, false, 0));
        pump(impl, sink);
        impl.onProduceResponse(cap.lastCorr, static_cast<int64_t>(k), 0);
    }
    EXPECT_EQ(impl.tier2ReservedBytes(), 0u);
    // The pool's introspection stays truthful without allocating it: every configured slot is free.
    EXPECT_EQ(impl.tier2SlotCount(), 64u);
    EXPECT_EQ(impl.tier2FreeCount(), 64u);
    EXPECT_EQ(impl.tier2ReservedBytes(), 0u) << "asking for the counts is not a first use";
    // A batch() from a thread that is not the batch thread is refused without touching (allocating) the pool.
    std::thread t([&] {
        BatchBuilder off(&impl, 1, 0);
        EXPECT_EQ(off.send(), OpResult::WrongThread);
    });
    t.join();
    EXPECT_EQ(impl.tier2ReservedBytes(), 0u);
    // Just the first batch() / bindBatchThread() reserves it: slots x 2 x slotBytes.
    {
        BatchBuilder first(&impl, 1, 0);
        EXPECT_EQ(impl.tier2ReservedBytes(), 64u * 2u * impl.tier2SlotBytes());
    }
    ProducerImpl bound(cfgOf(Acks::None, 8));
    EXPECT_EQ(bound.tier2ReservedBytes(), 0u);
    bound.bindBatchThread();
    EXPECT_EQ(bound.tier2ReservedBytes(), 8u * 2u * bound.tier2SlotBytes()) << "bindBatchThread allocates it";
    bound.bindBatchThread();
    EXPECT_EQ(bound.tier2ReservedBytes(), 8u * 2u * bound.tier2SlotBytes()) << "once";
}

TEST(ProducerTier2Regression, FirstBatchAllocatesThePoolOnceThenSteadyStateIsAllocFreeRegression) {
    ProducerImpl impl(cfgOf(Acks::None, 8));
    Cap cap;
    const auto sink = cap.fn();
    alloc_counter::start();
    ASSERT_EQ(sendBatch(impl, 1), OpResult::Accepted);
    const long first = alloc_counter::stop();
    EXPECT_GE(first, 1L) << "the first batch() allocated the pool";
    const std::size_t reserved = impl.tier2ReservedBytes();
    EXPECT_EQ(reserved, 8u * 2u * impl.tier2SlotBytes());
    pump(impl, sink);
    for (rune::client::Token t = 2; t < 66; ++t) {  // warm-up: builders, frames, the rest of the duty side
        ASSERT_EQ(sendBatch(impl, t), OpResult::Accepted);
        pump(impl, sink);
    }
    alloc_counter::start();
    for (rune::client::Token t = 66; t < 1066; ++t) {
        ASSERT_EQ(sendBatch(impl, t), OpResult::Accepted);
        pump(impl, sink);
    }
    EXPECT_EQ(alloc_counter::stop(), 0L) << "after the first use the Tier-2 path allocates nothing";
    EXPECT_EQ(impl.tier2ReservedBytes(), reserved) << "the pool was allocated once";
}
