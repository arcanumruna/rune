// tests/producer/ProducerFlushDirtyTest.cpp — F12: the duty cycle flushes ONCE,
// walking only the partitions that have work (dirty), never every partition.
//
// Bare ProducerImpl + ProducerHub, injected clock, counting / capturing sinks,
// no Aeron.  "Visits" = ProducerImpl::debugFlushVisits(): partitions examined by
// flushBatches.
#include "../support/AllocCounter.h"

#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace rc  = rune::client;
namespace rcc = rune::client::codec;
using rc::producer::CountingSink;
using rc::producer::ProducerHub;
using rc::producer::ProducerImpl;

namespace {

const std::vector<uint8_t> kVal(16, 0x11);
constexpr int32_t kTopic = 1;

struct Frame {
    int32_t partition;
    int64_t corrId;
    int32_t records;
    std::size_t wireBytes;
};

struct Capture {
    std::vector<Frame> frames;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({p, v->correlationId, r.recordCount(), v->recordBatch.size()});
            return true;
        };
    }
};

rc::ProducerConfig cfgOf(rc::Acks acks, int64_t lingerUs, int32_t maxInFlight = 8) {
    rc::ProducerConfig c;
    c.acks              = acks;
    c.lingerUs          = lingerUs;
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = maxInFlight;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

// N partitions, all created and flushed clean (one record each).
struct Rig {
    std::atomic<int64_t>          corr{1};
    std::shared_ptr<ProducerImpl> impl;
    ProducerHub                   hub;
    int64_t                       now = 1'000'000'000;

    Rig(const rc::ProducerConfig& cfg, int32_t partitions, bool direct) {
        impl = std::make_shared<ProducerImpl>(cfg, corr, 4 * 1024 * 1024);
        if (direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, now});
        hub.add(impl);
    }
    rc::SendResult send(int32_t partition) {
        return impl->trySubmit(kTopic, partition, rc::Key(uint64_t{7}), kVal, 0);
    }
    template <class Sink>
    int cycle(const Sink& sink) { return hub.drainAndFlush(now, /*sessionId=*/42, sink); }
};

void createAll(Rig& r, int32_t partitions) {
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int32_t p = 0; p < partitions; ++p) {
        ASSERT_EQ(r.send(p), rc::SendResult::Ok);
        if ((p & 63) == 63) r.cycle(sink);
    }
    r.cycle(sink);
    r.cycle(sink);
    ASSERT_EQ(frames, static_cast<uint64_t>(partitions));
}

} // namespace

// ---------------------------------------------------------------------------
// The regression: per-cycle flush cost is O(dirty), not O(partitions).
// ---------------------------------------------------------------------------

TEST(ProducerFlushDirty, FlushWalksOnlyDirtyPartitionsRegression) {
    Rig r(cfgOf(rc::Acks::None, 0), 1000, /*direct=*/false);
    createAll(r, 1000);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};

    // Idle cycle: nothing to do, nothing visited.
    uint64_t v0 = r.impl->debugFlushVisits();
    r.cycle(sink);
    EXPECT_LE(r.impl->debugFlushVisits() - v0, 1u) << "idle cycle walked partitions";

    // One dirty partition: O(1) visits, one frame.
    ASSERT_EQ(r.send(500), rc::SendResult::Ok);
    v0 = r.impl->debugFlushVisits();
    r.cycle(sink);
    EXPECT_EQ(frames, 1u);
    EXPECT_LE(r.impl->debugFlushVisits() - v0, 2u) << "1 dirty of 1000 must be O(1)";
}

// 64 records into one partition in one cycle: ONE flush pass (not one per drained
// record — the old drain-1/flush-all loop did 65 x 1000 visits).
TEST(ProducerFlushDirty, RingCycleFlushesOncePerCycleRegression) {
    Rig r(cfgOf(rc::Acks::None, 0), 1000, /*direct=*/false);
    createAll(r, 1000);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int i = 0; i < 64; ++i) ASSERT_EQ(r.send(3), rc::SendResult::Ok);
    const uint64_t v0 = r.impl->debugFlushVisits();
    r.cycle(sink);
    EXPECT_LE(r.impl->debugFlushVisits() - v0, 4u);
    EXPECT_EQ(frames, 1u) << "one pass: the 64 records leave as one frame";
    EXPECT_EQ(r.impl->recordsSent(), 1000u + 64u);
}

TEST(ProducerFlushDirty, DirectPassWalksOnlyDirtyPartitionsRegression) {
    Rig r(cfgOf(rc::Acks::None, 0), 1000, /*direct=*/true);
    createAll(r, 1000);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    for (int i = 0; i < 10; ++i) ASSERT_EQ(r.send(999), rc::SendResult::Ok);
    ASSERT_EQ(r.send(17), rc::SendResult::Ok);
    const uint64_t v0 = r.impl->debugFlushVisits();
    r.cycle(sink);
    EXPECT_LE(r.impl->debugFlushVisits() - v0, 4u);
    EXPECT_EQ(frames, 2u);
}

// Direct flush() marks only partitions with buffered records (O(dirty)).
TEST(ProducerFlushDirty, DirectFlushCostIsDirtyOnlyRegression) {
    Rig r(cfgOf(rc::Acks::None, 60'000'000), 1000, /*direct=*/true);
    {
        uint64_t f = 0, b = 0;
        const CountingSink s{&f, &b};
        for (int32_t p = 0; p < 1000; ++p) ASSERT_EQ(r.send(p), rc::SendResult::Ok);
        r.impl->submitFlush();
        r.cycle(s);
        ASSERT_EQ(f, 1000u);
    }
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    ASSERT_EQ(r.send(42), rc::SendResult::Ok);
    r.cycle(sink);
    EXPECT_EQ(frames, 0u) << "linger holds the batch";
    r.impl->submitFlush();
    const uint64_t v0 = r.impl->debugFlushVisits();
    r.cycle(sink);
    EXPECT_EQ(frames, 1u);
    EXPECT_LE(r.impl->debugFlushVisits() - v0, 2u);
}

// ---------------------------------------------------------------------------
// Semantics the once-per-cycle flush must keep.
// ---------------------------------------------------------------------------

// A partition whose batch is still lingering stays dirty and flushes once the
// linger expires — with no new record arriving.
TEST(ProducerFlushDirty, LingeringPartitionFlushesWhenLingerExpiresRegression) {
    Rig r(cfgOf(rc::Acks::None, /*lingerUs=*/1'000), 64, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_EQ(r.send(5), rc::SendResult::Ok);
    r.cycle(sink);
    EXPECT_TRUE(cap.frames.empty());
    r.now += 999'000;
    r.cycle(sink);
    EXPECT_TRUE(cap.frames.empty());
    r.now += 1'000;
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(cap.frames[0].partition, 5);
}

// A batch held by a full in-flight window stays dirty and leaves after the ack.
TEST(ProducerFlushDirty, WindowBlockedPartitionFlushesAfterAckRegression) {
    Rig r(cfgOf(rc::Acks::Leader, 0, /*maxInFlight=*/1), 16, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_EQ(r.send(2), rc::SendResult::Ok);
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    ASSERT_EQ(r.send(2), rc::SendResult::Ok);
    r.cycle(sink);
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 1u) << "window full";
    r.hub.onProduceResponse(cap.frames[0].corrId, 0);
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 2u);
}

// No session (sessionId 0): nothing is sent, the work stays dirty, and it all
// leaves on the first cycle with a session.
TEST(ProducerFlushDirty, NoSessionKeepsPartitionsDirtyRegression) {
    Rig r(cfgOf(rc::Acks::None, 0), 8, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_EQ(r.send(1), rc::SendResult::Ok);
    ASSERT_EQ(r.send(6), rc::SendResult::Ok);
    r.hub.drainAndFlush(r.now, /*sessionId=*/0, sink);
    r.hub.drainAndFlush(r.now, /*sessionId=*/0, sink);
    EXPECT_TRUE(cap.frames.empty());
    r.cycle(sink);
    EXPECT_EQ(cap.frames.size(), 2u);
}

// flush() is a boundary inside one ring drain: A (before flush) leaves alone;
// B (after it) keeps lingering — even though both are drained in one cycle.
TEST(ProducerFlushDirty, RingFlushTokenIsABatchBoundaryRegression) {
    Rig r(cfgOf(rc::Acks::None, 60'000'000), 4, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    ASSERT_EQ(r.send(0), rc::SendResult::Ok);
    r.impl->submitFlush();
    ASSERT_EQ(r.send(0), rc::SendResult::Ok);
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    EXPECT_EQ(cap.frames[0].records, 1);
    r.cycle(sink);
    EXPECT_EQ(cap.frames.size(), 1u) << "B lingers";
}

// Draining many records in one pass never builds a frame past batchBytes: a
// full builder stops the drain, is flushed, and the drain continues.
TEST(ProducerFlushDirty, RingDrainNeverOvershootsBatchBytesRegression) {
    rc::ProducerConfig cfg = cfgOf(rc::Acks::None, 60'000'000);
    cfg.batchBytes = 512;
    Rig r(cfg, 1, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 100; ++i) ASSERT_EQ(r.send(0), rc::SendResult::Ok);
    r.cycle(sink);
    ASSERT_GT(cap.frames.size(), 1u);
    int32_t total = 0;
    for (const Frame& f : cap.frames) {
        total += f.records;
        // A builder flushes once it reaches batchBytes; one more record never joins it.
        EXPECT_LT(f.wireBytes, 512u + 64u);
    }
    r.impl->submitFlush();
    r.cycle(sink);
    total = 0;
    for (const Frame& f : cap.frames) total += f.records;
    EXPECT_EQ(total, 100);
}

// batchRecords is honoured inside one drain pass too.
TEST(ProducerFlushDirty, RingDrainHonoursBatchRecordsRegression) {
    rc::ProducerConfig cfg = cfgOf(rc::Acks::None, 60'000'000);
    cfg.batchRecords = 10;
    Rig r(cfg, 1, /*direct=*/false);
    Capture cap;
    const auto sink = cap.fn();
    for (int i = 0; i < 35; ++i) ASSERT_EQ(r.send(0), rc::SendResult::Ok);
    r.cycle(sink);
    ASSERT_EQ(cap.frames.size(), 3u);
    for (const Frame& f : cap.frames) EXPECT_EQ(f.records, 10);
}

// The dirty list is preallocated with the partition table: steady-state cycles
// over many partitions allocate nothing.
TEST(ProducerFlushDirty, DirtyTrackingAllocatesNothingRegression) {
    Rig r(cfgOf(rc::Acks::None, 0), 256, /*direct=*/false);
    createAll(r, 256);
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    auto round = [&] {
        for (int32_t p = 0; p < 256; p += 3) (void)r.send(p);
        r.cycle(sink);
    };
    for (int i = 0; i < 4; ++i) round();  // warm-up: scratch buffers at steady size
    alloc_counter::start();
    for (int i = 0; i < 50; ++i) round();
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0);
    EXPECT_GT(frames, 0u);
}
