// CheckedWritesAllocRegressionTest.cpp - checked writes Part 03 (hard rule): the write hook and the View acks allocate
// nothing in steady state - the hook's check and stage, the View waiter queue and its fold/HW/deadline release.  Replaces
// the global operator new (binary-wide) through AllocWindow.cpp, so it lives in its own binary.
#include "support/AllocWindow.h"
#include "support/CheckedWritesAckRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int16_t kLevel = 5;
constexpr uint8_t kAll = 1, kAllView = 3;
constexpr int32_t kView = 0x40000001;
}  // namespace

// The test thread drives the worker, so the window brackets exactly one batch's enqueue, doWork and drainReturn; the rig's
// own bookkeeping (the ack maps, the egress poll) runs between windows.  The records reuse 16 keys: a view's row table
// growing for new keys is the fold's, not the hook's or the ack's.
TEST(CheckedWritesAllocRegression, SteadyStateHookAndViewAcksAllocateNothingRegression) {
    AckRigOptions o;
    o.replicas     = {1, 2};
    o.minIsr       = 2;
    o.ackTimeoutNs = 5'000'000;
    AckRig r(o);
    r.typed();
    r.setLevel(kLevel);
    r.installView(kView);

    int64_t corr = 0;
    int32_t seq = 0;
    const auto write = [&](int records, uint8_t acks, uint8_t attr) {
        AckRig::Write w;
        w.corr = ++corr;
        w.acks = acks;
        w.pid  = 11;
        w.seq  = seq;
        for (int i = 0; i < records; ++i) w.records.push_back(validRecord(static_cast<uint64_t>(i)));
        w.attributes.assign(static_cast<std::size_t>(records), attr);
        return w;
    };
    const auto drive = [&](const AckRig::Write& w, bool release, int rounds) {
        const auto batchBytes = AckRig::batchFor(w);
        uint64_t allocs = 0;
        {
            rune::testing::AllocWindow win;
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = AckRig::kTopicId; op.partition = 0;
            op.replica = r.replica.get(); op.logPtr = r.log.get();
            op.sessionId = AckRig::kSession; op.correlationId = w.corr; op.acks = w.acks;
            op.producerId = w.pid; op.producerEpoch = 0; op.baseSequence = w.seq;
            op.recordCount = static_cast<int32_t>(w.records.size()); op.lastOffsetDelta = op.recordCount - 1; op.durabilityTier = 1;
            op.deferAck = true;
            op.recordBatchView = std::span<const uint8_t>(batchBytes);
            if (!r.worker->tryEnqueueOp(std::move(op))) ADD_FAILURE() << "op ring full";
            for (int i = 0; i < rounds; ++i) {
                r.worker->doWork(r.now += 10'000);
                if (release && i == 1)
                    (void)r.replica->onFetchRequest(rune::replication::FetchRequestMsg{r.replica->epoch(), 0, 2, r.leo(), 1 << 20, AckRig::kTopicId}, r.now);
            }
            r.worker->drainReturn([](WorkerReturn&&) {});
            allocs = win.count();
        }
        r.steps(2);   // the egress poll and the rig's bookkeeping, outside the window
        return allocs;
    };
    const auto mix = [&](int n, uint64_t& allocs) {
        for (int i = 0; i < n; ++i) {
            const int records = i % 2 == 0 ? 1 : 16;
            const uint8_t acks = i % 3 == 0 ? kAllView : kAll;
            const auto w = write(records, acks, 0);
            allocs += drive(w, /*release=*/true, 6);
            seq += records;
        }
    };

    uint64_t warm = 0;
    mix(200, warm);
    ASSERT_EQ(r.acks.size(), 200u) << "every warm-up batch was acked";

    uint64_t allocs = 0;
    mix(2000, allocs);
    for (int i = 0; i < 100; ++i) {   // hook refusals: an insert intent on a topic without a key index
        const auto w = write(1, kAllView, 1);
        allocs += drive(w, false, 3);
    }
    for (int i = 0; i < 50; ++i) {    // View deadline expiries: minIsr 2 and no follower fetch
        const auto w = write(1, kAllView, 0);
        allocs += drive(w, false, 600);
        seq += 1;
    }
    EXPECT_EQ(allocs, 0u);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::NoKeyIndex), 100u);
}
