// WorkerSubscribeTest.cpp — read-rework T3/T4 I2: worker-local subscription
// cursor table fed by Subscribe/Unsubscribe WorkerOps over the SPSC ingress ring.
//
// No Aeron / no driver: the control-thread enqueue path (tryEnqueueOp → encode →
// ingress_ ring) and the worker drain path (doWork → drainIngress_ → dispatch)
// run in-process on one thread.  Asserts the worker records/updates/removes
// subscriptions and round-trips every scalar field through the wire.
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>

#include <gtest/gtest.h>
#include <cstdint>

using namespace rune::node;

namespace {

constexpr std::size_t kRingBytes = 256 * 1024;

// Build a Subscribe op with distinctive field values for round-trip checks.
static WorkerOp makeSubscribe(int64_t subId, int32_t topicId, int32_t partition,
                              int64_t nextOffset, uint8_t isolation,
                              bool isGroup, bool carrier,
                              rune::storage::Log* logPtr = nullptr) {
    WorkerOp op;
    op.type                 = WorkerOp::Type::Subscribe;
    op.subscriptionId       = subId;
    op.sessionId            = 7000 + subId;
    op.topicId              = topicId;
    op.partition            = partition;
    op.nextOffset           = nextOffset;
    op.isolation            = isolation;
    op.isGroup              = isGroup;
    op.groupCarrierReceived = carrier;
    op.logPtr               = logPtr;
    return op;
}

static WorkerOp makeUnsubscribe(int64_t subId) {
    WorkerOp op;
    op.type           = WorkerOp::Type::Unsubscribe;
    op.subscriptionId = subId;
    return op;
}

TEST(WorkerSubscribe, RegisterRoundTripsAllFields) {
    Worker w(/*workerId=*/0, kRingBytes, kRingBytes);
    auto* fakeLog = reinterpret_cast<rune::storage::Log*>(0xABCD0000);

    ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(/*subId=*/100, /*topic=*/10, /*part=*/3,
                                             /*nextOffset=*/42, /*isolation=*/1,
                                             /*isGroup=*/true, /*carrier=*/false, fakeLog)));
    w.doWork(1);

    const auto& subs = w.subscriptionsForTest();
    ASSERT_EQ(subs.size(), 1u);
    const auto& s = subs[0];
    EXPECT_EQ(s.subscriptionId, 100);
    EXPECT_EQ(s.sessionId, 7100);
    EXPECT_EQ(s.topicId, 10);
    EXPECT_EQ(s.partition, 3);
    EXPECT_EQ(s.nextOffset, 42);
    EXPECT_EQ(s.isolation, 1);
    EXPECT_TRUE(s.isGroup);
    EXPECT_FALSE(s.groupCarrierReceived);
    EXPECT_EQ(s.logPtr, fakeLog) << "logPtr must round-trip through the wire as uintptr_t";
}

TEST(WorkerSubscribe, DuplicateSubscribeUpdatesNotDuplicates) {
    Worker w(/*workerId=*/0, kRingBytes, kRingBytes);

    ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(1, 10, 0, /*nextOffset=*/0, 0, false, false)));
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);

    // Re-subscribe same id with a new cursor + carrier flip (e.g. carrier arrival).
    ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(1, 10, 0, /*nextOffset=*/99, 1, true, true)));
    w.doWork(2);

    const auto& subs = w.subscriptionsForTest();
    ASSERT_EQ(subs.size(), 1u) << "same subscriptionId must update in place";
    EXPECT_EQ(subs[0].nextOffset, 99);
    EXPECT_EQ(subs[0].isolation, 1);
    EXPECT_TRUE(subs[0].isGroup);
    EXPECT_TRUE(subs[0].groupCarrierReceived);
}

TEST(WorkerSubscribe, MultipleSubscriptionsCoexist) {
    Worker w(/*workerId=*/0, kRingBytes, kRingBytes);
    for (int i = 0; i < 5; ++i)
        ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(200 + i, 10, i, i * 10, 0, false, false)));
    w.doWork(1);
    EXPECT_EQ(w.subscriptionsForTest().size(), 5u);
}

TEST(WorkerSubscribe, UnsubscribeRemovesOnlyTarget) {
    Worker w(/*workerId=*/0, kRingBytes, kRingBytes);
    for (int i = 0; i < 3; ++i)
        ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(300 + i, 10, i, 0, 0, false, false)));
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 3u);

    ASSERT_TRUE(w.tryEnqueueOp(makeUnsubscribe(301)));
    w.doWork(2);

    const auto& subs = w.subscriptionsForTest();
    ASSERT_EQ(subs.size(), 2u);
    for (const auto& s : subs) EXPECT_NE(s.subscriptionId, 301) << "301 must be gone";
    // 300 and 302 survive.
    bool has300 = false, has302 = false;
    for (const auto& s : subs) { has300 |= s.subscriptionId == 300; has302 |= s.subscriptionId == 302; }
    EXPECT_TRUE(has300 && has302);
}

TEST(WorkerSubscribe, UnsubscribeUnknownIsNoop) {
    Worker w(/*workerId=*/0, kRingBytes, kRingBytes);
    ASSERT_TRUE(w.tryEnqueueOp(makeSubscribe(400, 10, 0, 0, 0, false, false)));
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);

    ASSERT_TRUE(w.tryEnqueueOp(makeUnsubscribe(9999)));  // never registered
    w.doWork(2);
    EXPECT_EQ(w.subscriptionsForTest().size(), 1u) << "unknown unsubscribe must be a no-op";
}

} // namespace
