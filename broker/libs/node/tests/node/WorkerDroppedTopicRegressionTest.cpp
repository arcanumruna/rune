// WorkerDroppedTopicRegressionTest.cpp — Task 23 fix round 1 (R-M1): a shard-mode (worker-direct)
// produce to a topic this worker does not own answers NotLeader (retriable) — except for a topic the
// catalog DROPPED, which answers UnknownTopic exactly like the gateway (a retriable answer would make
// the producer retry a topic that no longer exists).  Single-threaded: the worker is driven by hand.
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <vector>

using namespace rune::node;
namespace pc = rune::protocol::client;

namespace {

std::vector<uint8_t> produceFrame(int64_t corr, int32_t topicId) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const auto batch = b.build();
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = topicId; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = 1; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
    msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> f(msg.encodedSize());
    msg.encode(f.data());
    return f;
}

int16_t answerFor(Worker& w, int64_t corr, int32_t topicId) {
    const auto f = produceFrame(corr, topicId);
    (void)w.handleIngressFrameForTest(f.data(), f.size(), 1'000'000);
    int16_t code = -1;
    int n = 0;
    (void)w.drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == corr) { code = r.errorCode; ++n; }
    });
    EXPECT_EQ(n, 1) << "exactly one answer for topic " << topicId;
    return code;
}

}  // namespace

TEST(WorkerDroppedTopicRegression, ProduceToDroppedTopicIsUnknownTopicRegression) {
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});
    w.setWorkerIngress(nullptr, true);
    w.setShardMode(true);
    constexpr int16_t kNotLeader = static_cast<int16_t>(pc::ErrorCode::NotLeader);
    constexpr int16_t kUnknown   = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);

    EXPECT_EQ(answerFor(w, 1, 7), kNotLeader) << "never-owned topic: retriable";
    EXPECT_EQ(answerFor(w, 2, 8), kNotLeader);

    WorkerOp op;
    op.type = WorkerOp::Type::TopicDropped;
    op.topicId = 7;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    (void)w.doWork(2'000'000);
    ASSERT_TRUE(w.topicDroppedForTest(7)) << "the TopicDropped op ran";

    EXPECT_EQ(answerFor(w, 3, 7), kUnknown) << "dropped topic: UnknownTopic, like the gateway";
    EXPECT_EQ(answerFor(w, 4, 8), kNotLeader) << "a topic that was never dropped stays retriable";
}

TEST(WorkerDroppedTopicRegression, BootDroppedIdsAnswerUnknownTopicRegression) {
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});
    w.setWorkerIngress(nullptr, true);
    w.setShardMode(true);
    w.setDroppedTopicsDirect({3, 70, 4096});
    EXPECT_EQ(answerFor(w, 1, 70), static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
    EXPECT_EQ(answerFor(w, 2, 4096), static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
    EXPECT_EQ(answerFor(w, 3, 71), static_cast<int16_t>(pc::ErrorCode::NotLeader));
}
