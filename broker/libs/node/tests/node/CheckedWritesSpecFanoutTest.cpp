// CheckedWritesSpecFanoutTest.cpp — checked writes Part 02 Task 5: SchemaFanout carries a topic's schema versions and
// its write spec through ONE FIFO, and pendingFor says whether a topic still has undelivered updates.
#include <rune/node/SchemaFanout.h>
#include <rune/node/WriteState.h>
#include <rune/metadata/ShardRouter.h>

#include "support/CheckedWritesBatches.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
std::unique_ptr<WorkerPool> poolOf(int32_t n, std::size_t ringBytes = 1 << 16) {
    auto p = std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
    p->setRunnersStartedForTest(true);
    return p;
}
void drainAll(WorkerPool& p) {
    for (int pass = 0; pass < 2; ++pass)
        for (int32_t w = 0; w < p.workerCount(); ++w) p.worker(w).doWork(1);
}
std::vector<uint8_t> blobOf(uint16_t version) { return rune::codec::serialize(matrixDescriptor(version)); }
std::vector<uint8_t> specBytes(int32_t topicId) {
    TopicWriteSpecWire w;
    w.topicId = topicId; w.keyFieldId = kIdTag; w.keyType = rune::codec::PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = true;
    std::vector<uint8_t> b(kTopicWriteSpecWireLen);
    (void)encodeTopicWriteSpec(w, b);
    return b;
}
}  // namespace

TEST(CheckedWritesSpecFanout, WriteSpecRidesTheSameFifo) {
    auto pool = poolOf(2);
    SchemaFanout f;
    f.setHoldWriteSpecPassesForTest(3);
    f.post(7, 1, blobOf(1));
    f.postWriteSpec(7, specBytes(7));
    f.post(7, 2, blobOf(2));
    (void)f.pump(*pool);
    drainAll(*pool);
    for (int32_t w = 0; w < 2; ++w) {
        EXPECT_TRUE(pool->worker(w).hasValidatorForTest(7, 1)) << "the update before the held spec is delivered";
        EXPECT_FALSE(pool->worker(w).hasValidatorForTest(7, 2)) << "v2 waits behind the held spec (one FIFO)";
    }
    EXPECT_TRUE(f.pendingFor(7));
    for (int i = 0; i < 10; ++i) (void)f.pump(*pool);
    drainAll(*pool);
    EXPECT_FALSE(f.pendingFor(7));
    for (int32_t w = 0; w < 2; ++w) EXPECT_TRUE(pool->worker(w).hasValidatorForTest(7, 2)) << "worker " << w;
    EXPECT_EQ(f.enqueuedForTest(), 6u) << "three updates on two workers";
}

TEST(CheckedWritesSpecFanout, PendingForTracksUndeliveredItems) {
    auto pool = poolOf(2, /*ringBytes=*/4096);
    WorkerOp filler;
    filler.type = WorkerOp::Type::ReadLease;
    filler.nextOffset = 1;
    while (pool->worker(1).tryEnqueueOp(filler)) {}
    SchemaFanout f;
    f.post(7, 1, blobOf(1));
    f.postWriteSpec(7, specBytes(7));
    (void)f.pump(*pool);
    EXPECT_TRUE(f.pendingFor(7)) << "worker 1's ring is full";
    EXPECT_FALSE(f.pendingFor(8));
    drainAll(*pool);
    (void)f.pump(*pool);
    EXPECT_FALSE(f.pendingFor(7));
    drainAll(*pool);
    EXPECT_TRUE(pool->worker(1).hasValidatorForTest(7, 1));
}

TEST(CheckedWritesSpecFanoutRegression, ForgottenTopicIsNeverDeliveredRegression) {
    auto pool = poolOf(2);
    SchemaFanout f;
    f.setHoldWriteSpecPassesForTest(100);
    f.post(7, 1, blobOf(1));
    f.postWriteSpec(7, specBytes(7));
    f.post(8, 1, blobOf(1));
    (void)f.pump(*pool);
    f.forgetTopic(7);
    EXPECT_FALSE(f.pendingFor(7));
    (void)f.pump(*pool);
    drainAll(*pool);
    EXPECT_TRUE(pool->worker(0).hasValidatorForTest(8, 1)) << "another topic's update is untouched";
}
