// WorkerOwnedIndexRegressionTest.cpp — Task 23 fix round 1 (R-I1): the worker's (topicId, partition)
// -> owned replica lookup is an O(1) index rebuilt cold on every ownership change; it must agree
// with the old linear scan for every key (first non-null match wins), and follow SetOwnership ops.
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

namespace {

struct Fleet {
    rune::replication::NullPartitionTransport t;
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> logs;
    std::vector<std::unique_ptr<PartitionReplica>> reps;
    PartitionReplica* make(int32_t topic, int32_t part) {
        logs.push_back(std::make_unique<rune::storage::InMemoryLog>(1 << 12, 16));
        PartitionConfig cfg{};
        cfg.topicId = topic; cfg.partitionId = part; cfg.selfId = 1; cfg.minIsr = 1;
        reps.push_back(std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, logs.back().get(), &t));
        reps.back()->setRegistryHandle(static_cast<uint32_t>(reps.size()), static_cast<uint32_t>(reps.size()));
        return reps.back().get();
    }
};

void expectAgree(const Worker& w, const std::vector<int32_t>& topics, int32_t maxPart) {
    for (int32_t t : topics)
        for (int32_t p = -1; p <= maxPart; ++p)
            EXPECT_EQ(w.findOwnedReplicaForTest(t, p), w.findOwnedReplicaLinearForTest(t, p)) << t << "/" << p;
}

}  // namespace

TEST(WorkerOwnedIndexRegression, IndexMatchesLinearScanForEveryKeyRegression) {
    for (const std::size_t n : {0u, 1u, 64u, 1024u}) {
        SCOPED_TRACE(n);
        Fleet f;
        Worker::OwnedSet owned;
        const int32_t topics[] = {0, 5, 100, -3};
        for (std::size_t i = 0; i < n; ++i) owned.push_back(f.make(topics[i % 4], static_cast<int32_t>(i / 4)));
        if (n >= 2) {
            owned.push_back(owned[1]);        // a duplicate pointer
            owned.push_back(nullptr);         // a null entry
            // a DISTINCT replica with an already-used key: the FIRST must win
            owned.push_back(f.make(topics[0], 0));
        }
        Worker w(0, 1 << 16, 1 << 16, 16, 16);
        w.setOwnedDirect(owned);
        expectAgree(w, {0, 5, 100, -3, 7, 9999, -1}, static_cast<int32_t>(n / 4) + 64);
        if (n >= 2) EXPECT_EQ(w.findOwnedReplicaForTest(0, 0), owned[0]) << "first duplicate wins";
    }
}

TEST(WorkerOwnedIndexRegression, SetOwnershipOpRebuildsTheIndexRegression) {
    Fleet f;
    PartitionReplica* a = f.make(5, 0);
    PartitionReplica* b = f.make(5, 1);
    PartitionReplica* c = f.make(5, 2);
    Worker w(0, 1 << 16, 1 << 16, 16, 16);
    w.setOwnedDirect(Worker::OwnedSet{a, b});
    ASSERT_EQ(w.findOwnedReplicaForTest(5, 0), a);
    ASSERT_EQ(w.findOwnedReplicaForTest(5, 1), b);
    ASSERT_EQ(w.findOwnedReplicaForTest(5, 2), nullptr);

    const uint64_t acks0 = w.ownershipAcksForTest();
    WorkerOp op;
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = {b, c};
    op.ownershipGen = 2;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    (void)w.doWork(1);
    EXPECT_GT(w.ownershipAcksForTest(), acks0) << "the SetOwnership op ran";
    EXPECT_EQ(w.findOwnedReplicaForTest(5, 0), nullptr) << "A was removed";
    EXPECT_EQ(w.findOwnedReplicaForTest(5, 1), b);
    EXPECT_EQ(w.findOwnedReplicaForTest(5, 2), c);
    expectAgree(w, {5, 0}, 4);
}
