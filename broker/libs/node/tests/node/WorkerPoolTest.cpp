// ---------------------------------------------------------------------------
// WorkerPoolTest.cpp — Unit tests for rune::node::WorkerPool (E1 Task 3).
//
// Test matrix:
//   1. EveryReplicaOwnedByExactlyOneWorker — totality: union of workers' owned
//      sets equals the full replica set; no replica is owned by 2+ workers.
//   2. CoGroupSameIndexSameWorker — co-partition affinity: two topics mapped to
//      the same groupKey + same partition index → same worker.
//   3. ReassignIsDeterministic — two independently constructed pools with the
//      same router config yield identical ownerWorkerId results.
//   4. AssignFromRegistryOwnsAllRegisteredReplicas — production path:
//      assignFromRegistry clears + re-derives ownership from the real registry.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerPool.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <vector>

using rune::node::WorkerPool;
using rune::node::ReplicaRegistry;
using rune::metadata::ShardRouter;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::storage::InMemoryLog;

namespace {

// Minimal no-op transport (same pattern as WorkerTest.cpp).
class NullTransport : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
};

// Helper struct that owns a log + transport + replica.
// Uses 7-arg ctor (role=Leader, epoch=1, replicas={1}, isr={1}).
struct Rep {
    InMemoryLog                       log;
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;

    Rep(int32_t topicId, int32_t partition) :
        log(/*maxBytes=*/1024 * 1024, /*maxRecords=*/1000)
    {
        PartitionConfig cfg{};
        cfg.topicId     = topicId;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId      = 1;
        cfg.minIsr      = 1;
        replica = std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            &log, &transport);
    }

    // Non-copyable (InMemoryLog is not copyable).
    Rep(const Rep&)            = delete;
    Rep& operator=(const Rep&) = delete;
};

} // namespace

// ---------------------------------------------------------------------------
// Test 1: every replica is owned by exactly one worker.
// Drives ownerWorkerId + ownReplica directly (bypasses assignFromRegistry).
// ---------------------------------------------------------------------------
TEST(WorkerPool, EveryReplicaOwnedByExactlyOneWorker) {
    std::vector<std::unique_ptr<Rep>> keep;
    for (int32_t p = 0; p < 6; ++p) {
        keep.push_back(std::make_unique<Rep>(/*topicId=*/0, p));
    }

    ShardRouter router(3);
    WorkerPool  pool(3, router);

    // Assign directly via ownerWorkerId (mirrors what assignFromRegistry does).
    for (auto& rep : keep) {
        const int32_t tid = rep->replica->topicId();
        const int32_t pid = static_cast<int32_t>(rep->replica->partitionId());
        const int32_t wid = pool.ownerWorkerId(tid, pid);
        pool.worker(wid).ownReplica(rep->replica.get());
    }

    // Totality: each replica owned by exactly one worker.
    for (auto& rep : keep) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(rep->replica.get())) ++owners;
        }
        EXPECT_EQ(owners, 1) << "replica must be owned by exactly one worker";
    }
}

// ---------------------------------------------------------------------------
// Test 2: co-partition affinity — topics in the same group + same partition
// index must route to the same worker.
// ---------------------------------------------------------------------------
TEST(WorkerPool, CoGroupSameIndexSameWorker) {
    ShardRouter router(4);
    router.mapTopicToGroupKey(0, -100);
    router.mapTopicToGroupKey(1, -100);
    WorkerPool pool(4, router);
    for (int32_t idx = 0; idx < 8; ++idx) {
        EXPECT_EQ(pool.ownerWorkerId(0, idx), pool.ownerWorkerId(1, idx))
            << "co-group members must share a worker per index";
    }
}

// ---------------------------------------------------------------------------
// Test 3: deterministic reassignment — two independently constructed pools
// with the same router config must agree on every ownerWorkerId.
// ---------------------------------------------------------------------------
TEST(WorkerPool, ReassignIsDeterministic) {
    ShardRouter r1(3);
    WorkerPool  p1(3, r1);
    ShardRouter r2(3);
    WorkerPool  p2(3, r2);
    for (int32_t t = 0; t < 4; ++t) {
        for (int32_t p = 0; p < 5; ++p) {
            EXPECT_EQ(p1.ownerWorkerId(t, p), p2.ownerWorkerId(t, p))
                << "topic=" << t << " partition=" << p;
        }
    }
}

// ---------------------------------------------------------------------------
// Test 4: assignFromRegistry — production path.  Registry OWNS the replicas;
// assignFromRegistry clears all workers and re-derives ownership from allReplicas().
// Every registered replica ends up owned by exactly one worker.
// ---------------------------------------------------------------------------
TEST(WorkerPool, AssignFromRegistryOwnsAllRegisteredReplicas) {
    // Keep logs and transports alive for the lifetime of the test.
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition)
        -> std::unique_ptr<PartitionReplica>
    {
        logs.push_back(std::make_unique<InMemoryLog>(1024 * 1024, 1000));
        transports.push_back(std::make_unique<NullTransport>());
        PartitionConfig cfg{};
        cfg.topicId     = topicId;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId      = 1;
        cfg.minIsr      = 1;
        return std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            logs.back().get(), transports.back().get());
    };

    ReplicaRegistry reg;
    for (int32_t p = 0; p < 4; ++p) reg.add(0, p, mk(0, p));

    ShardRouter router(2);
    WorkerPool  pool(2, router);
    pool.assignFromRegistry(reg);

    // Every registered replica is owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) ++owners;
        }
        EXPECT_EQ(owners, 1) << "replica must be owned by exactly one worker after assignFromRegistry";
    }
}
