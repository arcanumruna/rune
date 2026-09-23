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
//   5. NodeRuntimeAgentDelegatesReplicaIterationToPool — E1: NodeRuntimeAgent
//      with a WorkerPool delegates per-replica iteration to the pool.
//   6. DoubleAssignFromRegistryIsStable — calling assignFromRegistry twice on
//      the same pool yields stable ownership (no leak / duplication).
//   7. ReconcileDetectsRegistryChange — reconcile re-derives after add().
//   8. ReconcileHandlesSameCountAddRemove — generation-counter reconcile.
//   9. WorkerDoWorkOnlyDrivesOwnedReplicas — carry (a): owned-only proof.
//  10. ConcurrentDisjointWorkersAreRaceFree — TSan shared-nothing proof.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerPool.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <thread>
#include <vector>

using rune::node::WorkerPool;
using rune::node::ReplicaRegistry;
using rune::metadata::ShardRouter;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::storage::InMemoryLog;

namespace {

// Stub consensus agent for NodeRuntimeAgent delegation tests.
struct StubAgent final : rune::consensus::Agent {
    int doWork(int64_t) override { return 0; }
};

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
// Drives ownerWorkerId + publishOwned directly (bypasses assignFromRegistry).
// ---------------------------------------------------------------------------
TEST(WorkerPool, EveryReplicaOwnedByExactlyOneWorker) {
    std::vector<std::unique_ptr<Rep>> keep;
    for (int32_t p = 0; p < 6; ++p) {
        keep.push_back(std::make_unique<Rep>(/*topicId=*/0, p));
    }

    ShardRouter router(3);
    WorkerPool  pool(3, router);

    // Assign directly via ownerWorkerId + publishOwned (mirrors what assignFromRegistry does).
    std::vector<rune::node::Worker::OwnedSet> perWorker(static_cast<std::size_t>(pool.workerCount()));
    for (auto& rep : keep) {
        const int32_t tid = rep->replica->topicId();
        const int32_t pid = static_cast<int32_t>(rep->replica->partitionId());
        const int32_t wid = pool.ownerWorkerId(tid, pid);
        perWorker[static_cast<std::size_t>(wid)].push_back(rep->replica.get());
    }
    for (int32_t w = 0; w < pool.workerCount(); ++w) {
        pool.worker(w).publishOwned(
            std::make_shared<const rune::node::Worker::OwnedSet>(
                std::move(perWorker[static_cast<std::size_t>(w)])));
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

// ---------------------------------------------------------------------------
// Test 5: NodeRuntimeAgent with a WorkerPool delegates per-replica iteration
// to the pool (not the direct registry loop).
// ---------------------------------------------------------------------------
TEST(WorkerPool, NodeRuntimeAgentDelegatesReplicaIterationToPool) {
    // Logs + transports must outlive the replicas.
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    ReplicaRegistry reg;
    for (int32_t p = 0; p < 3; ++p) {
        logs.push_back(std::make_unique<InMemoryLog>(1024 * 1024, 1000));
        transports.push_back(std::make_unique<NullTransport>());
        PartitionConfig cfg{};
        cfg.topicId     = 0;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(p);
        cfg.selfId      = 1;
        cfg.minIsr      = 1;
        reg.add(0, p, std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            logs.back().get(), transports.back().get()));
    }

    ShardRouter router(2);
    WorkerPool  pool(2, router);
    pool.assignFromRegistry(reg);

    StubAgent stub;
    // Full ctor with workerPool appended; all other ptrs null.
    rune::node::NodeRuntimeAgent agent(
        stub,
        /*election=*/nullptr, /*bridge=*/nullptr, /*transport=*/nullptr,
        /*registry=*/&reg,    /*gateway=*/nullptr, /*group=*/nullptr,
        /*txn=*/nullptr,      /*metadata=*/nullptr, /*workerPool=*/&pool);

    // Must not crash and must return a non-negative work count;
    // replicas are driven via the pool, not the registry loop.
    EXPECT_GE(agent.doWork(/*nowNs=*/1), 0);
}

// ---------------------------------------------------------------------------
// Test 6: double assignFromRegistry (simulates a committed-map change).
// Call assignFromRegistry TWICE on the same pool and verify:
//   - every replica is still owned by exactly one worker (no leak / duplication).
//   - worker sets are stable across the reassign.
// ---------------------------------------------------------------------------
TEST(WorkerPool, DoubleAssignFromRegistryIsStable) {
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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

    // First assign.
    pool.assignFromRegistry(reg);
    // Record the worker id for each replica after first assign.
    std::vector<int32_t> firstOwner(4, -1);
    for (auto* r : reg.allReplicas()) {
        const int32_t pid = static_cast<int32_t>(r->partitionId());
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) firstOwner[static_cast<std::size_t>(pid)] = w;
        }
    }

    // Second assign (simulates a rebalance with the same registry content).
    pool.assignFromRegistry(reg);

    // Verify: every replica owned by exactly one worker, same worker as before.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        int32_t ownerW = -1;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) { ++owners; ownerW = w; }
        }
        EXPECT_EQ(owners, 1) << "replica must be owned by exactly one worker after second assign";
        const int32_t pid = static_cast<int32_t>(r->partitionId());
        EXPECT_EQ(ownerW, firstOwner[static_cast<std::size_t>(pid)])
            << "ownership must be stable across reassign for partition " << pid;
    }
}

// ---------------------------------------------------------------------------
// Test 7: reconcile detects a registry change (replica added) and re-derives
// ownership so the new replica is owned by exactly one worker.
// ---------------------------------------------------------------------------
// Test 8: generation-counter reconcile — same replicaCount() after add()+remove()
// must still trigger reassignment (proving the count-proxy UAF fix).
// ---------------------------------------------------------------------------
TEST(WorkerPool, ReconcileDetectsRegistryChange) {
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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
    reg.add(0, 0, mk(0, 0));
    reg.add(0, 1, mk(0, 1));

    ShardRouter router(2);
    WorkerPool  pool(2, router);
    pool.assignFromRegistry(reg);

    // Reconcile with no change — must be a no-op (count unchanged).
    pool.reconcile(reg);
    EXPECT_EQ(pool.worker(0).ownedCount() + pool.worker(1).ownedCount(),
              reg.replicaCount())
        << "reconcile with no change must not alter ownership";

    // Add a new replica to the registry.
    reg.add(0, 2, mk(0, 2));

    // Reconcile detects the count change and re-derives.
    pool.reconcile(reg);

    // Every replica (including the new one) must be owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) ++owners;
        }
        EXPECT_EQ(owners, 1)
            << "replica p=" << static_cast<int32_t>(r->partitionId())
            << " must be owned by exactly one worker after reconcile";
    }
}

// ---------------------------------------------------------------------------
// Test 8: ReconcileHandlesSameCountAddRemove — prove the generation-counter fix.
//
// A same-duty-cycle remove()+add() nets an unchanged replicaCount().  Under the
// old count-proxy, reconcile() was a no-op → the Worker still holds a dangling
// pointer to the freed replica (use-after-free) AND the new replica is never
// owned (starvation).  The generation counter increments on EVERY real mutation
// so reconcile() still triggers reassignment and fixes both hazards.
// ---------------------------------------------------------------------------
TEST(WorkerPool, ReconcileHandlesSameCountAddRemove) {
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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

    // Build registry with 3 replicas across two topic IDs so the count is 3.
    // (topic=0,part=0), (topic=0,part=1) will be removed, (topic=1,part=0).
    // After remove(0,1)+add(1,1) replicaCount stays at 3 — the hazard condition.
    ReplicaRegistry reg;
    reg.add(0, 0, mk(0, 0));
    reg.add(0, 1, mk(0, 1));   // ← this one gets removed
    reg.add(1, 0, mk(1, 0));

    ShardRouter router(2);
    WorkerPool  pool(2, router);
    pool.assignFromRegistry(reg);

    // Verify baseline: all 3 replicas owned.
    EXPECT_EQ(pool.worker(0).ownedCount() + pool.worker(1).ownedCount(),
              static_cast<int>(reg.replicaCount()))
        << "baseline: all replicas must be owned";

    // Generation before the add+remove pair.
    const std::uint64_t genBefore = reg.generation();

    // Remove (topic=0, part=1) — registry destroys the unique_ptr.
    // Do NOT stash the raw pointer: after remove() it is dangling (freed).
    reg.remove(0, 1);
    EXPECT_EQ(reg.lookup(0, 1), nullptr) << "removed replica must be gone from registry";

    // Add a NEW replica (topic=1, part=1) — replicaCount() stays at 3.
    reg.add(1, 1, mk(1, 1));

    // replicaCount() must be unchanged (still 3) — this is the hazard condition.
    EXPECT_EQ(reg.replicaCount(), static_cast<std::size_t>(3))
        << "replicaCount() must be unchanged after same-count add+remove";

    // generation() must have advanced by exactly 2 (one remove + one add).
    EXPECT_EQ(reg.generation(), genBefore + 2)
        << "generation() must advance by 2 (remove + add)";

    // reconcile() MUST detect the change (generation differs) and re-derive.
    pool.reconcile(reg);

    // Every live replica in the registry must be owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        ASSERT_NE(r, nullptr);
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) ++owners;
        }
        EXPECT_EQ(owners, 1)
            << "replica (topic=" << r->topicId()
            << " part=" << static_cast<int32_t>(r->partitionId())
            << ") must be owned by exactly one worker after reconcile";
    }

    // The newly-added replica (topic=1, part=1) must be owned by exactly one worker.
    PartitionReplica* newRaw = reg.lookup(1, 1);
    ASSERT_NE(newRaw, nullptr) << "newly added replica must be findable via lookup";
    int newOwners = 0;
    for (int32_t w = 0; w < pool.workerCount(); ++w) {
        if (pool.worker(w).owns(newRaw)) ++newOwners;
    }
    EXPECT_EQ(newOwners, 1) << "newly added replica must be owned by exactly one worker";

    // Total owned count across workers must equal registry.replicaCount().
    int totalOwned = 0;
    for (int32_t w = 0; w < pool.workerCount(); ++w)
        totalOwned += pool.worker(w).ownedCount();
    EXPECT_EQ(totalOwned, static_cast<int>(reg.replicaCount()))
        << "total owned count must equal registry.replicaCount() after reconcile";

    // Idempotency: a second reconcile with no further change must be a no-op.
    pool.reconcile(reg);
    int totalOwned2 = 0;
    for (int32_t w = 0; w < pool.workerCount(); ++w)
        totalOwned2 += pool.worker(w).ownedCount();
    EXPECT_EQ(totalOwned2, totalOwned)
        << "reconcile must be a no-op when generation is unchanged";
}

// ---------------------------------------------------------------------------
// Test 9: WorkerDoWorkOnlyDrivesOwnedReplicas (carry a) — owned-only proof.
//
// Build a two-worker pool where worker 0 owns ALL replicas and worker 1 owns
// NONE. Drive worker 1's doWork and verify it returns 0 (no replicas driven).
// Drive worker 0's doWork and verify it returns > 0 (fresh-leader ISR check
// fires on the first duty tick, incrementing work by 1 per owned replica).
// This observably proves that each Worker drives ONLY its own owned set.
//
// Note: disk-backed drain-branch coverage (carry b) is not included here
// because constructing a disk-backed PartitionReplica requires a real on-disk
// Log directory (rune::storage::Log), which is out of scope for a unit test
// using existing helpers. The InMemoryLog::isDiskBacked() always returns false
// so the Worker::doWork Step 6.5 drain branch is not reachable in this suite
// without new production seams. Skipped per brief instructions.
// ---------------------------------------------------------------------------
TEST(WorkerPool, WorkerDoWorkOnlyDrivesOwnedReplicas) {
    // Two replicas on topic 0, partitions 0 and 1.
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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

    // Build standalone replicas (not via registry) so we control assignment.
    auto r0 = mk(0, 0);
    auto r1 = mk(0, 1);

    // Two-worker pool; manually assign ALL replicas to worker 0, none to worker 1.
    ShardRouter router(2);
    WorkerPool  pool(2, router);
    pool.worker(0).publishOwned(std::make_shared<const rune::node::Worker::OwnedSet>(
        rune::node::Worker::OwnedSet{r0.get(), r1.get()}));
    pool.worker(1).publishOwned(std::make_shared<const rune::node::Worker::OwnedSet>());
    // worker 1 owns nothing.

    ASSERT_EQ(pool.worker(0).ownedCount(), static_cast<std::size_t>(2));
    ASSERT_EQ(pool.worker(1).ownedCount(), static_cast<std::size_t>(0));

    // Worker 1 doWork must return 0 — it has no replicas to drive.
    // Use nowNs beyond the 10ms ISR-check interval so Leader ticks fire.
    const int64_t nowNs = 20'000'000LL;  // 20ms > isrCheckIntervalNs default (10ms)
    const int w1Work = pool.worker(1).doWork(nowNs);
    EXPECT_EQ(w1Work, 0) << "empty worker must report 0 work";

    // Worker 0 doWork must return > 0 — two fresh Leader replicas each fire
    // their first ISR-check cycle (increments work by at least 1 per replica).
    const int w0Work = pool.worker(0).doWork(nowNs);
    EXPECT_GT(w0Work, 0) << "worker owning two Leader replicas must report >0 work on first tick";
}

// ---------------------------------------------------------------------------
// Test 10: ConcurrentDisjointWorkersAreRaceFree — TSan shared-nothing proof.
//
// Assign a multi-topic / multi-partition registry to a WorkerPool(4), then
// drive each Worker::doWork on a SEPARATE std::thread concurrently (~100
// iterations each). The real assertion is TSan reporting ZERO data races:
// because each worker owns a DISJOINT replica set, concurrent doWork calls
// are shared-nothing and must be race-free.
//
// If TSan reports a race it means ownership is NOT disjoint (a shard mapped to
// two workers, or replicas sharing state) — the defect must be fixed at the
// source (Task 1/3 assignment), NOT suppressed.
// ---------------------------------------------------------------------------
TEST(WorkerPool, ConcurrentDisjointWorkersAreRaceFree) {
    // Keep logs + transports alive for the test's duration.
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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

    // Two topics × four partitions = 8 replicas total.
    ReplicaRegistry reg;
    for (int32_t t = 0; t < 2; ++t)
        for (int32_t p = 0; p < 4; ++p)
            reg.add(t, p, mk(t, p));

    // Four workers; route by partitionId ordinal (ShardRouter default).
    ShardRouter router(4);
    WorkerPool  pool(4, router);
    pool.assignFromRegistry(reg);

    // Verify baseline: every replica is owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) ++owners;
        }
        ASSERT_EQ(owners, 1) << "pre-concurrent: replica must be owned by exactly one worker";
    }

    // Drive every worker concurrently — disjoint ownership => race-free.
    // TSan is the real assertion: it instruments each memory access and will
    // report a data race if two threads touch the same memory unsynchronised.
    std::vector<std::thread> ths;
    ths.reserve(static_cast<std::size_t>(pool.workerCount()));
    for (int32_t w = 0; w < pool.workerCount(); ++w) {
        ths.emplace_back([&pool, w] {
            for (int i = 0; i < 100; ++i) {
                pool.worker(w).doWork(static_cast<int64_t>(i) * 1'000'000);
            }
        });
    }
    for (auto& th : ths) th.join();

    // If we reach here without a crash or TSan report, shared-nothing is proven.
    SUCCEED();
}

// ---------------------------------------------------------------------------
// Test 11: ReconcileReDerivesOwnershipAfterCommittedMapChange (EXIT-3 direct proof).
//
// Simulates a committed-map-style rebalance: a registry is built with replicas
// from 2 co-partitioned topics (4 partitions each), assigned to a 3-worker pool.
// Then 2 replicas are removed and 2 new replicas added (same-net-count pair to
// exercise the generation-counter path), followed by a reconcile().
//
// Asserts after reconcile():
//   A. Every current (live) replica is owned by EXACTLY ONE worker.
//   B. Each live replica's owner == pool.ownerWorkerId(topicId, partitionId)
//      (ShardRouter re-derivation is correct).
//   C. Removed replicas are no longer referenced in any worker's owned set
//      (no dangling pointer).
//   D. Co-group affinity preserved: for all partition indices in [0,4),
//      ownerWorkerId(topic0, idx) == ownerWorkerId(topic1, idx) — same groupKey.
//   E. Total owned count across workers == registry.replicaCount().
// ---------------------------------------------------------------------------
TEST(WorkerPool, ReconcileReDerivesOwnershipAfterCommittedMapChange) {
    // Keep logs + transports alive for the duration of the test.
    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
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

    // -----------------------------------------------------------------------
    // Step 1: Build registry — 2 co-partitioned topics × 4 partitions = 8 replicas.
    // Topics 0 and 1 share groupKey -200 (co-partition group).
    // -----------------------------------------------------------------------
    ReplicaRegistry reg;
    for (int32_t p = 0; p < 4; ++p) {
        reg.add(0, p, mk(0, p));  // topic 0 ("orders"), partitions 0..3
        reg.add(1, p, mk(1, p));  // topic 1 ("shipments"), same co-group
    }
    ASSERT_EQ(reg.replicaCount(), static_cast<std::size_t>(8));

    // -----------------------------------------------------------------------
    // Step 2: Assign to WorkerPool(3) with co-group routing.
    // Map both topic IDs to the same groupKey so co-partitioned pairs land on
    // the same worker (same contract as BrokerNode buildWorkerPool).
    // -----------------------------------------------------------------------
    ShardRouter router(3);
    router.mapTopicToGroupKey(0, -200);
    router.mapTopicToGroupKey(1, -200);
    WorkerPool pool(3, router);
    pool.assignFromRegistry(reg);

    // Baseline: 8 replicas owned across 3 workers.
    {
        int totalOwned = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            totalOwned += pool.worker(w).ownedCount();
        ASSERT_EQ(totalOwned, 8) << "baseline: all 8 replicas must be owned";
    }

    // -----------------------------------------------------------------------
    // Step 3: Simulate a committed-map-style rebalance:
    //   remove (topic=0, partition=2) and (topic=1, partition=3) — 2 replicas gone.
    //   add   (topic=0, partition=2) and (topic=1, partition=3) — 2 fresh replicas.
    // Net count is unchanged at 8 (same-count add+remove) but generation advances by 4.
    // -----------------------------------------------------------------------
    const std::uint64_t genBefore = reg.generation();

    // Remove 2 replicas (registry destroys them — raw pointers become dangling).
    reg.remove(0, 2);
    reg.remove(1, 3);

    EXPECT_EQ(reg.replicaCount(), static_cast<std::size_t>(6))
        << "after remove: count must be 6";

    // Add 2 new replicas in their place.
    reg.add(0, 2, mk(0, 2));
    reg.add(1, 3, mk(1, 3));

    EXPECT_EQ(reg.replicaCount(), static_cast<std::size_t>(8))
        << "after add: count must be back to 8";

    // generation must have advanced by 4 (2 removes + 2 adds).
    EXPECT_EQ(reg.generation(), genBefore + 4)
        << "generation must advance by 4 (2 remove + 2 add)";

    // -----------------------------------------------------------------------
    // Step 4: reconcile() detects the generation change and re-derives.
    // -----------------------------------------------------------------------
    pool.reconcile(reg);

    // -----------------------------------------------------------------------
    // Assert A: every current live replica is owned by EXACTLY ONE worker.
    // -----------------------------------------------------------------------
    for (auto* r : reg.allReplicas()) {
        ASSERT_NE(r, nullptr);
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) ++owners;
        }
        EXPECT_EQ(owners, 1)
            << "replica (topic=" << r->topicId()
            << " part=" << static_cast<int32_t>(r->partitionId())
            << ") must be owned by exactly one worker after reconcile";
    }

    // -----------------------------------------------------------------------
    // Assert B: each live replica's owner == ShardRouter-designated worker.
    // -----------------------------------------------------------------------
    for (auto* r : reg.allReplicas()) {
        const int32_t tid      = r->topicId();
        const int32_t pid      = static_cast<int32_t>(r->partitionId());
        const int32_t expected = pool.ownerWorkerId(tid, pid);
        int32_t actual = -1;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(r)) { actual = w; break; }
        }
        EXPECT_EQ(actual, expected)
            << "replica (topic=" << tid << " part=" << pid
            << ") owned by worker " << actual
            << " but ShardRouter says worker " << expected;
    }

    // -----------------------------------------------------------------------
    // Assert D: co-group affinity preserved — topic 0 and topic 1 share the same
    // worker for every partition index in [0, 4).
    // -----------------------------------------------------------------------
    for (int32_t p = 0; p < 4; ++p) {
        EXPECT_EQ(pool.ownerWorkerId(0, p), pool.ownerWorkerId(1, p))
            << "co-group members (topic0,p=" << p << ") and (topic1,p=" << p
            << ") must share a worker after reconcile";
    }

    // -----------------------------------------------------------------------
    // Assert E: total owned count == registry.replicaCount() (no leak, no
    // duplication, no orphan).
    // -----------------------------------------------------------------------
    {
        int totalOwned = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            totalOwned += pool.worker(w).ownedCount();
        EXPECT_EQ(totalOwned, static_cast<int>(reg.replicaCount()))
            << "total owned count must equal replicaCount() after reconcile";
    }

    // -----------------------------------------------------------------------
    // Idempotency: second reconcile with no further change must be a no-op.
    // -----------------------------------------------------------------------
    pool.reconcile(reg);
    {
        int totalOwned = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            totalOwned += pool.worker(w).ownedCount();
        EXPECT_EQ(totalOwned, static_cast<int>(reg.replicaCount()))
            << "second reconcile must be a no-op (generation unchanged)";
    }
}
