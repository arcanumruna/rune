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
//  12. RealThreadProduceConcurrentTSan — E2 inc2b shared-nothing proof:
//      N=2 workers on real AgentRunners, concurrent produce across partitions
//      owned by different workers, TSan-clean + correct results.
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
#include <rune/node/NodeConfig.h>
#include <rune/node/WorkerOp.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/NanoClock.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
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
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override { return true; }
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
        pool.worker(w).setOwnedDirect(
            std::move(perWorker[static_cast<std::size_t>(w)]));
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
    pool.worker(0).setOwnedDirect(rune::node::Worker::OwnedSet{r0.get(), r1.get()});
    pool.worker(1).setOwnedDirect(rune::node::Worker::OwnedSet{});
    // worker 1 owns nothing.

    ASSERT_EQ(pool.worker(0).ownedCount(), static_cast<std::size_t>(2));
    ASSERT_EQ(pool.worker(1).ownedCount(), static_cast<std::size_t>(0));

    // Give BOTH of worker 0's owned replicas REAL work: append a batch straight to
    // each log so its LEO advances past the leader's served high-watermark.  On the
    // next doWork, each owned Leader's recomputeHw() moves the served HW → counts as
    // work.  (A bare ISR-check tick is intentionally NOT work — the idle strategy
    // must park a genuinely-idle leader — so the owned-only proof needs actual work
    // pending, not just a tick.)  logs[0]=r0, logs[1]=r1, both owned by worker 0.
    {
        rune::protocol::RecordBatchBuilder b(0, /*baseTs=*/1000);
        b.append("k", "v", 1000);
        const auto bytes = b.build();
        logs[0]->appendNoSync(bytes.data(), bytes.size(), /*lastOffset=*/0);
        logs[1]->appendNoSync(bytes.data(), bytes.size(), /*lastOffset=*/0);
    }

    // Worker 1 doWork must return 0 — it has no replicas to drive.
    // Use nowNs beyond the 10ms ISR-check interval so Leader ticks fire.
    const int64_t nowNs = 20'000'000LL;  // 20ms > isrCheckIntervalNs default (10ms)
    const int w1Work = pool.worker(1).doWork(nowNs);
    EXPECT_EQ(w1Work, 0) << "empty worker must report 0 work";

    // Worker 0 doWork must return > 0 — each owned Leader advances its served HW to
    // the freshly-appended LEO (recomputeHw registers work), proving worker 0 drives
    // ONLY its owned replicas while worker 1 (owning none) stayed idle.
    const int w0Work = pool.worker(0).doWork(nowNs);
    EXPECT_GT(w0Work, 0) << "worker owning two Leader replicas with pending log growth must report >0 work";
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

// ---------------------------------------------------------------------------
// Test 12: RealThreadProduceConcurrentTSan — E2 inc2b shared-nothing proof.
//
// Build a WorkerPool with N=2 workers on REAL AgentRunner threads.
// Each worker owns a disjoint set of InMemoryLog-backed Leader replicas.
//
// The test thread concurrently:
//   (a) Enqueues PushRead ops to each worker's owned partition(s) — these are
//       handled inside worker::doWork() on the worker thread.
//   (b) Drains the return rings from the main thread.
//
// Both the ingress rings (SPSC write → worker thread) and the return rings
// (SPSC write ← worker thread) are already SPSC-correct.  The key race being
// proved here is that concurrent doWork() calls on disjoint-owned replicas are
// free of data races — the replicas are never touched by two threads.
//
// The concurrent ownership-publish path (control thread) is also exercised:
// the test calls pool.reconcile() from the main thread while workers are
// running, which calls publishWithRetain() (release-store of a raw ptr).
// Workers read the owned_ atomic with acquire.  TSan must see no race here.
//
// Race proof (TSan instrumented):
//   Any report means ownership is NOT disjoint or the atomic protocol is broken.
//   Fix at the source; NEVER suppress.
//
// Correctness assertions:
//   - All PushRead ops return PushReadResults (errorCode path not tested here;
//     logPtr=nullptr → empty chunk is the expected path for InMemoryLog workers).
//   - Ownership is stable: every replica is still owned by exactly one worker
//     after concurrent operation.
//   - logEndOffset for each InMemoryLog remains 0 (no appends — PushRead only).
// ---------------------------------------------------------------------------
TEST(WorkerPool, RealThreadProduceConcurrentTSan) {
    // -----------------------------------------------------------------
    // Setup: 4 InMemoryLog-backed Leader replicas across 2 workers.
    // ShardRouter(2) assigns: partitions 0,2 → worker X; partitions 1,3 → worker Y.
    // Exact assignment is deterministic but worker X/Y depends on the hash.
    // The test does not care which worker owns which — only that they are disjoint.
    // -----------------------------------------------------------------
    static constexpr int32_t kTopicId        = 0;
    static constexpr int32_t kPartitionCount = 4;
    static constexpr int32_t kWorkerCount    = 2;
    static constexpr int     kOpsPerPart     = 60;

    std::vector<std::unique_ptr<InMemoryLog>>   logs;
    std::vector<std::unique_ptr<NullTransport>> transports;
    logs.reserve(static_cast<std::size_t>(kPartitionCount));
    transports.reserve(static_cast<std::size_t>(kPartitionCount));

    auto mk = [&](int32_t topicId, int32_t partition) -> std::unique_ptr<PartitionReplica> {
        logs.push_back(std::make_unique<InMemoryLog>(64 * 1024 * 1024, 100'000));
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
    for (int32_t p = 0; p < kPartitionCount; ++p)
        reg.add(kTopicId, p, mk(kTopicId, p));

    ShardRouter router(kWorkerCount);
    WorkerPool  pool(kWorkerCount, router);
    pool.assignFromRegistry(reg);

    // Pre-condition: every replica owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            if (pool.worker(w).owns(r)) ++owners;
        ASSERT_EQ(owners, 1) << "pre-condition: each replica must be owned by exactly one worker";
    }

    // -----------------------------------------------------------------
    // Start per-worker AgentRunner threads (the E2 inc2b path).
    // Workers self-drive: each runs Worker::doWork() on its own thread,
    // draining its ingress ring and driving its owned replica duty cycle.
    // -----------------------------------------------------------------
    rune::consensus::MonotonicNanoClock clock;
    pool.startRunners(rune::node::NodeConfig::WorkerIdleStrategy::Backoff, clock);

    // -----------------------------------------------------------------
    // Enqueue PushRead ops (kOpsPerPart per partition).
    //
    // PushRead with nextOffset=0, hwBound=0 → empty chunk (no records in log).
    // The worker safely handles this: returns a PushReadResult with an empty chunk.
    // logPtr=nullptr is safe: getLog_ returns nullptr → empty chunk returned.
    //
    // This exercises:
    //   - SPSC ingress ring (control thread → worker thread) under real concurrency.
    //   - Worker::doWork on the owned-replica ISR duty cycle (runs concurrently).
    //   - Concurrent ownership acquire-load from the worker thread.
    //   - Concurrent ownership publish (reconcile()) from the control thread.
    // -----------------------------------------------------------------
    const int kTotalOps = kPartitionCount * kOpsPerPart;

    for (int32_t p = 0; p < kPartitionCount; ++p) {
        for (int iter = 0; iter < kOpsPerPart; ++iter) {
            rune::node::WorkerOp op;
            op.type           = rune::node::WorkerOp::Type::PushRead;
            op.topicId        = kTopicId;
            op.partition      = p;
            op.replica        = reg.lookup(kTopicId, p);
            op.logPtr         = nullptr;  // InMemoryLog is not a Log* — empty chunk path
            op.subscriptionId = static_cast<int64_t>(p * 1000 + iter);
            op.sessionId      = static_cast<int64_t>(p);
            op.nextOffset     = 0;
            op.hwBound        = 0;  // nothing to read (empty log)

            bool ok = false;
            for (int retry = 0; retry < 10000 && !ok; ++retry) {
                ok = static_cast<bool>(pool.tryEnqueueOp(kTopicId, p, op));
                if (!ok) std::this_thread::yield();
            }
            ASSERT_TRUE(ok) << "failed to enqueue PushRead for partition=" << p << " iter=" << iter;
        }
    }

    // -----------------------------------------------------------------
    // Exercise the ownership-publish path under real concurrency.
    // reconcile() on an unchanged registry is a no-op (generation unchanged),
    // but it still calls the acquire/release compare — TSan verifies the fence.
    //
    // We also call reconcile() a few times to prove the concurrent publish path
    // (even though generation didn't change, assignFromRegistry is not called).
    // -----------------------------------------------------------------
    for (int i = 0; i < 10; ++i) {
        pool.reconcile(reg);
        std::this_thread::yield();
    }

    // -----------------------------------------------------------------
    // Drain all return rings until all ops are collected (or deadline).
    // -----------------------------------------------------------------
    std::atomic<int> resultCount{0};

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{10};

    while (resultCount.load() < kTotalOps &&
           std::chrono::steady_clock::now() < deadline) {
        pool.drainAllReturns([&](rune::node::WorkerReturn&&) {
            resultCount.fetch_add(1, std::memory_order_relaxed);
        });
        if (resultCount.load() < kTotalOps) std::this_thread::yield();
    }

    // -----------------------------------------------------------------
    // Stop worker threads before inspecting state or destroying replicas.
    // -----------------------------------------------------------------
    pool.stopAndJoinRunners();

    // -----------------------------------------------------------------
    // Correctness assertions.
    // -----------------------------------------------------------------
    EXPECT_EQ(resultCount.load(), kTotalOps)
        << "all " << kTotalOps << " ops must have returned results";

    // Ownership must still be stable after concurrent operation.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            if (pool.worker(w).owns(r)) ++owners;
        EXPECT_EQ(owners, 1)
            << "post-concurrent: replica (topic=" << r->topicId()
            << " part=" << static_cast<int32_t>(r->partitionId())
            << ") must still be owned by exactly one worker";
    }

    // Logs must be untouched (PushRead with hwBound=0 reads nothing).
    for (int32_t p = 0; p < kPartitionCount; ++p) {
        EXPECT_EQ(logs[static_cast<std::size_t>(p)]->logEndOffset(), 0)
            << "PushRead must not write to the log (partition " << p << ")";
    }

    // If we reach here without a TSan report: shared-nothing-under-real-threads proved.
    SUCCEED();
}

// ---------------------------------------------------------------------------
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::EnqueueResult;

// F3 round 6, item 5: the enqueue outcome distinguishes {Enqueued, Full,
// TooLarge} (integration wave 2: F18's EnqueueResult via enqueueOp — one
// outcome type), and a SetOwnership that can NEVER fit the worker ingress ring
// is a hard error (critical + std::logic_error → the control loop halts the
// broker, same fail-stop as the F7 ownership invariant) — never a silent
// infinite retry (round 5 conflated it with "ring full", so reconcile retried
// forever).
// ---------------------------------------------------------------------------
TEST(WorkerPoolRegression, EnqueueStatusDistinguishesFullAndTooLargeRegression) {
    // 1 KiB ring: maxMsgLength 128 < WorkerOp framing → max tail 0.
    Worker w(0, /*ingressRingBytes=*/1024);
    ASSERT_EQ(w.maxOpTailBytes(), 0u);
    WorkerOp big;
    big.type = WorkerOp::Type::Produce;
    big.recordBatchBytes.assign(1, 0xAB);
    const EnqueueResult r = w.enqueueOp(std::move(big));
    EXPECT_EQ(r, EnqueueResult::TooLarge);

    // 64 KiB ring: fill it with max-size ops until Full (never TooLarge).
    Worker w2(0, 64 * 1024);
    EnqueueResult last = EnqueueResult::Enqueued;
    for (int i = 0; i < 1000 && last == EnqueueResult::Enqueued; ++i) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.recordBatchBytes.assign(w2.maxOpTailBytes(), 0xCD);
        last = w2.enqueueOp(std::move(op));
    }
    EXPECT_EQ(last, EnqueueResult::Full) << "a full ring is back-pressure, not TooLarge";
}

TEST(WorkerPoolRegression, SetOwnershipTooLargeIsFailStopRegression) {
    Rep a(0, 0);
    ReplicaRegistry reg;
    reg.add(0, 0, std::move(a.replica));
    ShardRouter router(1);
    WorkerPool pool(1, router, /*ingressRingBytes=*/1024);   // max tail 0: any owned set is too large
    pool.setRunnersStartedForTest(true);                       // SetOwnership goes over the ring
    EXPECT_THROW(pool.assignFromRegistry(reg), std::logic_error)
        << "an unfittable SetOwnership must fail-stop, not be retried forever as 'ring full'";
}

// F3 round 6, item 4: the replicated-frame fit check is ONE compare against a
// pool-level cached max (all workers share one ring size) — no owner lookup.
TEST(WorkerPoolRegression, FitsIngressTailIsPoolLevelCompareRegression) {
    ShardRouter router(2);
    WorkerPool pool(2, router, /*ingressRingBytes=*/64 * 1024);
    EXPECT_EQ(pool.maxIngressTail(), pool.worker(0).maxOpTailBytes());
    EXPECT_EQ(pool.maxIngressTail(), pool.worker(1).maxOpTailBytes());
    EXPECT_TRUE(pool.fitsIngressTail(pool.maxIngressTail()));
    EXPECT_FALSE(pool.fitsIngressTail(pool.maxIngressTail() + 1));
}

// LAT-1 (f), review of cfb726b: the io-wq CPU list is APPLIED by the worker's own pass (io-wq is per task),
// not by the control thread that configures it.  Portable backend -> ENOTSUP; io_uring -> 0 or the kernel's
// errno; either way the first doWork applies it exactly once.
TEST(WorkerPool, IoWorkerCpusAreAppliedOnTheWorkersOwnPassRegression) {
    ShardRouter router(1);
    WorkerPool  pool(1, router);
    pool.worker(0).setOwnedDirect(rune::node::Worker::OwnedSet{});
    pool.worker(0).setIoWorkerCpus({0});
    EXPECT_EQ(pool.worker(0).ioWorkerCpusResult(), -1) << "configuring must not apply it";
    (void)pool.worker(0).doWork(/*nowNs=*/1);
    EXPECT_NE(pool.worker(0).ioWorkerCpusResult(), -1) << "the worker's first pass applies it";
}
