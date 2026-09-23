#pragma once
// WorkerPool.h — E1 Task 3: owns N Workers; deterministically assigns each owned
// PartitionReplica to a worker via ShardRouter; fans out doWork over workers.
//
// Single-owner-per-shard is guaranteed by the ShardRouter being a total,
// deterministic function of (topicId, partitionIndex): assignFromRegistry
// publishes fresh immutable OwnedSets for all workers, so a shard is never
// held by two workers simultaneously. Reassignment (single-writer control plane)
// is just a re-run of assignFromRegistry — the shard is never concurrently owned.
//
// ShardRouter lookups are ASSIGNMENT-TIME only (assignFromRegistry / ownerWorkerId).
// doWork iterates already-assigned workers with NO ShardRouter / map lookups on
// the hot path.
//
// E1: driven cooperatively by the single AgentRunner (no per-worker thread).
// No virtual calls on the hot path (WorkerPool::doWork calls Worker::doWork
// as a direct concrete call; Worker is non-virtual).
//
// E2 prep: Worker is non-movable (atomic<shared_ptr>), so WorkerPool stores
// workers as std::vector<std::unique_ptr<Worker>>.

#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/metadata/ShardRouter.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace rune::node {

class WorkerPool {
public:
    // Construct N workers (ids 0..N-1) and take ownership of the router.
    // Precondition: workerCount >= 1 and must match router.workerCount().
    WorkerPool(int32_t workerCount, rune::metadata::ShardRouter router)
        : router_(std::move(router))
    {
        assert(workerCount >= 1 && "workerCount must be >= 1");
        assert(workerCount == router_.workerCount() &&
               "router.workerCount() must match pool workerCount");
        workers_.reserve(static_cast<std::size_t>(workerCount));
        for (int32_t i = 0; i < workerCount; ++i)
            workers_.emplace_back(std::make_unique<Worker>(i));
    }

    // Non-copyable; non-movable (workers_ contains unique_ptr<Worker> where
    // Worker itself is non-movable, but unique_ptr is movable — left as default).
    WorkerPool(const WorkerPool&)            = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;
    WorkerPool(WorkerPool&&)                 = default;
    WorkerPool& operator=(WorkerPool&&)      = default;

    [[nodiscard]] int32_t workerCount() const noexcept {
        return static_cast<int32_t>(workers_.size());
    }

    [[nodiscard]] Worker& worker(int32_t id) noexcept {
        assert(id >= 0 && id < workerCount());
        return *workers_[static_cast<std::size_t>(id)];
    }

    [[nodiscard]] const rune::metadata::ShardRouter& router() const noexcept {
        return router_;
    }

    // Return the worker that owns (topicId, partition) per the router.
    // ASSIGNMENT-TIME only — do not call on the doWork hot path.
    [[nodiscard]] int32_t ownerWorkerId(int32_t topicId,
                                        int32_t partition) const noexcept {
        return router_.workerForPartition(topicId, partition);
    }

    // Re-derive ownership deterministically from the registry and publish an
    // immutable OwnedSet to each worker. Every replica maps to exactly one worker
    // (total assignment). Idle workers (no replicas) receive an empty published
    // set — not null — so doWork() returns 0 cleanly.
    // Must be called on the single duty-cycle thread only.
    //
    // partitionIndex used for routing = static_cast<int32_t>(r->partitionId()).
    // Correctness invariant (verified in BrokerNode.cpp:434):
    //   partitionId() == the partition ordinal (pcfg.partitionId = part).
    // If partitionId ever becomes a global gpid this must be updated to use a
    // separate ordinal accessor; the ShardRouter test (Task 1) already guards
    // the router given the ordinal — this is the wiring guard.
    void assignFromRegistry(const ReplicaRegistry& registry) {
        const std::size_t n = static_cast<std::size_t>(workerCount());

        // Build per-worker vectors.
        std::vector<Worker::OwnedSet> perWorker(n);
        for (auto* r : registry.allReplicas()) {
            if (!r) continue;
            const int32_t tid = r->topicId();
            const int32_t pid = static_cast<int32_t>(r->partitionId());
            const int32_t wid = ownerWorkerId(tid, pid);
            perWorker[static_cast<std::size_t>(wid)].push_back(r);
        }

        // Publish each worker's snapshot (empty set for idle workers, not null).
        for (std::size_t i = 0; i < n; ++i) {
            workers_[i]->publishOwned(
                std::make_shared<const Worker::OwnedSet>(std::move(perWorker[i])));
        }

        lastGeneration_ = registry.generation();  // seed for reconcile()
    }

    // Fan out doWork over all workers and return the total work count.
    // HOT PATH: no ShardRouter lookups, no map lookups — just iterates the
    // pre-assigned workers directly (concrete, non-virtual Worker::doWork).
    int doWork(int64_t nowNs) {
        int work = 0;
        for (auto& w : workers_) work += w->doWork(nowNs);
        return work;
    }

    // -----------------------------------------------------------------------
    // E2 inc2a: enqueue a WorkerOp to the worker that owns (topicId, partition).
    //
    // Returns true on success, false when the ring is full (back-pressure).
    // The caller must retry on the next duty cycle.
    //
    // ASSIGNMENT-TIME lookup (ownerWorkerId) is acceptable here: the assignment
    // is stable within a duty cycle and only changes on reconcile(), which is
    // driven before doWork() on the same thread.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool tryEnqueueOp(int32_t topicId, int32_t partition,
                                    WorkerOp op) {
        const int32_t wid = ownerWorkerId(topicId, partition);
        return workers_[static_cast<std::size_t>(wid)]->tryEnqueueOp(std::move(op));
    }

    // -----------------------------------------------------------------------
    // E2 inc2a: drain all workers' return rings, calling fn for each result.
    //
    // Called by the gateway in gateway::doWork to collect results from the
    // previous cycle and send the corresponding egress frames.
    // -----------------------------------------------------------------------
    template <typename Fn>
    std::size_t drainAllReturns(Fn&& fn) {
        std::size_t total = 0;
        for (auto& w : workers_) {
            total += w->drainReturn(std::forward<Fn>(fn));
        }
        return total;
    }

    // Re-derive shard→worker ownership when the registry's replica set changes.
    // Cheap: compares ReplicaRegistry::generation() (O(1); correct under runtime
    // add()+remove(), unlike a count proxy which misses a same-count add+remove —
    // a latent UAF once E2 adds shards at runtime); a full re-derivation
    // (assignFromRegistry) only runs on change. Deterministic + single-writer.
    // Called every duty cycle before workerPool_->doWork (D dynamic rebalance).
    void reconcile(const ReplicaRegistry& registry) {
        const std::uint64_t g = registry.generation();
        if (g != lastGeneration_) {
            assignFromRegistry(registry);   // updates lastGeneration_
        }
    }

private:
    rune::metadata::ShardRouter          router_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::uint64_t                        lastGeneration_ = static_cast<std::uint64_t>(-1); // sentinel: not yet assigned
};

} // namespace rune::node
