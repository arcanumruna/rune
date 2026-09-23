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
// E2 inc2b: each Worker runs on its own AgentRunner thread (real thread-per-core).
//
// WorkerPool owns per-worker:
//   - One IdleStrategy instance (BackoffIdleStrategy or BusySpinIdleStrategy).
//   - One AgentRunner wrapping the Worker (mirrors LogCleanerAgent in BrokerNode).
//   - A K=4 deep shared_ptr retain ring for safe raw-pointer reclamation:
//       On publishWithRetain(i, set), the new shared_ptr is pushed into the ring
//       (evicting the oldest — which is safe because publishes happen only on
//       committed-map changes, seconds apart, while the worker executes thousands
//       of doWork iterations per second — the evicted set is long unreferenced).
//       The raw pointer is stored with release AFTER the shared_ptr is retained.
//
// startRunners(strategy, clock) / stopAndJoinRunners() are the lifecycle hooks
// called by BrokerNode::start() and BrokerNode::stop() respectively.
//
// Worker is non-movable (atomic<OwnedSet*>), so WorkerPool stores workers as
// std::vector<std::unique_ptr<Worker>>.

#include <rune/consensus/ConsensusAgent.h>  // defines Agent (needed before AgentRunner.h)
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/metadata/ShardRouter.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace rune::node {

class WorkerPool {
public:
    // Depth of the per-worker shared_ptr retain ring.
    // K=4 means the 4 most-recently published OwnedSets are kept alive.
    // Ownership publishes happen only on registry-generation changes (seconds apart);
    // with K=4 the evicted set has been unreferenced for at least 3 publish cycles
    // before it may be freed — far longer than any worker duty cycle.
    static constexpr std::size_t kRetainRingDepth = 4;

    // Construct N workers (ids 0..N-1) and take ownership of the router.
    // Precondition: workerCount >= 1 and must match router.workerCount().
    WorkerPool(int32_t workerCount, rune::metadata::ShardRouter router)
        : router_(std::move(router))
    {
        assert(workerCount >= 1 && "workerCount must be >= 1");
        assert(workerCount == router_.workerCount() &&
               "router.workerCount() must match pool workerCount");
        const std::size_t n = static_cast<std::size_t>(workerCount);
        workers_.reserve(n);
        retainRings_.resize(n);
        retainHeads_.resize(n, 0);
        for (int32_t i = 0; i < workerCount; ++i)
            workers_.emplace_back(std::make_unique<Worker>(i));
    }

    // Non-copyable; movable (runners not yet started at construction).
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
        // publishWithRetain() retains the shared_ptr in the K-deep ring first,
        // then stores the raw pointer with release — safe raw-ptr reclamation.
        for (std::size_t i = 0; i < n; ++i) {
            publishWithRetain(i,
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
    // Called every duty cycle (D dynamic rebalance) on the CONTROL thread only.
    // NOTE: in E2 inc2b, NodeRuntimeAgent::doWork no longer calls doWork() on the
    // pool (workers self-drive on their own threads).  reconcile() remains on the
    // control thread to publish updated owned-sets when the registry changes.
    void reconcile(const ReplicaRegistry& registry) {
        const std::uint64_t g = registry.generation();
        if (g != lastGeneration_) {
            assignFromRegistry(registry);   // updates lastGeneration_
        }
    }

    // -----------------------------------------------------------------------
    // E2 inc2b: per-worker AgentRunner lifecycle
    // -----------------------------------------------------------------------

    // Start one AgentRunner per worker.  Must be called after assignFromRegistry
    // (owned sets must be published before workers start spinning).
    //
    // @param strategy  WorkerIdleStrategy enum (from NodeConfig).
    // @param clock     Shared NanoClock (stateless nowNs(); safe to share).
    //
    // Idempotent: if runners_ is already populated this is a no-op.
    void startRunners(NodeConfig::WorkerIdleStrategy strategy,
                      rune::consensus::NanoClock&    clock) {
        if (!runners_.empty()) return; // idempotent

        const std::size_t n = workers_.size();
        idles_.reserve(n);
        runners_.reserve(n);

        for (std::size_t i = 0; i < n; ++i) {
            // Map NodeConfig enum → concrete IdleStrategy instance.
            // Yielding / Sleeping are not yet implemented; fall back to Backoff
            // (the safe default — never pegs a core).
            switch (strategy) {
            case NodeConfig::WorkerIdleStrategy::BusySpin:
                idles_.emplace_back(
                    std::make_unique<rune::consensus::BusySpinIdleStrategy>());
                break;
            case NodeConfig::WorkerIdleStrategy::Backoff:
            case NodeConfig::WorkerIdleStrategy::Yielding:  // fallback to Backoff
            case NodeConfig::WorkerIdleStrategy::Sleeping:  // fallback to Backoff
            default:
                idles_.emplace_back(
                    std::make_unique<rune::consensus::BackoffIdleStrategy>());
                break;
            }
            runners_.emplace_back(
                std::make_unique<rune::consensus::AgentRunner>(
                    *workers_[i], *idles_[i], clock));
            runners_.back()->start();
        }
    }

    // Stop and join every worker AgentRunner.  Called in BrokerNode::stop()
    // BEFORE runtimeAgent_ and replicaRegistry_ are torn down (workers hold
    // non-owning replica pointers; they must be quiesced first).
    //
    // Idempotent + safe if startRunners() was never called.
    void stopAndJoinRunners() {
        for (auto& r : runners_) {
            if (r) r->stopAndJoin();
        }
        runners_.clear();
        idles_.clear();
    }

private:
    // -----------------------------------------------------------------------
    // publishWithRetain — retain the shared_ptr in the K-deep ring, then
    // store the raw pointer with release.
    //
    // Must be called on the single control thread only.
    //
    // Order:
    //   1. Push the new shared_ptr into the per-worker ring (retaining lifetime).
    //      The oldest entry is evicted.  It is safe to evict because:
    //        - Publishes happen only on registry-generation changes (seconds apart).
    //        - Workers execute thousands of doWork iterations per second.
    //        - With K=4, the evicted set has been unreferenced for ≥3 publish
    //          cycles before it can be freed — far longer than any worker duty cycle.
    //   2. Store the raw pointer with release (visible to worker acquire-loads).
    // -----------------------------------------------------------------------
    void publishWithRetain(std::size_t workerIdx,
                           std::shared_ptr<const Worker::OwnedSet> set) noexcept {
        // Step 1: retain in ring (evict oldest).
        auto& ring = retainRings_[workerIdx];
        auto& head = retainHeads_[workerIdx];
        ring[head % kRetainRingDepth] = std::move(set);
        // Keep a raw pointer to the new set BEFORE incrementing head.
        const Worker::OwnedSet* rawPtr =
            ring[head % kRetainRingDepth].get();
        ++head;

        // Step 2: store raw pointer with release — worker reads with acquire.
        workers_[workerIdx]->publishOwned(rawPtr);
    }

    rune::metadata::ShardRouter          router_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::uint64_t                        lastGeneration_ = static_cast<std::uint64_t>(-1); // sentinel: not yet assigned

    // E2 inc2b: per-worker K-deep retain ring for raw-pointer reclamation.
    // retainRings_[i][j] keeps the shared_ptr for worker i alive.
    // retainHeads_[i] is the next slot to write (ring buffer head).
    using RetainRing = std::array<std::shared_ptr<const Worker::OwnedSet>, kRetainRingDepth>;
    std::vector<RetainRing>    retainRings_;  // size == workerCount()
    std::vector<std::size_t>   retainHeads_;  // size == workerCount()

    // E2 inc2b: per-worker idle strategies and agent runners.
    // Populated by startRunners(); cleared by stopAndJoinRunners().
    std::vector<std::unique_ptr<rune::consensus::IdleStrategy>>  idles_;
    std::vector<std::unique_ptr<rune::consensus::AgentRunner>>   runners_;
};

} // namespace rune::node
