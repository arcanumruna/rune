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

    // Construct N workers (ids 0..N-1) and take ownership of the router.
    // Precondition: workerCount >= 1 and must match router.workerCount().
    // ingressRingBytes:   per-worker ingress SPSC ring byte capacity (power of two,
    //                     NodeConfig workerIngressRingBytes). Default Worker::kIngressByteCapacity.
    // returnRingBytes:    per-worker return SPSC ring byte capacity (power of two,
    //                     NodeConfig workerReturnRingBytes). Default Worker::kReturnByteCapacity.
    // ioSubmitDepth:      IoBackend submission ring depth (NodeConfig ioSubmitRingDepth).
    //                     Default Worker::kIoSubmitDepth.
    // ioCompletionDepth:  IoBackend completion ring depth (NodeConfig ioCompletionRingDepth).
    //                     Default Worker::kIoCompletionDepth.
    // workerCoreBase:     E4: base core for CPU affinity (NodeConfig workerCoreBase).
    //                     -1 (default) = affinity disabled (no pinning).
    //                     >= 0 → worker i is assigned core (workerCoreBase + i);
    //                     setCoreId() is called here (ctor, before runners start).
    WorkerPool(int32_t workerCount, rune::metadata::ShardRouter router,
               std::size_t ingressRingBytes  = Worker::kIngressByteCapacity,
               std::size_t returnRingBytes   = Worker::kReturnByteCapacity,
               std::size_t ioSubmitDepth     = Worker::kIoSubmitDepth,
               std::size_t ioCompletionDepth = Worker::kIoCompletionDepth,
               int32_t     workerCoreBase    = -1)
        : router_(std::move(router))
    {
        assert(workerCount >= 1 && "workerCount must be >= 1");
        assert(workerCount == router_.workerCount() &&
               "router.workerCount() must match pool workerCount");
        const std::size_t n = static_cast<std::size_t>(workerCount);
        workers_.reserve(n);
        for (int32_t i = 0; i < workerCount; ++i) {
            workers_.emplace_back(std::make_unique<Worker>(i, ingressRingBytes,
                                                           returnRingBytes,
                                                           ioSubmitDepth, ioCompletionDepth));
            // E4: assign core affinity before the runner thread starts.
            // Shared-nothing ([[rune-shared-nothing-law]]): setCoreId is called here
            // (ctor, single-threaded) — no AgentRunner is running yet.
            if (workerCoreBase >= 0) {
                workers_.back()->setCoreId(static_cast<int>(workerCoreBase + i));
            }
        }
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
        // SHARED-NOTHING RULE: only USER partitions (topicId >= 0) are owned by
        // workers. Internal / control-plane partitions (topicId < 0, e.g.
        // __cluster_metadata topicId=-3, __consumer_offsets topicId=-1,
        // __transaction_state topicId=-2) remain driven by the CONTROL THREAD
        // (NodeRuntimeAgent::doWork).
        //
        // Rationale: MetadataService, GroupCoordinationService, and
        // TransactionCoordinationService all run on the control thread and
        // read/drive the internal replicas (replica->role(), replica->doWork).
        // If those replicas were assigned to workers, the control-thread
        // services would race with the worker's doWork + role changes — a second
        // TSan-confirmed data race (MetadataService::doWork reading role() while
        // the worker executes becomeLeader).
        std::vector<Worker::OwnedSet> perWorker(n);
        for (auto* r : registry.allReplicas()) {
            if (!r) continue;
            if (r->topicId() < 0) continue;  // internal partitions: control-thread-driven
            const int32_t tid = r->topicId();
            const int32_t pid = static_cast<int32_t>(r->partitionId());
            const int32_t wid = ownerWorkerId(tid, pid);
            perWorker[static_cast<std::size_t>(wid)].push_back(r);
        }

        // Hand each worker its new owned set ([[rune-shared-nothing-law]]):
        //   - Before runners start (startup / cooperative / tests): set DIRECTLY
        //     — single-threaded, no other thread touches owned_ yet, and keeps
        //     synchronous owns()/ownedCount() assertions valid immediately.
        //   - After runners start (runtime reassignment, cross-thread): enqueue a
        //     SetOwnership ring op; the worker applies it on ITS OWN thread. Never
        //     a cross-thread write to owned_. Back-pressure: if the ring is full,
        //     skip — reconcile re-runs every duty cycle and ownership is latest-wins
        //     (idempotent), so the next cycle re-delivers.
        bool allDelivered = true;
        for (std::size_t i = 0; i < n; ++i) {
            if (!runnersStarted_) {
                workers_[i]->setOwnedDirect(std::move(perWorker[i]));
            } else {
                WorkerOp op;
                op.type     = WorkerOp::Type::SetOwnership;
                op.ownedSet = std::move(perWorker[i]);
                if (!workers_[i]->tryEnqueueOp(std::move(op)))
                    allDelivered = false;  // full ring — retry next reconcile
            }
        }

        // Seed generation only if fully delivered; otherwise leave the sentinel
        // so reconcile() re-derives next duty cycle (latest-wins, idempotent).
        lastGeneration_ = allDelivered ? registry.generation()
                                       : static_cast<std::uint64_t>(-1);
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
                      rune::consensus::NanoClock&    clock,
                      int64_t                        maxParkUs = 200) {
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
                // Backoff with a configurable maxPark (default 200µs) — caps the
                // per-hop wake latency on the replication round-trip (the 1ms Agrona
                // default was a ~1ms tax at every cross-thread hop under acks=maj).
                idles_.emplace_back(
                    std::make_unique<rune::consensus::BackoffIdleStrategy>(
                        10, 20, std::chrono::microseconds{1},
                        std::chrono::microseconds{maxParkUs}));
                break;
            }
            runners_.emplace_back(
                std::make_unique<rune::consensus::AgentRunner>(
                    *workers_[i], *idles_[i], clock));
            runners_.back()->start();
        }
        // From now on, ownership changes must go via SetOwnership ring ops
        // (workers are live on their own threads — no direct owned_ writes).
        runnersStarted_ = true;
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
        runnersStarted_ = false;  // workers quiesced — direct owned_ writes safe again
    }

private:
    rune::metadata::ShardRouter          router_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::uint64_t                        lastGeneration_ = static_cast<std::uint64_t>(-1); // sentinel: not yet assigned

    // Shared-nothing: true once per-worker AgentRunners are started. Before this,
    // ownership is set directly (single-threaded); after, via SetOwnership ring ops.
    bool runnersStarted_ = false;

    // E2 inc2b: per-worker idle strategies and agent runners.
    // Populated by startRunners(); cleared by stopAndJoinRunners().
    std::vector<std::unique_ptr<rune::consensus::IdleStrategy>>  idles_;
    std::vector<std::unique_ptr<rune::consensus::AgentRunner>>   runners_;
};

} // namespace rune::node
