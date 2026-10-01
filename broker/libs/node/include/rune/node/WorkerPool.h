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
#include <rune/node/ShardAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/metadata/ShardRouter.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::node {

// Backoff idle shared by the worker + runtime loops.  Fixed shape (10 spins,
// 20 yields, 1 µs → maxParkUs park) — identical to the two former inline
// constructions; this helper only de-duplicates them.
inline std::unique_ptr<rune::consensus::BackoffIdleStrategy>
makeBackoffIdle(int64_t maxParkUs) {
    return std::make_unique<rune::consensus::BackoffIdleStrategy>(
        10, 20, std::chrono::microseconds{1},
        std::chrono::microseconds{maxParkUs});
}

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
    // -----------------------------------------------------------------------
    // F7 (Task 10 fix 3) — OWNERSHIP INVARIANT.  The router is fixed for the
    // pool's lifetime (deterministic (topic, partition) → worker), so NO
    // production path moves an existing replica from one worker to another once
    // runners have started: replicas are only ADDED (to their router worker) or
    // REMOVED (retired in the registry, freed after every worker acked).  A
    // runtime move is a hard error (std::logic_error → the control loop halts
    // the broker).  The only sanctioned migration is the explicit test-only
    // two-phase API below (releaseReplicaForTestMigration / grant...).
    // -----------------------------------------------------------------------
    void assignFromRegistry(const ReplicaRegistry& registry) {
        const std::size_t n = static_cast<std::size_t>(workerCount());

        std::vector<Worker::OwnedSet> perWorker(n);
        for (auto* r : registry.allReplicas()) {
            if (!r) continue;
            if (r->topicId() < 0) continue;  // internal partitions: control-thread-driven
            const int32_t tid = r->topicId();
            const int32_t pid = static_cast<int32_t>(r->partitionId());
            const int32_t wid = ownerWorkerId(tid, pid);
            perWorker[static_cast<std::size_t>(wid)].push_back(r);
            if (r->incarnation() != 0) {
                auto [it, inserted] = ownerOfIncarnation_.emplace(r->incarnation(), wid);
                if (!inserted && it->second != wid && runnersStarted_)
                    throw std::logic_error("WorkerPool: replica topic=" + std::to_string(tid) +
                                           " partition=" + std::to_string(pid) + " would MOVE from worker " +
                                           std::to_string(it->second) + " to " + std::to_string(wid) +
                                           " at runtime — unsupported (ownership invariant)");
                it->second = wid;
            }
        }

        const std::uint64_t gen = registry.generation();
        bool allDelivered = true;
        ownershipDelivered_.resize(n, true);
        ackedGen_.resize(n, 0);
        for (std::size_t i = 0; i < n; ++i) {
            if (!runnersStarted_) {
                workers_[i]->setOwnedDirect(std::move(perWorker[i]));
                ownershipDelivered_[i] = true;
                ackedGen_[i] = gen;            // applied synchronously: nothing to wait for
            } else {
                WorkerOp op;
                op.type         = WorkerOp::Type::SetOwnership;
                op.ownedSet     = std::move(perWorker[i]);
                op.ownershipGen = gen;
                ownershipDelivered_[i] = workers_[i]->tryEnqueueOp(std::move(op));
                if (!ownershipDelivered_[i]) allDelivered = false;  // full ring — retry next reconcile
            }
        }

        lastGeneration_ = allDelivered ? gen : static_cast<std::uint64_t>(-1);
    }

    /// Runtime (control thread) — a worker applied SetOwnership(gen).
    void onOwnershipAck(int32_t workerId, std::uint64_t gen) noexcept {
        if (workerId < 0 || static_cast<std::size_t>(workerId) >= ackedGen_.size()) return;
        if (gen > ackedGen_[static_cast<std::size_t>(workerId)])
            ackedGen_[static_cast<std::size_t>(workerId)] = gen;
    }
    /// Removed replicas at or below this generation are unreachable by any worker.
    [[nodiscard]] std::uint64_t minAckedOwnershipGen() const noexcept {
        if (ackedGen_.empty()) return 0;
        std::uint64_t m = ackedGen_[0];
        for (auto g : ackedGen_) m = g < m ? g : m;
        return m;
    }
    /// Ops are only routed once the registry is wired: tryEnqueueOp reconciles
    /// inline first (SetOwnership always precedes the ops it governs).
    void setRegistry(const ReplicaRegistry* registry) noexcept { registry_ = registry; }
    /// Test-only: treat the pool as running (SetOwnership goes over the rings)
    /// without spawning runner threads; tests drive the workers themselves.
    void setRunnersStartedForTest(bool on) noexcept { runnersStarted_ = on; }
    /// Test-only: record that `incarnation` was owned by `workerId` (to exercise
    /// the F7 runtime-move refusal, which no production router can trigger).
    void seedOwnerForTest(uint32_t incarnation, int32_t workerId) { ownerOfIncarnation_[incarnation] = workerId; }

    // ---- Test-only two-phase migration (the ONLY sanctioned move) -----------
    // Release: the losing worker demotes the replica and drops it from its owned
    // set (FIFO: anything queued before is processed first).  The caller drives
    // the losing worker until ownedCount() reflects the release and its flushes
    // drained, and only THEN grants: the gaining worker is told it owns the
    // replica BEFORE it is asked to lead it (SetOwnership precedes role ops).
    static bool releaseReplicaForTestMigration(Worker& from, rune::replication::PartitionReplica* r,
                                               Worker::OwnedSet remaining,
                                               rune::replication::Epoch epoch,
                                               rune::replication::NodeId newLeader) {
        WorkerOp demote;
        demote.type = WorkerOp::Type::BecomeFollower;
        demote.topicId = r->topicId(); demote.partition = static_cast<int32_t>(r->partitionId());
        demote.replica = r; demote.roleEpoch = epoch; demote.roleLeaderId = newLeader;
        WorkerOp own;
        own.type = WorkerOp::Type::SetOwnership;
        own.ownedSet = std::move(remaining);
        return from.tryEnqueueOp(std::move(demote)) && from.tryEnqueueOp(std::move(own));
    }
    static bool grantReplicaForTestMigration(Worker& to, rune::replication::PartitionReplica* r,
                                             Worker::OwnedSet owned,
                                             rune::replication::Epoch epoch,
                                             std::vector<rune::replication::NodeId> isr) {
        WorkerOp own;
        own.type = WorkerOp::Type::SetOwnership;
        own.ownedSet = std::move(owned);
        WorkerOp lead;
        lead.type = WorkerOp::Type::BecomeLeader;
        lead.topicId = r->topicId(); lead.partition = static_cast<int32_t>(r->partitionId());
        lead.replica = r; lead.roleEpoch = epoch; lead.isr = std::move(isr);
        return to.tryEnqueueOp(std::move(own)) && to.tryEnqueueOp(std::move(lead));
    }

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
        // Task 10 fix 3 (I2): never route an op ahead of the SetOwnership that
        // governs it.  Reconcile inline when the registry moved (control thread:
        // every caller of this method runs there), and back-pressure (false —
        // callers retry) while the target worker's latest SetOwnership is still
        // undelivered (its ring was full during reconcile).
        if (registry_ && registry_->generation() != lastGeneration_) assignFromRegistry(*registry_);
        const int32_t wid = ownerWorkerId(topicId, partition);
        const auto w = static_cast<std::size_t>(wid);
        if (w < ownershipDelivered_.size() && !ownershipDelivered_[w]) [[unlikely]] return false;
        return workers_[w]->tryEnqueueOp(std::move(op));
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
                idles_.emplace_back(makeBackoffIdle(maxParkUs));
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

    // Shard mode (spec §3.1): one runner per ShardAgent.  Busy-spin ONLY when the
    // worker is pinned to its own core; otherwise the backoff idle (§1a.3).
    // Idempotent (no-op if runners are already started).  Throws (startup
    // refusal) when there is not exactly one agent per worker.
    void startShardRunners(std::vector<std::unique_ptr<ShardAgent>>& agents,
                           rune::consensus::NanoClock& clock,
                           bool busySpin, int64_t maxParkUs) {
        if (!runners_.empty()) return;
        if (agents.size() != workers_.size())      // hard check in release too (startup refusal)
            throw std::logic_error("WorkerPool::startShardRunners: " + std::to_string(agents.size()) +
                                   " ShardAgent(s) for " + std::to_string(workers_.size()) + " worker(s)");
        idles_.reserve(agents.size());
        runners_.reserve(agents.size());
        for (auto& a : agents) {
            if (busySpin) idles_.emplace_back(std::make_unique<rune::consensus::BusySpinIdleStrategy>());
            else          idles_.emplace_back(makeBackoffIdle(maxParkUs));
            runners_.emplace_back(std::make_unique<rune::consensus::AgentRunner>(*a, *idles_.back(), clock));
            runners_.back()->start();
        }
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
    const ReplicaRegistry*               registry_ = nullptr;
    std::vector<bool>                    ownershipDelivered_;   // per worker: latest SetOwnership enqueued
    std::vector<std::uint64_t>           ackedGen_;             // per worker: latest OwnershipAck
    std::unordered_map<uint32_t, int32_t> ownerOfIncarnation_;  // F7 invariant (cold)

    // Shared-nothing: true once per-worker AgentRunners are started. Before this,
    // ownership is set directly (single-threaded); after, via SetOwnership ring ops.
    bool runnersStarted_ = false;

    // E2 inc2b: per-worker idle strategies and agent runners.
    // Populated by startRunners(); cleared by stopAndJoinRunners().
    std::vector<std::unique_ptr<rune::consensus::IdleStrategy>>  idles_;
    std::vector<std::unique_ptr<rune::consensus::AgentRunner>>   runners_;
};

} // namespace rune::node
