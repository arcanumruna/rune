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
#include <rune/node/ParkUntilWorkIdle.h>
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
#include <type_traits>
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

// A7 (findings §4.12): the shard runner's idle strategy.  Busy-spin ONLY on the explicit
// RUNE_WORKER_IDLE=busyspin opt-in.  Otherwise park-until-work on the agent's readiness fds (`fds`, n > 0:
// driver pollers + io_uring completions) after the Aeron spin / yield phase (review M1); with
// RUNE_IDLE_WAKE=0 or no fds (non-Linux): the legacy Aeron backoff — byte-identical to pre-A7 (kernel-default timer slack) — unless
// RUNE_IDLE_SPINS / RUNE_IDLE_YIELDS set an explicit shape (then a timed ParkUntilWorkIdle of that shape).
//
// A9: `bell` (may be null) — the doorbell of the rings this runner's thread consumes.  With wake fds and
// RUNE_IDLE_DOORBELL on (default) the runner parks on it too, with the doorbell shape
// (NodeConfig::shardIdleShape(true): the Aeron spin / yield phase, then 1 ms parks).
/// A9 review M-1: the runner parks on a ring doorbell — the only case in which any ring gets one.
[[nodiscard]] inline bool shardRunnerUsesDoorbell(const NodeConfig& cfg, int nWakeFds) noexcept {
    return !cfg.shardWorkersBusySpin() && cfg.shardIdleWantsWake() && cfg.idleDoorbell && nWakeFds > 0;
}

inline std::unique_ptr<rune::consensus::IdleStrategy>
makeShardIdle(const NodeConfig& cfg, const int* fds, int n, detail::RingDoorbell* bell = nullptr) {
    if (cfg.shardWorkersBusySpin()) return std::make_unique<rune::consensus::BusySpinIdleStrategy>();
    if (cfg.shardIdleWantsWake() && n > 0) {
        if (shardRunnerUsesDoorbell(cfg, n) && bell != nullptr && bell->valid())
            return std::make_unique<ParkUntilWorkIdle>(cfg.shardIdleShape(/*doorbell=*/true), fds, n, bell);
        return std::make_unique<ParkUntilWorkIdle>(cfg.shardIdleShape(), fds, n);
    }
    if (cfg.idleSpins == NodeConfig::kIdleAuto && cfg.idleYields == NodeConfig::kIdleAuto)
        return makeBackoffIdle(cfg.idleMaxParkUs);
    return std::make_unique<ParkUntilWorkIdle>(cfg.shardIdleShape(), nullptr, 0);
}

/// One line per shard worker naming the RESOLVED idle (proof of the measured path in perf logs).
inline std::string describeShardIdle(const rune::consensus::IdleStrategy& idle) {
    if (const auto* p = dynamic_cast<const ParkUntilWorkIdle*>(&idle)) {
        const IdleShape& s = p->shape();
        return std::string(p->wakeFdCount() > 0 ? "park-until-work" : "timed-park") + "{wakeFds=" +
               std::to_string(p->wakeFdCount()) +
               (p->doorbell() != nullptr ? " doorbell=" + std::to_string(p->doorbell()->watchCount()) + "rings" : "") +
               " spins=" + std::to_string(s.maxSpins) + " yields=" +
               std::to_string(s.maxYields) + " parkNs=" + std::to_string(s.minParkNs) + "-" +
               std::to_string(s.maxParkNs) + "}";
    }
    if (dynamic_cast<const rune::consensus::BusySpinIdleStrategy*>(&idle) != nullptr) return "busy-spin";
    return "backoff{spins=10 yields=20 (legacy Aeron)}";
}

/// A7 review L3: the shard runner's idle strategy as a CONCRETE type — the strategy makeShardIdle picked,
/// dispatched by a well-predicted branch on its kind to the final class (a direct call), never a virtual call
/// per duty pass.  `strategy()` exposes it for describeShardIdle / tests.
class ShardIdle final {
public:
    explicit ShardIdle(std::unique_ptr<rune::consensus::IdleStrategy> s) noexcept : owned_(std::move(s)) {
        if ((park_ = dynamic_cast<ParkUntilWorkIdle*>(owned_.get())) != nullptr) kind_ = Kind::Park;
        else if ((backoff_ = dynamic_cast<rune::consensus::BackoffIdleStrategy*>(owned_.get())) != nullptr)
            kind_ = Kind::Backoff;
        else if ((spin_ = dynamic_cast<rune::consensus::BusySpinIdleStrategy*>(owned_.get())) != nullptr)
            kind_ = Kind::Spin;
    }
    void idle(int workCount) noexcept {
        switch (kind_) {
        case Kind::Park:    park_->idle(workCount); return;      // final classes: direct calls
        case Kind::Backoff: backoff_->idle(workCount); return;
        case Kind::Spin:    spin_->idle(workCount); return;
        case Kind::Other:   owned_->idle(workCount); return;     // never produced by makeShardIdle
        }
    }
    void reset() noexcept {
        switch (kind_) {
        case Kind::Park:    park_->reset(); return;
        case Kind::Backoff: backoff_->reset(); return;
        case Kind::Spin:    spin_->reset(); return;
        case Kind::Other:   owned_->reset(); return;
        }
    }
    [[nodiscard]] const rune::consensus::IdleStrategy& strategy() const noexcept { return *owned_; }
    /// Tests: the strategy is dispatched without a virtual call.
    [[nodiscard]] bool devirtualizedForTest() const noexcept { return kind_ != Kind::Other; }

private:
    enum class Kind : uint8_t { Park, Backoff, Spin, Other };
    std::unique_ptr<rune::consensus::IdleStrategy> owned_;
    Kind                                     kind_    = Kind::Other;
    ParkUntilWorkIdle*                       park_    = nullptr;
    rune::consensus::BackoffIdleStrategy*    backoff_ = nullptr;
    rune::consensus::BusySpinIdleStrategy*   spin_    = nullptr;
};

/// A7 review L3: the shard duty loop — every call direct (final agent, idle and clock).
using ShardRunner = rune::consensus::BasicAgentRunner<ShardAgent, ShardIdle, rune::consensus::MonotonicNanoClock>;
static_assert(std::is_final_v<ShardAgent> && std::is_final_v<ShardIdle> &&
                  std::is_final_v<rune::consensus::MonotonicNanoClock> && std::is_final_v<ParkUntilWorkIdle> &&
                  std::is_final_v<rune::consensus::BackoffIdleStrategy> &&
                  std::is_final_v<rune::consensus::BusySpinIdleStrategy>,
              "the shard runner's calls are direct only through final classes");

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
        // F3 round 6, item 4: every worker is built with the SAME ingress ring
        // size, so the replicated-frame fit check is one pool-level compare.
        maxIngressTail_ = workers_.empty() ? 0 : workers_.front()->maxOpTailBytes();
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

    // Task 20 (spec §11.2 co-location): pin a materialized view's sink topic to
    // its SOURCE's co-partition group, so workerForPartition(viewTopicId, p) ==
    // workerForPartition(sourceTopicId, p).  The per-worker WorkerViewSet folds a
    // view PARTITION from the worker that OWNS that source partition (driveViews_
    // iterates owned source replicas), so the view table for (viewTopicId, p)
    // lives on workerForPartition(sourceTopicId, p).  Without this mapping a
    // runtime-created view sink is a singleton group (groupKey == viewTopicId),
    // and a GetViewRow routed by viewTopicId would land on a worker that folded
    // nothing for the view — a SILENT AbsentKey for a live key when workerCount>1.
    //
    // Control-thread only: the ShardRouter's group map is written here (at view
    // creation / fan-out) and read only by routing (ownerWorkerId / tryEnqueueOp)
    // on the SAME thread; workers never read the router at runtime (doWork
    // iterates already-assigned replicas).  Shared-nothing preserved.
    void coLocateViewWithSource(int32_t viewTopicId, int32_t sourceTopicId) noexcept {
        router_.mapTopicToGroupKey(viewTopicId, router_.groupKeyForTopic(sourceTopicId));
    }
    /// Catalog history Task 8 (M5): the catalog dropped the topic (a view sink
    /// among them, after its uninstall) and its replicas are retired — forget
    /// its routing group.  Control thread.
    void forgetTopicRoute(int32_t topicId) noexcept { router_.unmapTopic(topicId); }

    /// F18: does an op with a `tailLen`-byte tail fit ONE frame of the owning
    /// worker's ingress ring?  (See Worker::opTailFits.)  Called once per follower
    /// FetchResponse on the control thread: the router lookup is pure arithmetic
    /// (ShardRouter::workerForPartition), the same one tryEnqueueOp already does.
    [[nodiscard]] bool opTailFits(int32_t topicId, int32_t partition, std::size_t tailLen) const {
        return workers_[static_cast<std::size_t>(router_.workerForPartition(topicId, partition))]
            ->opTailFits(tailLen);
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
        computeAssignment_(registry);
        deliverOwnership_();
    }

    // -----------------------------------------------------------------------
    // Task 10 round 4 (Important 1): ownership fan-out is keyed on the registry
    // GENERATION and tracked PER WORKER.
    //   computeAssignment_  — cold, ONCE per generation: the per-worker owned
    //                         sets + the F7 invariant + the owner-map bound, all
    //                         O(replicas) (no owner-map × replicas scan).
    //   deliverOwnership_   — (re)sends the generation's SetOwnership ONLY to the
    //                         workers that have not received it yet (a ring-full
    //                         worker never causes a re-send to the others).
    // Routed ops never trigger a re-computation: tryEnqueueOp re-computes only
    // when the generation actually moved, and otherwise at most retries the
    // single target worker's pending SetOwnership.
    // -----------------------------------------------------------------------
    void computeAssignment_(const ReplicaRegistry& registry) {
        const std::size_t n = static_cast<std::size_t>(workerCount());
        const std::uint64_t gen = registry.generation();
        ++assignmentsComputed_;

        std::vector<Worker::OwnedSet> perWorker(n);
        for (auto* r : registry.allReplicas()) {
            if (!r) continue;
            if (r->topicId() < 0) continue;  // internal partitions: control-thread-driven
            const int32_t tid = r->topicId();
            const int32_t pid = static_cast<int32_t>(r->partitionId());
            const int32_t wid = ownerWorkerId(tid, pid);
            perWorker[static_cast<std::size_t>(wid)].push_back(r);
            if (r->incarnation() != 0) {
                auto [it, inserted] = ownerOfIncarnation_.emplace(r->incarnation(), OwnerRec{wid, 0, 0});
                if (!inserted && it->second.worker != wid && runnersStarted_)
                    throw std::logic_error("WorkerPool: replica topic=" + std::to_string(tid) +
                                           " partition=" + std::to_string(pid) + " would MOVE from worker " +
                                           std::to_string(it->second.worker) + " to " + std::to_string(wid) +
                                           " at runtime — unsupported (ownership invariant)");
                it->second.worker   = wid;
                it->second.seenAt   = computeSeq_ + 1;
                it->second.removedAt = 0;
            }
        }
        ++computeSeq_;
        // Bound the invariant map: an incarnation no longer registered is
        // erased once every worker acked an ownership gen >= this one (the same
        // ack that frees the retired replica).  One pass over the map.
        for (auto& [inc, rec] : ownerOfIncarnation_) {
            if (rec.seenAt == computeSeq_ || rec.removedAt != 0) continue;
            rec.removedAt = gen == 0 ? 1 : gen;
            pendingOwnerErase_.emplace_back(gen, inc);
        }

        pendingSets_ = std::move(perWorker);
        computedGen_ = gen;
        deliveredGen_.resize(n, kNoGen);
        ackedGen_.resize(n, 0);
        setOwnershipSent_.resize(n, 0);
        for (std::size_t i = 0; i < n; ++i) deliveredGen_[i] = kNoGen;   // this generation: nobody yet
    }

    /// Deliver the computed generation to every worker that has not received it.
    /// Returns true when all workers have it.
    bool deliverOwnership_() {
        bool all = true;
        for (std::size_t i = 0; i < workers_.size(); ++i) all = deliverTo_(i) && all;
        pruneOwnerMap_();
        return all;
    }
    bool deliverTo_(std::size_t i) {
        if (deliveredGen_[i] == computedGen_) return true;
        if (!runnersStarted_) {
            workers_[i]->setOwnedDirect(pendingSets_[i]);
            deliveredGen_[i] = computedGen_;
            ackedGen_[i]     = computedGen_;   // applied synchronously: nothing to wait for
            return true;
        }
        WorkerOp op;
        op.type         = WorkerOp::Type::SetOwnership;
        op.ownedSet     = pendingSets_[i];      // copy: a refused enqueue keeps the set for the retry
        op.ownershipGen = computedGen_;
        const std::size_t setBytes = op.ownedSet.size() * sizeof(uintptr_t);
        const EnqueueResult r = workers_[i]->enqueueOp(std::move(op));
        if (r == EnqueueResult::TooLarge) [[unlikely]] {
            // F3 round 6, item 5: an owned set that can NEVER fit the worker
            // ingress ring is not back-pressure — retrying it forever would
            // silently freeze ownership.  Fail-stop like the F7 invariant (the
            // control loop halts the broker).
            const std::string what =
                "WorkerPool: SetOwnership for worker " + std::to_string(i) + " (" +
                std::to_string(setBytes) + " B owned set) can never fit the worker "
                "ingress ring (max tail " + std::to_string(maxIngressTail_) +
                " B) — raise RUNE_WORKER_INGRESS_RING_BYTES";
            spdlog::critical("{}", what);
            throw std::logic_error(what);
        }
        if (r != EnqueueResult::Enqueued) return false;   // full ring — retried later
        deliveredGen_[i] = computedGen_;
        ++setOwnershipSent_[i];
        return true;
    }

    void pruneOwnerMap_() {
        if (pendingOwnerErase_.empty()) return;
        const std::uint64_t safe = minAckedOwnershipGen();
        for (std::size_t i = 0; i < pendingOwnerErase_.size(); ) {
            if (pendingOwnerErase_[i].first <= safe) {
                ownerOfIncarnation_.erase(pendingOwnerErase_[i].second);
                pendingOwnerErase_[i] = pendingOwnerErase_.back();
                pendingOwnerErase_.pop_back();
            } else {
                ++i;
            }
        }
    }

    /// Runtime (control thread) — a worker applied SetOwnership(gen).
    void onOwnershipAck(int32_t workerId, std::uint64_t gen) noexcept {
        if (workerId < 0 || static_cast<std::size_t>(workerId) >= ackedGen_.size()) return;
        if (gen > ackedGen_[static_cast<std::size_t>(workerId)])
            ackedGen_[static_cast<std::size_t>(workerId)] = gen;
        pruneOwnerMap_();
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
    /// Round 5: the F7 owner map still tracks `inc` (live, or removed but not
    /// yet erased because a worker has not acked).  ReplicaRegistry consults it
    /// after an incarnation wrap (setIncarnationInUseHook).
    [[nodiscard]] bool tracksIncarnation(uint32_t inc) const noexcept {
        return ownerOfIncarnation_.find(inc) != ownerOfIncarnation_.end();
    }
    static bool tracksIncarnationThunk(const void* self, uint32_t inc) noexcept {
        return static_cast<const WorkerPool*>(self)->tracksIncarnation(inc);
    }
    /// Test-only: treat the pool as running (SetOwnership goes over the rings)
    /// without spawning runner threads; tests drive the workers themselves.
    void setRunnersStartedForTest(bool on) noexcept { runnersStarted_ = on; }
    /// Test-only: record that `incarnation` was owned by `workerId` (to exercise
    /// the F7 runtime-move refusal, which no production router can trigger).
    void seedOwnerForTest(uint32_t incarnation, int32_t workerId) { ownerOfIncarnation_[incarnation] = OwnerRec{workerId, 0, 0}; }
    /// Important 1 observability: full re-computations of the owned sets, and
    /// SetOwnership ops delivered to worker `w`.
    [[nodiscard]] uint64_t assignmentsComputedForTest() const noexcept { return assignmentsComputed_; }
    [[nodiscard]] uint64_t setOwnershipSentForTest(int32_t w) const noexcept {
        return static_cast<std::size_t>(w) < setOwnershipSent_.size() ? setOwnershipSent_[static_cast<std::size_t>(w)] : 0;
    }
    /// Ops routed for a (topic, partition) no longer in the registry: stamped
    /// stale (ReplicaRegistry::kStaleIncarnation) and refused by the worker.
    [[nodiscard]] uint64_t staleOpsRoutedForTest() const noexcept { return staleOpsRouted_; }
    [[nodiscard]] std::size_t ownerMapSizeForTest() const noexcept { return ownerOfIncarnation_.size(); }

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
        demote.targetReplicaId = r->registryId(); demote.incarnation = r->incarnation();
        WorkerOp own;
        own.type = WorkerOp::Type::SetOwnership;
        own.ownedSet = std::move(remaining);
        return static_cast<bool>(from.tryEnqueueOp(std::move(demote))) &&
               static_cast<bool>(from.tryEnqueueOp(std::move(own)));
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
        lead.targetReplicaId = r->registryId(); lead.incarnation = r->incarnation();
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
    // F3 round 5, item I1 / round 6, item 4: true when a variable tail of
    // tailLen bytes fits the workers' ingress ring — ONE compare against the
    // pool-level cached max (all workers share one ring size).
    [[nodiscard]] bool fitsIngressTail(std::size_t tailLen) const noexcept {
        return tailLen <= maxIngressTail_;
    }
    [[nodiscard]] std::size_t maxIngressTail() const noexcept { return maxIngressTail_; }

    [[nodiscard]] bool tryEnqueueOp(int32_t topicId, int32_t partition,
                                    WorkerOp op) {
        return enqueueOp(topicId, partition, std::move(op)) == EnqueueResult::Enqueued;
    }

    /// F18 (m-1): tryEnqueueOp with the failure kind exposed (Full vs TooLarge).
    [[nodiscard]] EnqueueResult enqueueOp(int32_t topicId, int32_t partition,
                                          WorkerOp op) {
        // Task 10 fix 3 (I2): never route an op ahead of the SetOwnership that
        // governs it.  Round 4 (Important 1): re-compute ONLY when the registry
        // generation moved (once per generation, cold); otherwise retry at most
        // the target worker's own pending SetOwnership.  Back-pressure (false —
        // callers retry) while it is still undelivered (its ring is full).
        if (registry_ && registry_->generation() != computedGen_) reconcile(*registry_);
        const int32_t wid = ownerWorkerId(topicId, partition);
        const auto w = static_cast<std::size_t>(wid);
        if (w < deliveredGen_.size() && deliveredGen_[w] != computedGen_ && !deliverTo_(w)) [[unlikely]]
            return EnqueueResult::Full;
        // Task 10 round 4 (Critical 1): the target identity comes from the
        // registry at ROUTING time — never from a pointer a caller held across
        // cycles.  A (topic, partition) no longer registered is stamped stale:
        // the worker refuses it through the op's normal response path, and
        // nothing it carries is ever dereferenced.
        if (registry_ && isPartitionOp_(op.type)) {
            if (const auto h = registry_->lookupHandle(topicId, partition)) {
                op.replica         = h.replica;
                op.targetReplicaId = h.id;
                op.incarnation     = h.incarnation;
            } else {
                op.replica         = nullptr;
                op.targetReplicaId = 0;
                op.incarnation     = ReplicaRegistry::kStaleIncarnation;
                ++staleOpsRouted_;
            }
        }
        return workers_[w]->enqueueOp(std::move(op));
    }

    /// Ops that target one partition replica (ownership-gated on the worker).
    static constexpr bool isPartitionOp_(WorkerOp::Type t) noexcept {
        switch (t) {
        case WorkerOp::Type::Produce:
        case WorkerOp::Type::PushRead:
        case WorkerOp::Type::ApplyReplicated:
        case WorkerOp::Type::BecomeLeader:
        case WorkerOp::Type::BecomeFollower:
        case WorkerOp::Type::FetchRequest:
        case WorkerOp::Type::OffsetsForLeaderEpochReq:
        case WorkerOp::Type::ReconcileFetchResp:
        case WorkerOp::Type::OffsetsForLeaderEpochResp:
        case WorkerOp::Type::ReconcileFetchReq:
        case WorkerOp::Type::AlterIsrResponse:
        case WorkerOp::Type::AppendControlRecord:
        case WorkerOp::Type::Subscribe:
            return true;
        default:
            return false;
        }
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
    /// F5: zero-copy variant — chunks arrive as WorkerReturn::chunkView, valid
    /// only inside `fn` (Worker::drainReturnView).
    template <typename Fn>
    std::size_t drainAllReturnsView(Fn&& fn) {
        std::size_t total = 0;
        for (auto& w : workers_) total += w->drainReturnView(fn);
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
        if (registry.generation() != computedGen_) computeAssignment_(registry);
        // Retry only the workers that have not received this generation yet
        // (O(workers) compare in steady state; nothing is re-sent otherwise).
        bool pending = false;
        for (auto g : deliveredGen_) pending = pending || g != computedGen_;
        if (pending) deliverOwnership_();
    }

    // -----------------------------------------------------------------------
    /// Worker w's runner thread id (default id when not started).  Controller
    /// only, after the runners started (fused placement: worker 0 hosts the
    /// runtime — BrokerNode records it as the control thread).
    [[nodiscard]] std::thread::id runnerThreadId(std::size_t w) const noexcept {
        if (w < shardRunners_.size() && shardRunners_[w]) return shardRunners_[w]->threadId();
        if (w < runners_.size() && runners_[w]) return runners_[w]->threadId();
        return {};
    }

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
        if (!runners_.empty() || !shardRunners_.empty()) return; // idempotent

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
        // P2: BrokerNode arms ring ownership BEFORE the control thread starts
        // (armRingOwnership), so in production this is a no-op read; only
        // control-thread-less tests (no concurrent reader) take the write.
        if (!runnersStarted_) runnersStarted_ = true;
    }

    // Shard mode (spec §3.1): one runner per ShardAgent.  Busy-spin ONLY when the
    // worker is pinned to its own core; otherwise the backoff idle (§1a.3).
    // Idempotent (no-op if runners are already started).  Throws (startup
    // refusal) when there is not exactly one agent per worker.
    void startShardRunners(std::vector<std::unique_ptr<ShardAgent>>& agents,
                           rune::consensus::MonotonicNanoClock& clock,
                           const NodeConfig& cfg) {
        if (!runners_.empty() || !shardRunners_.empty()) return;
        if (agents.size() != workers_.size())      // hard check in release too (startup refusal)
            throw std::logic_error("WorkerPool::startShardRunners: " + std::to_string(agents.size()) +
                                   " ShardAgent(s) for " + std::to_string(workers_.size()) + " worker(s)");
        shardIdles_.reserve(agents.size());
        shardRunners_.reserve(agents.size());
        for (std::size_t i = 0; i < agents.size(); ++i) {
            auto& a = agents[i];
            // A7: park on this shard's driver readiness (read here, before its runner owns the pump).
            int fds[ParkUntilWorkIdle::kMaxWakeFds];
            const int nfds = a->wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds - 1);   // one slot for the doorbell
            shardIdles_.emplace_back(std::make_unique<ShardIdle>(
                makeShardIdle(cfg, fds, nfds, a->doorbellEnabled() ? &a->doorbell() : nullptr)));
            // WARN: visible in perf runs (RUNE_LOG_LEVEL=warn) — the RESOLVED idle of this worker.
            spdlog::warn("WorkerPool: shard worker {} idle={}", i, describeShardIdle(shardIdles_.back()->strategy()));
            shardRunners_.emplace_back(std::make_unique<ShardRunner>(*a, *shardIdles_.back(), clock));
            shardRunners_.back()->start();
        }
        if (!runnersStarted_) runnersStarted_ = true;   // see startRunners (P2)
    }

    // A9 (review M-1): wire each shard worker's ring doorbell iff its runner will park on it (wake fds +
    // RUNE_IDLE_DOORBELL, never busy-spin).  STARTUP ONLY: before the control thread (thread placement), the
    // runners, the admin / cleaner threads and the fused-runtime attach (BrokerNode::start).  `fused`: worker
    // 0's ret_ is consumed on its own thread — no space doorbell there (review L-1).  Returns how many.
    int enableShardDoorbells(std::vector<std::unique_ptr<ShardAgent>>& agents, const NodeConfig& cfg, bool fused) {
        int enabled = 0;
        for (std::size_t i = 0; i < agents.size(); ++i) {
            int fds[ParkUntilWorkIdle::kMaxWakeFds];
            if (!shardRunnerUsesDoorbell(cfg, agents[i]->wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds - 1))) continue;
            agents[i]->enableDoorbell(/*retSpaceWait=*/!(fused && i == 0));
            enabled += agents[i]->doorbellEnabled() ? 1 : 0;
        }
        return enabled;
    }

    // -----------------------------------------------------------------------
    // P2 (Task 10 round 4): runnersStarted_ is CONTROL-THREAD state (read by
    // every reconcile / routed op).  It is only ever written while no control
    // thread runs:
    //   armRingOwnership()       — BrokerNode::start, BEFORE agentRunner_->start()
    //                              (single-threaded startup).  From then on every
    //                              ownership change goes over the rings; ops queued
    //                              before a worker's runner starts are applied
    //                              in FIFO order when it does.
    //   disarmAfterControlJoined() — BrokerNode::stop, AFTER agentRunner_->
    //                              stopAndJoin() (the control thread is joined).
    // stopAndJoinRunners() no longer touches it (it ran while the control
    // thread was still live: a cross-thread plain write — a data race).
    // -----------------------------------------------------------------------
    void armRingOwnership() noexcept { runnersStarted_ = true; }
    void disarmAfterControlJoined() noexcept { runnersStarted_ = false; }
    [[nodiscard]] bool ringOwnershipArmedForTest() const noexcept { return runnersStarted_; }

    // Stop and join every worker AgentRunner.  Called in BrokerNode::stop()
    // BEFORE runtimeAgent_ and replicaRegistry_ are torn down (workers hold
    // non-owning replica pointers; they must be quiesced first).
    //
    // Idempotent + safe if startRunners() was never called.
    void stopAndJoinRunners() {
        for (auto& r : runners_) {
            if (r) r->stopAndJoin();
        }
        for (auto& r : shardRunners_) {
            if (r) r->stopAndJoin();
        }
        runners_.clear();
        idles_.clear();
        shardRunners_.clear();
        shardIdles_.clear();
        // P2: runnersStarted_ is NOT written here — the control thread may still
        // be running (BrokerNode stops workers first).  disarmAfterControlJoined()
        // resets it once the control thread is joined.
    }

private:
    rune::metadata::ShardRouter          router_;
    std::vector<std::unique_ptr<Worker>> workers_;
    static constexpr std::uint64_t       kNoGen = static_cast<std::uint64_t>(-1);
    std::uint64_t                        computedGen_ = kNoGen;   // generation the pending sets reflect
    const ReplicaRegistry*               registry_ = nullptr;
    // Per worker: the computed generation's owned set.  These are raw replica
    // pointers held across cycles by the control thread — sanctioned against the
    // round-4 design ruling because they are (a) RECOMPUTED from the registry on
    // every generation change (computeAssignment_), so they only ever name
    // replicas live in the CURRENT generation, and (b) NEVER dereferenced on the
    // control thread: they are only copied into a SetOwnership op, and the worker
    // that receives them owns those replicas.  A replica removed at gen G is
    // freed only after every worker acked >= G, i.e. after any set naming it
    // has been superseded.
    std::vector<Worker::OwnedSet>        pendingSets_;
    std::vector<std::uint64_t>           deliveredGen_;           // per worker: generation last ENQUEUED
    std::vector<std::uint64_t>           ackedGen_;               // per worker: latest OwnershipAck
    std::vector<uint64_t>                setOwnershipSent_;       // per worker: SetOwnership ops delivered
    uint64_t                             assignmentsComputed_ = 0;
    uint64_t                             staleOpsRouted_      = 0;
    uint64_t                             computeSeq_          = 0;
    struct OwnerRec {
        int32_t  worker    = -1;
        uint64_t seenAt    = 0;   // computeSeq_ of the last computation that saw it live
        uint64_t removedAt = 0;   // registry gen it was found removed (0 = live)
    };
    std::unordered_map<uint32_t, OwnerRec> ownerOfIncarnation_;  // F7 invariant (cold, bounded: live replicas)
    std::vector<std::pair<std::uint64_t, uint32_t>> pendingOwnerErase_;   // (removal gen, incarnation)

    // Shared-nothing: true once per-worker AgentRunners are started. Before this,
    // ownership is set directly (single-threaded); after, via SetOwnership ring ops.
    bool runnersStarted_ = false;
    // F3 round 6, item 4: max WorkerOp tail of every worker's ingress ring.
    std::size_t maxIngressTail_ = 0;

    // E2 inc2b: per-worker idle strategies and agent runners.
    // Populated by startRunners(); cleared by stopAndJoinRunners().
    std::vector<std::unique_ptr<rune::consensus::IdleStrategy>>  idles_;
    std::vector<std::unique_ptr<rune::consensus::AgentRunner>>   runners_;
    std::vector<std::unique_ptr<ShardIdle>>                      shardIdles_;     ///< A7 L3: shard mode
    std::vector<std::unique_ptr<ShardRunner>>                    shardRunners_;   ///< A7 L3: shard mode
};

} // namespace rune::node
