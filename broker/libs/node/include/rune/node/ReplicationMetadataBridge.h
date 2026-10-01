#pragma once
// ReplicationMetadataBridge.h — Consensus → replication metadata bridge (GR-2 / MD1-Task-7).
//
// Per-cycle check comparing current (leaderId, leadershipTermId) from the
// consensus layer against last observed values, AND the committed assignment
// map from the MetadataStateMachine against its last-applied snapshot.
//
// Role-driving split (CONTROLLER RULING — FINDING-1):
//
//   INTERNAL partitions (topicId < 0:  -1 __consumer_offsets,
//                                      -2 __transaction_state,
//                                      -3 __cluster_metadata):
//     Follow CLUSTER-CONSENSUS leadership exactly as before:
//       if this node IS the cluster leader → becomeLeader(clusterEpoch, allReplicas)
//       otherwise                          → becomeFollower(clusterEpoch, clustLeaderId)
//     Rationale: __cluster_metadata's own leadership cannot come from the map it
//     holds (bootstrap chicken-and-egg); MD1-4/MD1-6 rely on this path to emit
//     assignments.
//
//   USER partitions (topicId >= 0):
//     Follow the COMMITTED assignment map (apply-committed-only — never pending):
//       if this node IS the assigned leader →
//           becomeLeader(pa.leaderEpoch, pa.isr)    (per-partition epoch + ISR)
//       otherwise, if this node is a replica →
//           becomeFollower(pa.leaderEpoch, pa.leaderNode)
//     The per-partition epoch comes from the map, NOT the cluster term.
//
// Ordering / convergence:
//   Bridge drives internal partitions from cluster leadership →
//   __cluster_metadata becomes Leader on the cluster leader →
//   MetadataService emits+applies assignments into the committed map →
//   subsequent doWork cycles see the committed map and drive USER-partition roles.
//   Map convergence over cycles is expected, not a bug.
//
// Epoch monotonicity:
//   The consensus leadershipTermId is initialized to 0 and only ever increases.
//   We additionally clamp: never let lastObservedEpoch_ decrease.
//   Combined: epoch NEVER regresses across calls.
//
// Stale-epoch fencing:
//   PartitionReplica::onFetchRequest() rejects requests where req.leaderEpoch
//   < local epoch_ (kErrStaleEpoch).  By stamping becomeFollower(newEpoch,...)
//   immediately on a leader change, the old leader's replicas adopt the new
//   epoch and will reject any late FetchRequests from a dethroned leader.
//
// Single-writer: doWork() MUST be called on the AgentRunner duty-cycle thread.
// No locks. No allocation beyond AssignmentMap copy on map change.
//
// Thread-safety note (TSan):
//   The bridge runs entirely on the duty-cycle thread.  MetadataService::doWork()
//   also runs on the duty-cycle thread (NodeRuntimeAgent step 2b).  Both access
//   the MSM's committed map on the SAME thread → single-threaded access → no races.
//   The observedEpoch_ / observedIsLeader_ atomics are only for cross-thread reads
//   by external observers (not modified by other threads during doWork).

#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerOp.h>
#include <rune/replication/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/consensus/Types.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/MetadataStateMachine.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <vector>

namespace rune::node {

// Forward declaration: WorkerPool is defined in WorkerPool.h.
// Not included here to avoid pulling in NodeConfig.h (rune::node::TopicSpec),
// which conflicts with rune::metadata::TopicSpec in tests using both namespaces.
// The routing methods that call WorkerPool::tryEnqueueOp are implemented in
// ReplicationMetadataBridge.cpp where WorkerPool.h is included directly.
class WorkerPool;

class ReplicationMetadataBridge {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param selfId         This node's NodeId.
    /// \param allReplicaIds  All NodeIds in the cluster (used as ISR seed on
    ///                       becomeLeader for INTERNAL partitions).
    /// \param registry       Non-owning reference to the ReplicaRegistry.
    ///                       Must outlive this bridge.
    /// \param msm            Non-owning pointer to the MetadataStateMachine.
    ///                       When non-null, its committed map drives USER-partition
    ///                       roles on each doWork cycle.  May be nullptr (no user
    ///                       partitions driven from map — original behaviour).
    ///                       Must be accessed only on the duty-cycle thread.
    ReplicationMetadataBridge(rune::consensus::NodeId  selfId,
                              std::vector<rune::replication::NodeId> allReplicaIds,
                              ReplicaRegistry&         registry,
                              const rune::metadata::MetadataStateMachine* msm = nullptr) noexcept
        : selfId_(selfId)
        , allReplicaIds_(std::move(allReplicaIds))
        , registry_(registry)
        , msm_(msm)
    {}

    // Non-copyable, non-movable.
    ReplicationMetadataBridge(const ReplicationMetadataBridge&)            = delete;
    ReplicationMetadataBridge& operator=(const ReplicationMetadataBridge&) = delete;

    // -----------------------------------------------------------------------
    // setMsm — update the MSM pointer (called after BrokerNode::start() has
    // wired both MetadataService and the bridge).
    //
    // Must be called on the duty-cycle thread (single-writer invariant).
    // -----------------------------------------------------------------------
    void setMsm(const rune::metadata::MetadataStateMachine* msm) noexcept {
        msm_ = msm;
        // Reset the prev-map snapshot so the next doWork re-derives the full delta.
        prevMap_ = rune::metadata::AssignmentMap{};
    }

    // -----------------------------------------------------------------------
    // setWorkerPool — wire the WorkerPool so role changes are routed to the
    // owning worker's SPSC ingress instead of calling becomeLeader/becomeFollower
    // directly on the control thread.
    //
    // Called from BrokerNode::buildGateway after buildWorkerPool() creates
    // the pool.  When non-null, ALL replica role changes (both INTERNAL and
    // USER partitions) are enqueued as WorkerOp::BecomeLeader/BecomeFollower
    // to the owning worker — eliminating the control-thread ↔ worker-thread
    // data race confirmed by TSan (E2 shared-nothing fix).
    //
    // When nullptr (bridge unit tests, single-thread fallback):
    // becomeLeader/becomeFollower are called DIRECTLY as before.
    //
    // Must be called on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setWorkerPool(WorkerPool* pool) noexcept {
        workerPool_ = pool;
    }

    // -----------------------------------------------------------------------
    // doWork — single duty-cycle step.
    //
    // Called each cycle AFTER consensus.doWork() (so the consensus state is
    // fully updated for this tick) and BEFORE transport.poll() / replica.doWork().
    //
    // Step A — INTERNAL partitions:
    //   If (currentLeaderId, currentEpoch) changed, call becomeLeader or
    //   becomeFollower on every INTERNAL replica (topicId < 0).
    //
    // Step B — USER partitions:
    //   Compute deriveLocalDelta(selfId_, prevMap_) against the current
    //   committed map.  For each changed partition:
    //     - electedLeaders / leaderUpdates → becomeLeader(pa.leaderEpoch, pa.isr)
    //     - followers / deletes            → becomeFollower(pa.leaderEpoch, pa.leaderNode)
    //   Then snapshot prevMap_ = currentMap for the next cycle.
    //
    // Returns 1 if any role-flip was performed in step A or B, 0 otherwise
    // (duty-cycle work count).
    // -----------------------------------------------------------------------
    int doWork(rune::consensus::NodeId  currentLeaderId,
               rune::replication::Epoch currentEpoch) noexcept
    {
        // Epoch MUST be monotonic: guard against any regress (shouldn't happen
        // with Aeron consensus but defensive belt-and-suspenders).
        if (currentEpoch < lastObservedEpoch_) {
            currentEpoch = lastObservedEpoch_;
        }

        int work = 0;

        // ── Step A: INTERNAL partitions ──────────────────────────────────────
        // Drive internal partitions (topicId < 0) from cluster-consensus
        // leadership, exactly as the original bridge did for ALL replicas.
        //
        // Only re-apply if cluster leader or epoch changed.
        const bool clusterChanged =
            (currentLeaderId != lastObservedLeaderId_) ||
            (currentEpoch    != lastObservedEpoch_);

        if (clusterChanged && currentLeaderId >= 0) {
            lastObservedLeaderId_ = currentLeaderId;
            lastObservedEpoch_    = currentEpoch;

            const bool selfIsClusterLeader =
                (currentLeaderId == static_cast<rune::consensus::NodeId>(selfId_));

            bool anyInternalFlip = false;
            for (auto* replica : registry_.allReplicas()) {
                if (!replica) continue;
                if (replica->topicId() >= 0) continue;  // skip user partitions

                if (selfIsClusterLeader) {
                    routeBecomeLeader_(replica, currentEpoch, allReplicaIds_);
                } else {
                    const auto repl_leader =
                        static_cast<rune::replication::NodeId>(currentLeaderId);
                    routeBecomeFollower_(replica, currentEpoch, repl_leader);
                }
                anyInternalFlip = true;
            }

            // Update the atomic mirrors after internal-partition transitions.
            // Both stores use release so observer threads that acquire-load
            // isLeaderAtomic() / lastEpochAtomic() see the effects.
            observedEpoch_.store(lastObservedEpoch_, std::memory_order_relaxed);
            observedIsLeader_.store(selfIsClusterLeader, std::memory_order_release);

            if (anyInternalFlip) {
                work += 1;
            }
        }

        // ── Step B: USER partitions ───────────────────────────────────────────
        // Drive user partitions (topicId >= 0) from the committed assignment map.
        // Requires msm_ to be set (production wiring via setMsm()).
        if (msm_ != nullptr) {
            work += applyUserPartitionRoles(msm_->committedMap());
        }

        return work;
    }

    // -----------------------------------------------------------------------
    // Accessors (for tests / GR-4 per-partition leader lookup)
    // -----------------------------------------------------------------------

    /// The last observed cluster leader id (-1 if no leader seen yet).
    [[nodiscard]] rune::consensus::NodeId lastLeaderId() const noexcept {
        return lastObservedLeaderId_;
    }

    /// The last observed cluster epoch (= leadershipTermId from consensus).
    [[nodiscard]] rune::replication::Epoch lastEpoch() const noexcept {
        return lastObservedEpoch_;
    }

    /// True if this node is currently the cluster leader (from the bridge's perspective).
    /// Observer-thread-safe: uses the atomic observedIsLeader_ mirror.
    [[nodiscard]] bool isLeaderAtomic() const noexcept {
        return observedIsLeader_.load(std::memory_order_acquire);
    }

    /// The last cluster epoch applied to replicas.
    /// Observer-thread-safe: acquire-load pairs with the release-store in doWork().
    [[nodiscard]] rune::replication::Epoch lastEpochAtomic() const noexcept {
        (void)observedIsLeader_.load(std::memory_order_acquire);
        return observedEpoch_.load(std::memory_order_relaxed);
    }

    /// True if this node is currently the cluster leader (from the bridge's perspective).
    /// Duty-cycle-thread-only: reads non-atomic state.
    [[nodiscard]] bool isLeader() const noexcept {
        return lastObservedLeaderId_ >= 0 &&
               lastObservedLeaderId_ == static_cast<rune::consensus::NodeId>(selfId_);
    }

private:
    // -----------------------------------------------------------------------
    // applyUserPartitionRoles
    //
    // Compute msm_->deriveLocalDelta(selfId_, prevMap_) on the current committed map.
    // For each changed user partition, call becomeLeader or becomeFollower.
    // Snapshot prevMap_ for the next cycle.
    //
    // Returns 1 if any role transition was performed, 0 if no-op.
    // Pre-condition: msm_ != nullptr (caller guarantees this).
    // -----------------------------------------------------------------------
    int applyUserPartitionRoles(
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        // Compute delta via the production MetadataStateMachine::deriveLocalDelta.
        rune::metadata::LocalRoleDelta delta =
            msm_->deriveLocalDelta(static_cast<int32_t>(selfId_), prevMap_);

        const bool anyChange =
            !delta.electedLeaders.empty() ||
            !delta.leaderUpdates.empty()  ||
            !delta.followers.empty()      ||
            !delta.deletes.empty();

        if (!anyChange) {
            return 0;
        }

        // Apply role transitions for user partitions only.
        // electedLeaders + leaderUpdates → becomeLeader
        for (const auto& key : delta.electedLeaders) {
            if (key.topicId < 0) continue;  // only user partitions (guard)
            applyBecomeLeaderForKey(key, currentMap);
        }
        for (const auto& key : delta.leaderUpdates) {
            if (key.topicId < 0) continue;
            applyBecomeLeaderForKey(key, currentMap);
        }

        // followers → becomeFollower
        for (const auto& key : delta.followers) {
            if (key.topicId < 0) continue;
            applyBecomeFollowerForKey(key, currentMap);
        }

        // deletes → stop the replica AND quarantine it out of routing (Note B /
        // spec §16).  Must NOT call becomeFollower(0, NO_LEADER) — that would
        // leave the replica visible to routing as a stray.
        for (const auto& key : delta.deletes) {
            if (key.topicId < 0) continue;
            applyDeleteForKey(key, currentMap);
        }

        // Snapshot current committed map for next cycle's delta computation.
        prevMap_ = currentMap;

        return 1;
    }

    // -----------------------------------------------------------------------
    // routeBecomeLeader_ / routeBecomeFollower_
    //
    // When workerPool_ != nullptr: enqueue a BecomeLeader/BecomeFollower op
    // to the owning worker's SPSC ingress so the role change executes on the
    // worker thread (shared-nothing E2 fix — eliminates the control-thread
    // vs worker-thread data race confirmed by TSan).
    //
    // When workerPool_ == nullptr: call becomeLeader/becomeFollower directly
    // (bridge unit tests; single-thread / cooperative fallback).
    //
    // Non-inline: implemented in ReplicationMetadataBridge.cpp to avoid
    // pulling WorkerPool.h (and transitively NodeConfig.h / rune::node::TopicSpec)
    // into headers that also use rune::metadata::TopicSpec (ambiguity).
    // -----------------------------------------------------------------------
    void routeBecomeLeader_(
        rune::replication::PartitionReplica*          replica,
        rune::replication::Epoch                      epoch,
        std::vector<rune::replication::NodeId>        isr) noexcept;

    void routeBecomeFollower_(
        rune::replication::PartitionReplica*  replica,
        rune::replication::Epoch              epoch,
        rune::replication::NodeId             leaderId) noexcept;

    void applyBecomeLeaderForKey(
        const rune::metadata::PartitionKey& key,
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        auto* replica = registry_.lookup(key.topicId, key.partition);
        if (!replica) return;

        const rune::metadata::PartitionAssignment* pa =
            currentMap.get(key.topicId, key.partition);
        if (!pa) return;

        // Use per-partition leaderEpoch and ISR from the committed map.
        const rune::replication::Epoch perPartEpoch =
            static_cast<rune::replication::Epoch>(pa->leaderEpoch);

        std::vector<rune::replication::NodeId> isr;
        isr.reserve(pa->isr.size());
        for (int32_t n : pa->isr) {
            isr.push_back(static_cast<rune::replication::NodeId>(n));
        }

        routeBecomeLeader_(replica, perPartEpoch, std::move(isr));
    }

    void applyBecomeFollowerForKey(
        const rune::metadata::PartitionKey& key,
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        auto* replica = registry_.lookup(key.topicId, key.partition);
        if (!replica) return;

        const rune::metadata::PartitionAssignment* pa =
            currentMap.get(key.topicId, key.partition);

        rune::replication::Epoch  perPartEpoch = 0;
        rune::replication::NodeId leaderNode   = rune::metadata::NO_LEADER;

        if (pa) {
            perPartEpoch = static_cast<rune::replication::Epoch>(pa->leaderEpoch);
            leaderNode   = static_cast<rune::replication::NodeId>(pa->leaderNode);
        }

        routeBecomeFollower_(replica, perPartEpoch, leaderNode);
    }

    // -----------------------------------------------------------------------
    // applyDeleteForKey — Note B fix / spec §16 stray-replica quarantine.
    //
    // Called for each `deletes` delta key: the committed map no longer lists
    // this node as a replica for (topicId, partition).
    //
    // 1. Fence the replica: becomeFollower(lastKnownEpoch, NO_LEADER) stamps the
    //    epoch so any in-flight FetchRequests from this (now-removed) leader will
    //    be stale-epoch-rejected by peers.  Uses the prev map's leaderEpoch when
    //    available (currentMap no longer has an entry for a deleted partition).
    // 2. Remove from registry: registry_.remove() erases the replica from the
    //    routing map so it can never be looked up again.  The replica's unique_ptr
    //    is destroyed; the underlying Log data is NOT touched (owned by topology).
    //
    // Topic/partition ids are never reused so a quarantined log is never
    // resurrected by a later add() with the same key.
    // -----------------------------------------------------------------------
    void applyDeleteForKey(
        const rune::metadata::PartitionKey& key,
        const rune::metadata::AssignmentMap& /*currentMap*/) noexcept
    {
        auto* replica = registry_.lookup(key.topicId, key.partition);
        if (!replica) return;

        // Fence: stamp the replica as a NO_LEADER follower so any late epoch
        // interactions are rejected.  prevMap_ still holds the last-known epoch
        // for this key (the map snapshot before this cycle's delta).
        const rune::metadata::PartitionAssignment* pa =
            prevMap_.get(key.topicId, key.partition);
        const rune::replication::Epoch fenceEpoch =
            pa ? static_cast<rune::replication::Epoch>(pa->leaderEpoch) : 0;

        // Task 10 fix 3: a USER replica is owned by a worker — fence it on the
        // owner's thread (routed role op, FIFO before the SetOwnership that drops
        // it), never by a direct cross-thread call.  Internal: direct (control).
        routeBecomeFollower_(replica, fenceEpoch, rune::metadata::NO_LEADER);

        // Quarantine: erase from routing.  The replica is RETIRED in the registry
        // and freed only after every worker acked the SetOwnership that drops it.
        registry_.remove(key.topicId, key.partition);
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    rune::consensus::NodeId                        selfId_;
    std::vector<rune::replication::NodeId>         allReplicaIds_;
    ReplicaRegistry&                               registry_;

    // Non-owning pointer to the WorkerPool.  When non-null, role changes are
    // routed to the owning worker's SPSC ingress (shared-nothing E2 fix).
    // When null (bridge unit tests / single-thread fallback), becomeLeader/
    // becomeFollower are called directly on the control thread.
    WorkerPool*                                    workerPool_ = nullptr;

    // Production path: non-owning pointer to the MetadataStateMachine.
    // Set via setMsm() after BrokerNode::start() wires the MetadataService.
    // Accessed only on the duty-cycle thread.
    const rune::metadata::MetadataStateMachine*    msm_       = nullptr;

    // Snapshot of the committed map as of the last applyUserPartitionRoles call.
    // Used to compute the delta on subsequent cycles.
    rune::metadata::AssignmentMap                  prevMap_;

    // Last observed consensus state (initialized to "no leader, epoch 0").
    // These are duty-cycle-thread-only (not atomic).
    rune::consensus::NodeId    lastObservedLeaderId_ = -1;
    rune::replication::Epoch   lastObservedEpoch_    = 0;

    // Observer-thread-safe atomic mirrors.
    // observedEpoch_ is relaxed-stored before observedIsLeader_ is release-stored;
    // a reader that acquire-loads observedIsLeader_ will see the relaxed store.
    std::atomic<rune::replication::Epoch> observedEpoch_{0};
    std::atomic<bool>                     observedIsLeader_{false};
};

} // namespace rune::node
