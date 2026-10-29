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
//     Re-review 2026-10-04 MEDIUM-B: with setCoordinatorPartitionsOnController
//     (multi-node), -1 / -2 instead follow the committed map like USER
//     partitions (the controller assigns them — see
//     MetadataService::decideCoordinatorPartition); only -3 stays here.
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
//   No cross-thread mirror (fix-on-sight, catalog history Task 6 — hard rule 1:
//   the former observedEpoch_ / observedIsLeader_ atomics are gone): observers
//   read isLeader() / lastEpoch() ON the control thread (BrokerNode's
//   replicationIsLeaderForTest / replicationEpochForTest post a control task).

#include <rune/node/AdminChannels.h>      // AdminRoleMsg (catalog history Task 6)
#include <array>
#include <rune/node/CatalogPartition.h>   // kCatalogTopicId
#include <rune/node/SecurityPartition.h>  // kSecurityTopicId (auth Task 9)
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerOp.h>
#include <rune/replication/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/consensus/Types.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/MetadataStateMachine.h>

#include <deque>
#include <string>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace rune::node {
class StorageGate;   // ISR v3 Step 10 (the bridge reads open(); defined in StorageGate.h)


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

    /// ISR v3 (Step 9): the storage gate changed state (founded / confirmed / unconfirmed): the next doWork re-derives
    /// every role from the committed map (a held leader role becomes a real one when the gate opens, and the other
    /// way round).  Step 10 adds the hold itself.  Control thread.
    void onStorageGateChanged() noexcept { prevMap_ = rune::metadata::AssignmentMap{}; }
    /// ISR v3 Step 10: the storage gate (null = open everywhere: single node).  Read on the control thread only.
    void setStorageGate(const StorageGate* gate) noexcept { gate_ = gate; }
    [[nodiscard]] uint64_t leaderRolesHeldForTest() const noexcept { return leaderRolesHeld_; }
    /// Leader roles routed as HELD (not led) because the storage gate was closed; monotonic.  Step 10.
    [[nodiscard]] uint64_t leaderRolesHeld() const noexcept { return leaderRolesHeld_; }
    /// Issue #82 (VLS A.5.1): role ops derived for worker `w` that its op ring has not accepted yet.  While any wait, a
    /// view read lease is not handed to that worker (it would overtake a demotion).  Duty-cycle thread.
    [[nodiscard]] bool roleOpsPendingFor(int32_t w) const noexcept {
        return w >= 0 && static_cast<std::size_t>(w) < pendingRoleOps_.size() &&
               !pendingRoleOps_[static_cast<std::size_t>(w)].empty();
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

    /// Checked writes: a user partition's BecomeLeader waits while hold(ctx, topicId) is true - its typed topic's
    /// validators and write spec are not on every worker's ring yet (BrokerNode: SchemaFanout::pendingFor).  The held op
    /// stays queued and is retried every cycle; ops of other replicas pass it.  Duty-cycle thread.
    using LeaderRoleHold = bool (*)(void* ctx, int32_t topicId) noexcept;
    void setLeaderRoleHold(LeaderRoleHold fn, void* ctx) noexcept { leaderHold_ = fn; leaderHoldCtx_ = ctx; }

    /// Re-review 2026-10-04 MEDIUM-B: the coordinator partitions
    /// (__consumer_offsets -1, __transaction_state -2) are on the controller-
    /// ISR path (MetadataService::decideCoordinatorPartition): when on, they are
    /// driven from the COMMITTED assignment map exactly like user partitions
    /// (Step B; never deleted), not from consensus leadership (Step A).
    /// BrokerNode turns it on multi-node.  Duty-cycle thread.
    void setCoordinatorPartitionsOnController(bool on) noexcept { coordinatorOnController_ = on; }

    /// Catalog history Task 6: the __catalog replica (-6, 0) is ADMIN-thread-
    /// owned (never in the registry).  On the controller path its committed
    /// assignment is relayed as an AdminRoleMsg{leader, epoch = leaderEpoch,
    /// leaderId, isr, partitionEpoch} (kMsgCatalogRole) on the admin role ring —
    /// never a direct becomeLeader/becomeFollower call.  This thread (control) is
    /// that ring's single producer.  A full ring (or a ring not wired yet) keeps
    /// the role PENDING (one slot: a newer committed role supersedes it — roles
    /// are absolute) and retries every cycle, never dropped.  Duty-cycle thread.
    void setCatalogRoleRing(OneToOneByteRing* ring) noexcept { catalogRoleRing_ = ring; }
    /// A catalog role is held (ring full / not wired).  Duty-cycle thread — the
    /// controller-answer relay holds its answer behind it (FIFO, fix round 1 M3).
    [[nodiscard]] bool catalogRolePending() const noexcept { return adminRolePending_[0].has_value(); }
    [[nodiscard]] bool catalogRolePendingForTest() const noexcept { return catalogRolePending(); }
    [[nodiscard]] uint64_t catalogRolesPostedForTest() const noexcept { return adminRolesPosted_[0]; }
    /// Authentication Task 9 (D39): the `__security` replica (-7, 0) rides the SAME admin role ring beside
    /// `__catalog`'s (kMsgSecurityRole / kMsgSecurityHoldLeadership); each log has its own held-role slot (a newer
    /// role of one log never supersedes the other's).
    [[nodiscard]] bool securityRolePending() const noexcept { return adminRolePending_[1].has_value(); }
    [[nodiscard]] uint64_t securityRolesPostedForTest() const noexcept { return adminRolesPosted_[1]; }

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

        // Task 10 round-3 addendum: role ops the worker ring could not take
        // (back-pressure / full ring) are retried every cycle, in order, until
        // delivered — never silently dropped.  Time-independent (no timers).
        int work = flushPendingRoleOps_();
        if (adminRolePending_[0] || adminRolePending_[1]) [[unlikely]] work += flushAdminLogRoles_();

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
                // MEDIUM-B: coordinator partitions follow the committed map
                // (Step B), not consensus leadership.
                if (coordinatorOnController_ && isCoordinatorTopic_(replica->topicId())) continue;

                if (selfIsClusterLeader) {
                    routeBecomeLeader_(replica, currentEpoch, allReplicaIds_);
                } else {
                    const auto repl_leader =
                        static_cast<rune::replication::NodeId>(currentLeaderId);
                    routeBecomeFollower_(replica, currentEpoch, repl_leader);
                }
                anyInternalFlip = true;
            }

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
        // Fix-on-sight (catalog history Task 11 fix round 5): a role committed
        // BEFORE this broker built the replica (it applied the topic's create a
        // moment after the controller placed it) was skipped — apply it once the
        // replica is registered, never only at the partition's next change.
        int late = 0;
        if (!rolesWithoutReplica_.empty() && registry_.generation() != lateRoleGen_) [[unlikely]] {
            lateRoleGen_ = registry_.generation();
            for (auto it = rolesWithoutReplica_.begin(); it != rolesWithoutReplica_.end();) {
                const rune::metadata::PartitionKey key{it->first, it->second};
                const auto* pa = currentMap.get(key.topicId, key.partition);
                if (pa == nullptr || std::find(pa->replicas.begin(), pa->replicas.end(),
                                               static_cast<int32_t>(selfId_)) == pa->replicas.end()) {
                    it = rolesWithoutReplica_.erase(it);   // no longer ours: nothing to apply
                    continue;
                }
                if (registry_.lookup(key.topicId, key.partition) == nullptr) { ++it; continue; }
                it = rolesWithoutReplica_.erase(it);
                if (pa->leaderNode == static_cast<int32_t>(selfId_)) applyBecomeLeaderForKey(key, currentMap);
                else applyBecomeFollowerForKey(key, currentMap);
                ++lateRolesApplied_;
                late = 1;
            }
        }

        // Compute delta via the production MetadataStateMachine::deriveLocalDelta.
        rune::metadata::LocalRoleDelta delta =
            msm_->deriveLocalDelta(static_cast<int32_t>(selfId_), prevMap_);

        const bool anyChange =
            !delta.electedLeaders.empty() ||
            !delta.leaderUpdates.empty()  ||
            !delta.followers.empty()      ||
            !delta.deletes.empty();

        if (!anyChange) {
            return late;
        }

        // Apply role transitions for user partitions only.
        // electedLeaders + leaderUpdates → becomeLeader
        // MEDIUM-B: plus the coordinator partitions when they are on the
        // controller path (mapDriven_).
        for (const auto& key : delta.electedLeaders) {
            if (!mapDriven_(key.topicId)) continue;  // user (+ coordinator) partitions only
            applyBecomeLeaderForKey(key, currentMap);
        }
        for (const auto& key : delta.leaderUpdates) {
            if (!mapDriven_(key.topicId)) continue;
            applyBecomeLeaderForKey(key, currentMap);
        }

        // followers → becomeFollower
        for (const auto& key : delta.followers) {
            if (!mapDriven_(key.topicId)) continue;
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
        std::vector<rune::replication::NodeId>        isr,
        int32_t                                       partitionEpoch = -1) noexcept;
    /// F15 §3.1: record the role just routed as the gateway's intended-role hint.
    void recordIntendedRole_(rune::replication::PartitionReplica* replica,
                             ReplicaRegistry::IntendedRole role) noexcept;

    // -----------------------------------------------------------------------
    // Role ops not yet accepted by the owning worker's ring (Task 10 round-3
    // addendum; round 4 M2):
    //   - one FIFO PER OWNING WORKER: a full ring on one worker never holds back
    //     role ops for the others (no cross-worker head-of-line blocking);
    //   - per-replica order is preserved (nothing is attempted directly while an
    //     older op waits in that worker's queue);
    //   - a newer role op for a replica that already has one pending SUPERSEDES
    //     it in place (BecomeLeader / BecomeFollower are absolute, epoch-stamped
    //     states: only the latest matters) — the queue is bounded by the number
    //     of replicas, not by the churn rate;
    //   - each entry refers to its replica by (topic, partition, incarnation)
    //     ONLY and is re-resolved through the registry at delivery: an op whose
    //     replica was removed / re-registered meanwhile is dropped without
    //     dereferencing anything (it would be stamped stale and refused anyway);
    //   - growth is warned, rate-limited (each doubling past 64).
    // A back-pressured REMOVAL FENCE (applyDeleteForKey) is either delivered or
    // provably moot — see flushPendingRoleOps_ in the .cpp for the argument.
    // -----------------------------------------------------------------------
    struct PendingRoleOp {
        WorkerOp op;
        uint32_t incarnation = 0;
        bool     fence       = false;   // the removal fence of applyDeleteForKey
    };
    std::vector<std::deque<PendingRoleOp>> pendingRoleOps_;   // indexed by owning worker
    std::size_t pendingRoleOpsTotal_ = 0;
    std::size_t pendingWarnAt_       = 64;
    uint64_t    roleOpsCoalesced_    = 0;
    uint64_t    fencesMoot_          = 0;
    uint64_t    staleRoleOpsDropped_ = 0;
    // M1: a std::logic_error from WorkerPool (the F7 ownership invariant, raised
    // inside the inline reconcile of tryEnqueueOp) must not cross this
    // noexcept chain (std::terminate).  It is caught at the bridge boundary and
    // handed to NodeRuntimeAgent, which routes it to the halt path (exit 70).
    bool        fatalPending_ = false;
    std::string fatalWhat_;
    LeaderRoleHold leaderHold_    = nullptr;
    void*          leaderHoldCtx_ = nullptr;
    [[nodiscard]] bool leaderHeld_(const WorkerOp& op) const noexcept {
        return leaderHold_ != nullptr && op.type == WorkerOp::Type::BecomeLeader && leaderHold_(leaderHoldCtx_, op.topicId);
    }
    void enqueueRoleOp_(WorkerOp op, bool fence) noexcept;
    int  flushPendingRoleOps_() noexcept;
    bool tryRoute_(WorkerOp op) noexcept;

public:
    /// Test-only.
    void routeBecomeLeaderForTest(rune::replication::PartitionReplica* r, rune::replication::Epoch e,
                                  std::vector<rune::replication::NodeId> isr) noexcept {
        routeBecomeLeader_(r, e, std::move(isr));
    }
    void routeBecomeFollowerForTest(rune::replication::PartitionReplica* r, rune::replication::Epoch e,
                                    rune::replication::NodeId leader) noexcept {
        routeBecomeFollower_(r, e, leader);
    }
    [[nodiscard]] std::size_t pendingRoleOpsForTest() const noexcept { return pendingRoleOpsTotal_; }
    [[nodiscard]] std::size_t pendingRoleOpsForWorkerForTest(std::size_t w) const noexcept {
        return w < pendingRoleOps_.size() ? pendingRoleOps_[w].size() : 0;
    }
    [[nodiscard]] uint64_t roleOpsCoalescedForTest() const noexcept { return roleOpsCoalesced_; }
    [[nodiscard]] uint64_t fencesMootForTest() const noexcept { return fencesMoot_; }
    int flushPendingRoleOpsForTest() noexcept { return flushPendingRoleOps_(); }
    void routeRemovalFenceForTest(rune::replication::PartitionReplica* r, rune::replication::Epoch e) noexcept {
        routeBecomeFollower_(r, e, rune::metadata::NO_LEADER, /*fence=*/true);
    }
    void applyDeleteForTest(int32_t topicId, int32_t partition) noexcept {
        applyDeleteForKey(rune::metadata::PartitionKey{topicId, partition}, prevMap_);
    }
    /// Catalog history Task 8: the catalog DROPPED the topic — fence the replica
    /// on its owner thread and retire it out of routing NOW (the same path a
    /// committed placement delete takes; that later delete then finds nothing:
    /// idempotent).  Control thread.
    void removePartition(int32_t topicId, int32_t partition) noexcept {
        applyDeleteForKey(rune::metadata::PartitionKey{topicId, partition}, prevMap_);
    }

    /// M1: an invariant violation caught at the bridge boundary (once).
    /// NodeRuntimeAgent takes it after doWork and halts (exit 70).
    [[nodiscard]] bool takeFatal(std::string& what) noexcept {
        if (!fatalPending_) return false;
        fatalPending_ = false;
        what = std::move(fatalWhat_);
        return true;
    }
private:

    /// ISR v3 Step 10: true while a gate is wired and not open.  Defined in the .cpp (StorageGate.h stays out of this
    /// header).
    [[nodiscard]] bool gateClosed_() const noexcept;
    /// A held leader role: user partitions with a worker pool -> a node-local AlterIsrResponse WorkerOp
    /// (kAlterIsrLocalHoldLeadership, R14: no new op type); otherwise the replica is held directly.
    void routeHoldLeadership_(rune::replication::PartitionReplica* replica, int32_t leaderEpoch,
                              int32_t partitionEpoch) noexcept;
    const StorageGate*                             gate_ = nullptr;
    void routeBecomeFollower_(
        rune::replication::PartitionReplica*  replica,
        rune::replication::Epoch              epoch,
        rune::replication::NodeId             leaderId,
        bool                                  fence = false) noexcept;

    void applyBecomeLeaderForKey(
        const rune::metadata::PartitionKey& key,
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        // ISR v3 Step 10 (I-S1): a leader role routed while the storage gate is closed is HELD - the replica leads
        // nothing and asks the controller for a failover (the restart-fence bump).  Follower roles and deletes are
        // never gated.  The gate opening / closing re-derives every role (onStorageGateChanged).
        if (gateClosed_()) {
            const auto* hp = currentMap.get(key.topicId, key.partition);
            if (!hp) return;
            ++leaderRolesHeld_;
            if (isAdminLogTopic_(key.topicId)) { postAdminLogRole_(key.topicId, false, hp, /*hold=*/true); return; }
            auto* held = registry_.lookup(key.topicId, key.partition);
            if (!held) { noteRoleWithoutReplica_(key); return; }
            routeHoldLeadership_(held, hp->leaderEpoch, hp->partitionEpoch);
            return;
        }
        if (isAdminLogTopic_(key.topicId)) {   // Task 6 / auth Task 9: admin-owned — relay on the role ring
            if (const auto* pa = currentMap.get(key.topicId, key.partition)) postAdminLogRole_(key.topicId, true, pa);
            return;
        }
        auto* replica = registry_.lookup(key.topicId, key.partition);
        if (!replica) { noteRoleWithoutReplica_(key); return; }

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

        routeBecomeLeader_(replica, perPartEpoch, std::move(isr), pa->partitionEpoch);
    }

    void applyBecomeFollowerForKey(
        const rune::metadata::PartitionKey& key,
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        if (isAdminLogTopic_(key.topicId)) {   // Task 6 / auth Task 9: admin-owned — relay on the role ring
            postAdminLogRole_(key.topicId, false, currentMap.get(key.topicId, key.partition));
            return;
        }
        auto* replica = registry_.lookup(key.topicId, key.partition);
        if (!replica) { noteRoleWithoutReplica_(key); return; }

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
        routeBecomeFollower_(replica, fenceEpoch, rune::metadata::NO_LEADER, /*fence=*/true);

        // Quarantine: erase from routing.  The replica is RETIRED in the registry
        // and freed only after every worker acked the SetOwnership that drops it.
        registry_.remove(key.topicId, key.partition);
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    // F26: __catalog (-6) and __security (-7) are on the controller-ISR path with the coordinator
    // partitions — otherwise mapDriven_ never forwards their assignments.
    [[nodiscard]] static constexpr bool isAdminLogTopic_(int32_t t) noexcept {
        return t == kAdminLogCatalogTopicId || t == kAdminLogSecurityTopicId;
    }
    [[nodiscard]] static constexpr bool isCoordinatorTopic_(int32_t t) noexcept {
        return t == -1 || t == -2 || isAdminLogTopic_(t);
    }
    // Catalog history Task 6 (see setCatalogRoleRing); generalized over the admin logs by auth Task 9 (D39).
    // Defined in the .cpp.  `hold` = ISR v3 Step 10: the held leader role (leaderId -1), kMsg{Catalog,Security}HoldLeadership;
    // otherwise the log's kMsg{Catalog,Security}Role.
    void postAdminLogRole_(int32_t topicId, bool leader, const rune::metadata::PartitionAssignment* pa,
                           bool hold = false) noexcept;
    int  flushAdminLogRoles_() noexcept;
    [[nodiscard]] static constexpr std::size_t adminLogIndex_(int32_t topicId) noexcept {
        return topicId == kAdminLogSecurityTopicId ? 1u : 0u;
    }
    OneToOneByteRing*                              catalogRoleRing_    = nullptr;
    struct PendingAdminRole { AdminRoleMsg m; int32_t msgType; };
    std::array<std::optional<PendingAdminRole>, 2> adminRolePending_{};   // [0] __catalog, [1] __security
    std::array<uint64_t, 2>                        adminRolesPosted_{};
    std::array<uint64_t, 2>                        adminRolesHeld_{};
    [[nodiscard]] bool mapDriven_(int32_t t) const noexcept {
        return t >= 0 || (coordinatorOnController_ && isCoordinatorTopic_(t));
    }
    rune::consensus::NodeId                        selfId_;
    bool                                           coordinatorOnController_ = false;   ///< MEDIUM-B
    std::vector<rune::replication::NodeId>         allReplicaIds_;
    ReplicaRegistry&                               registry_;
    // Fix round 5 (fix-on-sight): roles skipped because the replica was not
    // built yet; applied once the registry has it (generation change).
    std::vector<std::pair<int32_t, int32_t>>       rolesWithoutReplica_;
    std::uint64_t                                  lateRoleGen_      = ~0ull;
    std::uint64_t                                  lateRolesApplied_ = 0;
    void noteRoleWithoutReplica_(const rune::metadata::PartitionKey& key) noexcept {
        if (key.topicId < 0) return;   // internal partitions are built at boot
        for (const auto& k : rolesWithoutReplica_) if (k.first == key.topicId && k.second == key.partition) return;
        try { rolesWithoutReplica_.emplace_back(key.topicId, key.partition); } catch (...) {}
        lateRoleGen_ = ~0ull;
    }
public:
    [[nodiscard]] std::uint64_t lateRolesAppliedForTest() const noexcept { return lateRolesApplied_; }
private:

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
    uint64_t                                       leaderRolesHeld_ = 0;   ///< ISR v3: held leader roles (Step 10)

    // Last observed consensus state (initialized to "no leader, epoch 0").
    // These are duty-cycle-thread-only (not atomic).
    rune::consensus::NodeId    lastObservedLeaderId_ = -1;
    rune::replication::Epoch   lastObservedEpoch_    = 0;

};

} // namespace rune::node
