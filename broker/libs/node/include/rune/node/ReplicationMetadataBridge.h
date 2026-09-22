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
    // [Test-only] setCommittedMap — supply an AssignmentMap directly (no MSM).
    //
    // Used by per-partition unit tests that construct AssignmentMap objects
    // directly rather than going through MetadataStateMachine.  In production
    // code, setMsm() is the correct entry point.
    //
    // Must be called on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setCommittedMap(const rune::metadata::AssignmentMap* map) noexcept {
        directMap_ = map;
        msm_       = nullptr;  // directMap_ takes precedence when msm_ is null
        prevMap_   = rune::metadata::AssignmentMap{};
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
                    replica->becomeLeader(currentEpoch, allReplicaIds_);
                } else {
                    const auto repl_leader =
                        static_cast<rune::replication::NodeId>(currentLeaderId);
                    replica->becomeFollower(currentEpoch, repl_leader);
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
        //
        // The map source can be:
        //   (a) msm_->committedMap()  — production path (MetadataStateMachine)
        //   (b) directMap_            — test-only path (raw AssignmentMap pointer)
        const rune::metadata::AssignmentMap* currentMap = nullptr;
        if (msm_ != nullptr) {
            currentMap = &msm_->committedMap();
        } else if (directMap_ != nullptr) {
            currentMap = directMap_;
        }

        if (currentMap != nullptr) {
            work += applyUserPartitionRoles(*currentMap);
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
    // Compute deriveLocalDelta(selfId_, prevMap_) on the current committed map.
    // For each changed user partition, call becomeLeader or becomeFollower.
    // Snapshot prevMap_ for the next cycle.
    //
    // Returns 1 if any role transition was performed, 0 if no-op.
    // -----------------------------------------------------------------------
    int applyUserPartitionRoles(
        const rune::metadata::AssignmentMap& currentMap) noexcept
    {
        // Compute delta using deriveLocalDelta via the MSM (if available) or
        // via direct map comparison using a temporary MSM shell.
        rune::metadata::LocalRoleDelta delta;
        if (msm_ != nullptr) {
            delta = msm_->deriveLocalDelta(
                static_cast<int32_t>(selfId_), prevMap_);
        } else {
            // Test-only path: build a temporary MSM with currentMap as committed,
            // then derive delta.  We use the committed-map copy trick:
            // wrap the call through a shared helper.
            delta = deriveLocalDeltaFromMap(currentMap, prevMap_,
                                            static_cast<int32_t>(selfId_));
        }

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

        // followers + deletes → becomeFollower
        for (const auto& key : delta.followers) {
            if (key.topicId < 0) continue;
            applyBecomeFollowerForKey(key, currentMap);
        }
        for (const auto& key : delta.deletes) {
            if (key.topicId < 0) continue;
            applyBecomeFollowerForKey(key, currentMap);
        }

        // Snapshot current committed map for next cycle's delta computation.
        prevMap_ = currentMap;

        return 1;
    }

    // -----------------------------------------------------------------------
    // deriveLocalDeltaFromMap — standalone delta computation on two AssignmentMaps.
    //
    // Mirrors MetadataStateMachine::deriveLocalDelta but operates on plain
    // AssignmentMap objects (used by the test-only directMap_ path).
    // -----------------------------------------------------------------------
    static rune::metadata::LocalRoleDelta deriveLocalDeltaFromMap(
        const rune::metadata::AssignmentMap& current,
        const rune::metadata::AssignmentMap& prev,
        int32_t selfNode)
    {
        rune::metadata::LocalRoleDelta delta;

        auto containsNode = [](const std::vector<int32_t>& vec, int32_t node) {
            for (int32_t v : vec)
                if (v == node) return true;
            return false;
        };

        // Walk current map
        current.forEach([&](int32_t topicId, int32_t partition,
                            const rune::metadata::PartitionAssignment& cur) {
            const rune::metadata::PartitionAssignment* prevPa =
                prev.get(topicId, partition);
            const rune::metadata::PartitionKey key{topicId, partition};

            if (prevPa == nullptr) {
                // Brand-new
                if (cur.leaderNode == selfNode) {
                    delta.electedLeaders.push_back(key);
                } else if (containsNode(cur.replicas, selfNode)) {
                    delta.followers.push_back(key);
                }
                return;
            }

            const bool leaderEpochBumped    = (cur.leaderEpoch    > prevPa->leaderEpoch);
            const bool partitionEpochBumped = (cur.partitionEpoch > prevPa->partitionEpoch);

            if (!leaderEpochBumped && !partitionEpochBumped) return;

            if (cur.leaderNode == selfNode) {
                if (leaderEpochBumped) {
                    delta.electedLeaders.push_back(key);
                } else {
                    delta.leaderUpdates.push_back(key);
                }
            } else if (containsNode(cur.replicas, selfNode)) {
                delta.followers.push_back(key);
            } else if (containsNode(prevPa->replicas, selfNode)) {
                delta.deletes.push_back(key);
            }
        });

        // Walk prev for removed partitions
        prev.forEach([&](int32_t topicId, int32_t partition,
                         const rune::metadata::PartitionAssignment& prevPa) {
            if (current.get(topicId, partition) == nullptr) {
                auto containsNodeLocal = [](const std::vector<int32_t>& v, int32_t n) {
                    for (int32_t x : v) if (x == n) return true;
                    return false;
                };
                if (containsNodeLocal(prevPa.replicas, selfNode)) {
                    delta.deletes.push_back(
                        rune::metadata::PartitionKey{topicId, partition});
                }
            }
        });

        return delta;
    }

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

        replica->becomeLeader(perPartEpoch, std::move(isr));
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

        replica->becomeFollower(perPartEpoch, leaderNode);
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    rune::consensus::NodeId                        selfId_;
    std::vector<rune::replication::NodeId>         allReplicaIds_;
    ReplicaRegistry&                               registry_;

    // Production path: non-owning pointer to the MetadataStateMachine.
    // Accessed only on the duty-cycle thread.
    const rune::metadata::MetadataStateMachine*    msm_       = nullptr;

    // Test-only path: raw AssignmentMap pointer (when msm_ is null).
    const rune::metadata::AssignmentMap*           directMap_ = nullptr;

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
