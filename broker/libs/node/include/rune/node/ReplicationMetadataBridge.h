#pragma once
// ReplicationMetadataBridge.h — Consensus → replication metadata bridge (GR-2).
//
// Per-cycle check: compare the current (leaderId, leadershipTermId) from the
// consensus layer against the last observed values.  On a change:
//   - If this node IS the new leader: call becomeLeader(newEpoch, allReplicas)
//     on every replica in the registry.
//   - If this node is NOT the leader: call becomeFollower(newEpoch, leaderId)
//     on every replica.
//
// DR scope: cluster-leader == leader of ALL partitions.
//           consensus leadershipTermId == per-partition leaderEpoch.
//
// Epoch monotonicity:
//   The consensus leadershipTermId is initialized to 0 and only ever increases
//   (Election.cpp lines 252, 349, 361 set it from max(persistedTerm, prev)).
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
// No locks. No allocation.

#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/consensus/Types.h>

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
    ///                       becomeLeader).  For DR scope ISR = all replicas.
    /// \param registry       Non-owning reference to the ReplicaRegistry.
    ///                       Must outlive this bridge.
    ReplicationMetadataBridge(rune::consensus::NodeId  selfId,
                              std::vector<rune::replication::NodeId> allReplicaIds,
                              ReplicaRegistry&         registry) noexcept
        : selfId_(selfId)
        , allReplicaIds_(std::move(allReplicaIds))
        , registry_(registry)
    {}

    // Non-copyable, non-movable.
    ReplicationMetadataBridge(const ReplicationMetadataBridge&)            = delete;
    ReplicationMetadataBridge& operator=(const ReplicationMetadataBridge&) = delete;

    // -----------------------------------------------------------------------
    // doWork — single duty-cycle step.
    //
    // Called each cycle AFTER consensus.doWork() (so the consensus state is
    // fully updated for this tick) and BEFORE transport.poll() / replica.doWork().
    //
    // Checks the current (leaderId, leadershipTermId) via injected accessors.
    // If either changes, performs the appropriate becomeLeader / becomeFollower
    // transition on all registered replicas.
    //
    // Returns 1 if a role-flip was performed, 0 otherwise (duty-cycle work count).
    // -----------------------------------------------------------------------
    int doWork(rune::consensus::NodeId  currentLeaderId,
               rune::replication::Epoch currentEpoch) noexcept
    {
        // Epoch MUST be monotonic: guard against any regress (shouldn't happen
        // with Aeron consensus but defensive belt-and-suspenders).
        if (currentEpoch < lastObservedEpoch_) {
            currentEpoch = lastObservedEpoch_;
        }

        // Nothing changed — common fast path.
        if (currentLeaderId == lastObservedLeaderId_ &&
            currentEpoch    == lastObservedEpoch_) {
            return 0;
        }

        // No leader known yet — don't flip roles until we have a valid leader.
        if (currentLeaderId < 0) {
            return 0;
        }

        // Seed initial roles on the very first call when no leader was known.
        // (lastObservedLeaderId_ == -1 on construction.)
        lastObservedLeaderId_ = currentLeaderId;
        lastObservedEpoch_    = currentEpoch;

        const bool selfIsLeader =
            (currentLeaderId == static_cast<rune::consensus::NodeId>(selfId_));

        if (selfIsLeader) {
            // This node is the new leader — become leader for all partitions.
            for (auto* replica : registry_.allReplicas()) {
                if (replica) {
                    replica->becomeLeader(currentEpoch, allReplicaIds_);
                }
            }
        } else {
            // Another node is the leader — become follower for all partitions.
            const auto repl_leader =
                static_cast<rune::replication::NodeId>(currentLeaderId);
            for (auto* replica : registry_.allReplicas()) {
                if (replica) {
                    replica->becomeFollower(currentEpoch, repl_leader);
                }
            }
        }

        // Update the atomic mirrors after role transitions complete.
        // Both stores use release so observer threads that acquire-load
        // isLeaderAtomic() / lastEpochAtomic() see the effects of
        // becomeLeader/becomeFollower.
        observedEpoch_.store(lastObservedEpoch_, std::memory_order_relaxed);
        observedIsLeader_.store(selfIsLeader, std::memory_order_release);

        return 1;
    }

    // -----------------------------------------------------------------------
    // Accessors (for tests / GR-4 per-partition leader lookup)
    // -----------------------------------------------------------------------

    /// The last observed leader id (-1 if no leader seen yet).
    [[nodiscard]] rune::consensus::NodeId lastLeaderId() const noexcept {
        return lastObservedLeaderId_;
    }

    /// The last observed epoch (= leadershipTermId from consensus).
    [[nodiscard]] rune::replication::Epoch lastEpoch() const noexcept {
        return lastObservedEpoch_;
    }

    /// True if this node is currently the leader (from the bridge's perspective).
    /// Observer-thread-safe: uses the atomic observedIsLeader_ mirror.
    [[nodiscard]] bool isLeaderAtomic() const noexcept {
        return observedIsLeader_.load(std::memory_order_acquire);
    }

    /// The last epoch applied to replicas.
    /// Observer-thread-safe: acquire-load pairs with the release-store in doWork().
    /// The acquire on observedIsLeader_ also serves as the ordering fence for
    /// observedEpoch_ (relaxed store before the release on observedIsLeader_).
    [[nodiscard]] rune::replication::Epoch lastEpochAtomic() const noexcept {
        // Acquire on observedIsLeader_ ensures we see the relaxed store to
        // observedEpoch_ that happened before it (in doWork).
        (void)observedIsLeader_.load(std::memory_order_acquire);
        return observedEpoch_.load(std::memory_order_relaxed);
    }

    /// True if this node is currently the leader (from the bridge's perspective).
    /// Duty-cycle-thread-only: reads non-atomic state.
    [[nodiscard]] bool isLeader() const noexcept {
        return lastObservedLeaderId_ >= 0 &&
               lastObservedLeaderId_ == static_cast<rune::consensus::NodeId>(selfId_);
    }

private:
    rune::consensus::NodeId                        selfId_;
    std::vector<rune::replication::NodeId>         allReplicaIds_;
    ReplicaRegistry&                               registry_;

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
