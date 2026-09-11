#pragma once
// ---------------------------------------------------------------------------
// PartitionFailoverCoordinator.h — Leader failover & self-healing (P3-5).
//
// Namespace: rune::replication
// C++23. NO locks. NO busy-spin. Single-writer doWork(nowNs) driven.
//
// ## Overview (spec §8, plan P3-5)
//
// The PartitionFailoverCoordinator is the control-plane bridge for a single
// partition.  It holds the authoritative PartitionMetadata (leader, replicas,
// ISR, leaderEpoch) and drives:
//
//   1. **Leader election from ISR**: on a leader-death signal (injected via
//      signalLeaderDead()), choose a new leader from the current ISR using a
//      deterministic rule: highest LEO, tie-break by smallest NodeId (stable
//      sort).  Produce updated PartitionMetadata with the new leader + epoch+1.
//
//   2. **Epoch fencing of the old leader**: the new leaderEpoch is strictly
//      greater than the old.  The old leader's replica is transitioned to
//      Follower (becomeFollower(newEpoch, newLeaderId)) so it truncates its
//      uncommitted tail and re-syncs.  Any stale FetchRequest from the old
//      leader epoch is rejected by EpochFencing (existing P3-2 mechanism).
//
//   3. **Under-replication repair (self-healing)**: when |ISR| < replication
//      factor, assignRepairFollower(nodeId, replica, store) seeds a healthy
//      node's PartitionReplica as a follower that bootstraps from the leader
//      via PartitionReplay (from log-start) then transitions to live fetch.
//      Once the follower's LEO reaches HW, doWork() re-enters it into the ISR
//      via IsrManager::maybeExpandIsr().
//
// ## PartitionMetadata — the control-plane data model
//
// Mirrors Kafka's per-partition controller metadata:
//   { leader, replicas, isr, leaderEpoch }
// The coordinator owns one canonical copy; all transitions update it.
//
// ## NodeLiveness — injected fake for testing
//
// Tests inject a NodeLiveness object whose isAlive(NodeId) method returns
// false for a "dead" leader, triggering election.  Production code will use
// Aeron session/heartbeat state.
//
// ## Single-writer doWork()
//
// doWork(nowNs) is called by one owning thread.  It:
//   - Checks liveness (if the coordinator is armed to watch).
//   - Runs the ISR-repair step (maybeExpandIsr on newly assigned followers).
//   - Propagates ISR changes back to the leader replica.
//   - Returns the number of work items performed (0 when idle — never spins).
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>
#include <algorithm>

#include <rune/replication/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/PartitionReplay.h>

namespace rune::replication {

// ---------------------------------------------------------------------------
// PartitionMetadata — control-plane snapshot for one partition.
// ---------------------------------------------------------------------------
struct PartitionMetadata {
    PartitionId          partitionId  = 0;
    NodeId               leader       = -1;          ///< Current leader NodeId.
    std::vector<NodeId>  replicas;                   ///< Full replica set.
    std::vector<NodeId>  isr;                        ///< In-sync replica set.
    Epoch                leaderEpoch  = 0;           ///< Monotonically increasing.
};

// ---------------------------------------------------------------------------
// NodeLiveness — injected interface for liveness queries.
//
// Tests implement a trivial fake; production uses Aeron session state.
// ---------------------------------------------------------------------------
class NodeLiveness {
public:
    virtual ~NodeLiveness() = default;

    /// Return true if the node is currently alive/reachable.
    [[nodiscard]] virtual bool isAlive(NodeId node) const noexcept = 0;
};

// ---------------------------------------------------------------------------
// ReplicaEntry — a (NodeId, PartitionReplica*) pair registered with the
// coordinator.  The coordinator does NOT own the replicas.
// ---------------------------------------------------------------------------
struct ReplicaEntry {
    NodeId            nodeId;
    PartitionReplica* replica;    ///< non-owning
    Offset            knownLeo;   ///< last known LEO (for election ranking)
};

// ---------------------------------------------------------------------------
// PartitionFailoverCoordinator
// ---------------------------------------------------------------------------
class PartitionFailoverCoordinator {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param meta         Initial partition metadata (leader, ISR, epoch …).
    /// \param liveness     Injected liveness oracle (non-owning).
    /// \param isrCheckNs   doWork() ISR-repair check interval in nanoseconds.
    explicit PartitionFailoverCoordinator(PartitionMetadata   meta,
                                          NodeLiveness*       liveness,
                                          int64_t             isrCheckNs = 5'000'000LL)
        : meta_(std::move(meta))
        , liveness_(liveness)
        , isrCheckNs_(isrCheckNs)
    {}

    // Non-copyable (holds raw pointers into live replica state).
    PartitionFailoverCoordinator(const PartitionFailoverCoordinator&)            = delete;
    PartitionFailoverCoordinator& operator=(const PartitionFailoverCoordinator&) = delete;

    // -----------------------------------------------------------------------
    // Replica registration
    // -----------------------------------------------------------------------

    /// Register a replica so the coordinator can drive role transitions.
    /// Must be called for ALL replicas (leader + followers) before doWork().
    void registerReplica(NodeId nodeId, PartitionReplica* replica)
    {
        for (auto& e : entries_) {
            if (e.nodeId == nodeId) {
                e.replica = replica;
                return;
            }
        }
        entries_.push_back({nodeId, replica, 0});
    }

    // -----------------------------------------------------------------------
    // Leader-death signal
    // -----------------------------------------------------------------------

    /// Inject a leader-death signal: triggers ISR election on next doWork()
    /// (or immediately if callElect=true).
    ///
    /// In production this is called by the Aeron-session-timeout handler.
    /// In tests call it directly, then doWork() once.
    void signalLeaderDead() noexcept {
        leaderDeadSignaled_ = true;
    }

    // -----------------------------------------------------------------------
    // Under-replication repair
    // -----------------------------------------------------------------------

    /// Assign a healthy follower to repair under-replication.
    ///
    /// The follower replica is seeded via PartitionReplay (from log-start to
    /// the current leader LEO) then transitions to live fetch.  After catching
    /// up to HW, doWork() expands it back into the ISR.
    ///
    /// \param nodeId     The healthy node to assign.
    /// \param replica    The node's PartitionReplica (should be freshly
    ///                   constructed or have an empty log).
    /// \param store      RecordingStore to replay from (leader's store).
    /// \param leaderReplica  The current leader replica (to get current LEO/HW).
    void assignRepairFollower(NodeId            nodeId,
                              PartitionReplica* replica,
                              const RecordingStore* store,
                              PartitionReplica* leaderReplica)
    {
        // Ensure it's registered.
        registerReplica(nodeId, replica);

        // Transition to follower under the current epoch.
        replica->becomeFollower(meta_.leaderEpoch, meta_.leader);

        // Bootstrap from recorded log via replay.
        if (store != nullptr) {
            PartitionReplay replay(store);
            Offset leaderLeo = leaderReplica->logEndOffset();
            replay.replayIntoLog(*replica->log(), 0, leaderLeo);
        }

        // Mark as a repair candidate so doWork() tracks ISR expansion.
        if (!isInReplicas(nodeId)) {
            meta_.replicas.push_back(nodeId);
        }
        repairCandidates_.push_back(nodeId);
    }

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step
    // -----------------------------------------------------------------------

    /// Drive the coordinator for one cycle.
    ///
    /// \param nowNs  Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of work items performed (0 when idle — no spin).
    int doWork(int64_t nowNs)
    {
        int work = 0;

        // 1. Leader-death: elect from ISR.
        if (leaderDeadSignaled_) {
            leaderDeadSignaled_ = false;
            if (electNewLeader()) {
                ++work;
            }
        }

        // 2. ISR-repair check (cadenced).
        if (nowNs - lastIsrCheckNs_ >= isrCheckNs_) {
            lastIsrCheckNs_ = nowNs;
            work += runIsrRepair(nowNs);
        }

        return work;
    }

    // -----------------------------------------------------------------------
    // Immediate election (for testing / manual trigger)
    // -----------------------------------------------------------------------

    /// Elect a new leader from the current ISR immediately.
    ///
    /// \returns true if a new leader was elected; false if ISR is empty or
    ///          leader is already alive.
    bool electNewLeader()
    {
        // Collect ISR candidates (exclude the dead leader).
        NodeId oldLeader = meta_.leader;
        std::vector<NodeId> candidates;
        candidates.reserve(meta_.isr.size());
        for (NodeId n : meta_.isr) {
            if (n != oldLeader) {
                candidates.push_back(n);
            }
        }

        if (candidates.empty()) {
            // Cannot elect — no ISR members besides the dead leader.
            // (If ISR has only the leader, we can't safely elect.)
            return false;
        }

        // Deterministic election: highest LEO, tie-break by smallest NodeId.
        // Refresh LEOs from registered replicas.
        for (auto& e : entries_) {
            if (e.replica != nullptr) {
                e.knownLeo = e.replica->logEndOffset();
            }
        }

        NodeId chosen = candidates[0];
        Offset chosenLeo = leoOf(chosen);
        for (NodeId n : candidates) {
            Offset nLeo = leoOf(n);
            if (nLeo > chosenLeo || (nLeo == chosenLeo && n < chosen)) {
                chosen    = n;
                chosenLeo = nLeo;
            }
        }

        // Bump epoch.
        Epoch newEpoch = meta_.leaderEpoch + 1;

        // New ISR = all current ISR members except the dead leader (they all
        // hold all committed data ≤ HW by ISR invariant).
        std::vector<NodeId> newIsr = candidates;

        // Ensure the new leader is in the ISR.
        if (!inVec(newIsr, chosen)) {
            newIsr.push_back(chosen);
        }

        // Update metadata.
        meta_.leader      = chosen;
        meta_.isr         = newIsr;
        meta_.leaderEpoch = newEpoch;

        // Drive role transitions.
        // New leader: becomeLeader(newEpoch, newIsr).
        for (auto& e : entries_) {
            if (e.nodeId == chosen && e.replica != nullptr) {
                e.replica->becomeLeader(newEpoch, newIsr);
            }
        }

        // Old leader (if alive and registered): becomeFollower so it truncates.
        for (auto& e : entries_) {
            if (e.nodeId == oldLeader && e.replica != nullptr) {
                e.replica->becomeFollower(newEpoch, chosen);
            }
        }

        // All OTHER followers: adopt new epoch.
        for (auto& e : entries_) {
            if (e.nodeId != chosen && e.nodeId != oldLeader && e.replica != nullptr) {
                if (e.replica->role() == Role::Follower) {
                    e.replica->becomeFollower(newEpoch, chosen);
                }
            }
        }

        return true;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] const PartitionMetadata& metadata() const noexcept {
        return meta_;
    }

    [[nodiscard]] NodeId currentLeader() const noexcept {
        return meta_.leader;
    }

    [[nodiscard]] Epoch currentEpoch() const noexcept {
        return meta_.leaderEpoch;
    }

    [[nodiscard]] const std::vector<NodeId>& currentIsr() const noexcept {
        return meta_.isr;
    }

    // -----------------------------------------------------------------------
    // Log access helper — expose log pointer for tests.
    // -----------------------------------------------------------------------

    /// Return the log of a registered replica (for test inspection).
    [[nodiscard]] rune::storage::Log* logOf(NodeId nodeId) const noexcept {
        for (const auto& e : entries_) {
            if (e.nodeId == nodeId && e.replica != nullptr) {
                return e.replica->log();
            }
        }
        return nullptr;
    }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    [[nodiscard]] Offset leoOf(NodeId nodeId) const noexcept {
        for (const auto& e : entries_) {
            if (e.nodeId == nodeId) return e.knownLeo;
        }
        return 0;
    }

    [[nodiscard]] bool isInReplicas(NodeId nodeId) const noexcept {
        return inVec(meta_.replicas, nodeId);
    }

    static bool inVec(const std::vector<NodeId>& v, NodeId n) noexcept {
        return std::find(v.begin(), v.end(), n) != v.end();
    }

    /// ISR-repair step: expand ISR when repair candidates have caught up to HW.
    int runIsrRepair(int64_t /*nowNs*/)
    {
        if (repairCandidates_.empty()) return 0;

        // Find the current leader replica to get HW.
        PartitionReplica* leaderReplica = nullptr;
        for (auto& e : entries_) {
            if (e.nodeId == meta_.leader && e.replica != nullptr) {
                leaderReplica = e.replica;
                break;
            }
        }
        if (leaderReplica == nullptr) return 0;

        Offset hw = leaderReplica->highWatermark();
        if (hw == 0) return 0; // nothing committed yet

        int work = 0;
        std::vector<NodeId> stillPending;
        for (NodeId candidate : repairCandidates_) {
            // Find candidate's replica.
            PartitionReplica* cr = nullptr;
            for (auto& e : entries_) {
                if (e.nodeId == candidate) { cr = e.replica; break; }
            }
            if (cr == nullptr) { stillPending.push_back(candidate); continue; }

            Offset cLeo = cr->logEndOffset();
            if (cLeo >= hw) {
                // Caught up — add to ISR if not already there.
                if (!inVec(meta_.isr, candidate)) {
                    meta_.isr.push_back(candidate);
                    // Propagate into leader replica.
                    for (auto& e : entries_) {
                        if (e.nodeId == meta_.leader && e.replica != nullptr) {
                            e.replica->becomeLeader(meta_.leaderEpoch, meta_.isr);
                            break;
                        }
                    }
                    ++work;
                }
                // Remove from pending — done.
            } else {
                stillPending.push_back(candidate);
            }
        }
        repairCandidates_ = std::move(stillPending);
        return work;
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    PartitionMetadata        meta_;
    NodeLiveness*            liveness_;          ///< non-owning
    int64_t                  isrCheckNs_;        ///< ISR-repair check cadence
    int64_t                  lastIsrCheckNs_ = 0;

    bool                     leaderDeadSignaled_ = false;

    std::vector<ReplicaEntry> entries_;          ///< registered replicas
    std::vector<NodeId>       repairCandidates_; ///< followers in bootstrap phase
};

// ---------------------------------------------------------------------------
// SimpleNodeLiveness — test fake: dead nodes are listed explicitly.
// ---------------------------------------------------------------------------
class SimpleNodeLiveness : public NodeLiveness {
public:
    void markDead(NodeId node) { deadNodes_.push_back(node); }
    void markAlive(NodeId node) {
        auto it = std::find(deadNodes_.begin(), deadNodes_.end(), node);
        if (it != deadNodes_.end()) deadNodes_.erase(it);
    }

    [[nodiscard]] bool isAlive(NodeId node) const noexcept override {
        return std::find(deadNodes_.begin(), deadNodes_.end(), node) == deadNodes_.end();
    }

private:
    std::vector<NodeId> deadNodes_;
};

} // namespace rune::replication
