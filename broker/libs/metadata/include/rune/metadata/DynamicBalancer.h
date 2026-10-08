// DynamicBalancer.h — delta rebalance engine.
//
// Computes assignment DELTAS on membership/topology change:
//   - Failover: any partition whose current leader is not in liveNodes gets a
//     new leader elected from ISR survivors.
//   - Preferred-leader election: if replicas[0] is in ISR but not currently the
//     leader (and budget remains), elect it as the new leader (min-churn restore).
//   - Co-partition group: all topics in the same coPartitionGroup have same-index
//     partitions moved AS A UNIT (same leader decision applied to all members).
//   - Skip no-ops: records that don't change leader or ISR are not emitted.
//   - Cap preferred-leader elections per cycle (maxPreferredElectionsPerCycle).
//
// CARRY-FORWARD constraints honored here:
//   1. liveNodes MUST be populated from MembershipState::eligibleNodes()
//      (Unfenced ∧ caughtUp ∧ ¬expired) by the caller; this class is a pure
//      computation over the supplied set.
//   2. Preferred leader = replicas[0] ONLY if replicas[0] is in the current ISR
//      (never assign leadership to a non-ISR replica → no committed loss).
//   3. Epoch bumps are delegated entirely to mergeChange(); the balancer never
//      directly increments leaderEpoch or partitionEpoch.

#pragma once

#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/metadata/ReplicaPositionTable.h>

#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::metadata {

// ---------------------------------------------------------------------------
// ReverseIndex — bit-packed node → gpid set.
//
// Internal representation: open-addressing unordered_map<int32_t, vector<uint64_t>>
// where each uint64_t is a 64-bit word of a bitset indexed by gpid.
//
// Death cost: O(partitions-on-node) — only the node's entry needs to be walked.
// Rebuild cost: O(all-partitions) — one pass over AssignmentMap.
// Per-assignment update: O(1) amortized.
//
// partitionsLedBy() decodes the bitset back to (topicId, partition) pairs.
// ---------------------------------------------------------------------------
class ReverseIndex {
public:
    /// Rebuild the index from scratch.  O(|AssignmentMap|).
    void rebuild(const AssignmentMap& m);

    /// Return all (topicId, partition) pairs whose current leader is `node`.
    /// O(bitset words for that node).
    std::vector<std::pair<int32_t,int32_t>> partitionsLedBy(int32_t node) const;

    /// Update the index after a leader change for (topicId, partition).
    /// oldLeader == NO_LEADER (-1) → only adds the new entry.
    /// newLeader == NO_LEADER (-1) → only removes the old entry.
    void onAssign(int32_t topicId, int32_t partition,
                  int32_t oldLeader, int32_t newLeader);

    /// Clear the index.
    void clear() noexcept { nodeIndex_.clear(); }

    // Bitset word count: gpid range = topicId * 1024 + partition.
    // Max gpid per 64-bit word = 64 gpids per word.
    // We grow each node's bitset vector dynamically.
    static constexpr int32_t kGpidsPerWord = 64;

private:

    /// node → compact bitset (each bit = one gpid).
    std::unordered_map<int32_t, std::vector<uint64_t>> nodeIndex_;

    void setBit(int32_t node, int32_t gpid);
    void clearBit(int32_t node, int32_t gpid);
    bool testBit(int32_t node, int32_t gpid) const;
};

// ---------------------------------------------------------------------------
// DynamicBalancerInputs — everything rebalance() needs.
// ---------------------------------------------------------------------------
struct DynamicBalancerInputs {
    const AssignmentMap*    cur       = nullptr;   ///< Current committed assignments.
    std::vector<int32_t>    liveNodes;             ///< eligibleNodes() — NOT raw liveNodes.
    std::vector<TopicSpec>  topics;                ///< Topic descriptors with co-partition info.
    ReplicaPositionTable*   positions = nullptr;   ///< Soft position table (may be nullptr).
    /// Commit quorum (majority) of the replica set.  A FAILOVER must never install
    /// an ISR below this floor: a partial in-sync set (0 < |ISR∩live| < minIsr)
    /// means the observing controller is transiently ISOLATED (VM CPU starvation /
    /// heartbeat jitter), NOT that the peers died.  Seizing leadership there would
    /// install ISR<minIsr → permanent MinIsrViolation → every acks=all produce
    /// rejected (the startup-collapse bug).  Below the floor the balancer keeps the
    /// current leader and re-converges when a majority returns — time-independent,
    /// for ANY jitter window, no tuned grace.  Default 1 preserves single-survivor
    /// failover for callers (tests) that don't set it.
    int                     minIsr = 1;
    /// Nodes whose currently-led partitions are DEMONSTRABLY ALIVE via the data
    /// plane — their partition's replication frontier advanced since the last
    /// cycle, which only a live, serving leader can cause.  Such a leader is NEVER
    /// failed over even if its consensus heartbeat momentarily lapsed (busy leader
    /// starving its own control-plane thread under produce load = the acks=all
    /// leadership-churn bug).  Converge off DATA-PLANE progress, not a heartbeat
    /// timeout (time-independent; a crashed leader's frontier freezes within a
    /// cycle → drops out of this set → real failover proceeds).  Empty default
    /// preserves prior behaviour for callers (tests) that don't populate it.
    std::vector<int32_t>    dataPlaneAliveLeaders;
    /// Review 2026-10-04 MEDIUM-3 (Kafka unclean.leader.election.enable): the
    /// position-proven SURVIVOR election — no committed-ISR member is live, so
    /// elect the most complete reporting live replica when > RF − minIsr of them
    /// reported — is an availability escape Kafka does not have.  Its pigeonhole
    /// proof rests on soft positions and on no replica having lost acked data
    /// (KIP-966).  OFF by default: such a partition goes leaderless (ISR lineage
    /// kept) until an ISR member returns.  Opt in per cluster
    /// (NodeConfig::survivorLeaderElection / RUNE_SURVIVOR_LEADER_ELECTION).
    bool                    allowSurvivorElection = false;
    /// Epoch-restart fix round 1 (MAJOR-1): (topicId, partition, node) — the
    /// committed leader `node` of that partition restarted and asked for a new
    /// leader epoch; it may have lost acked records (power loss, memory log).
    /// Kafka (a re-registered broker is fenced): the partition FAILS OVER to
    /// another live ISR member and `node` leaves the ISR (it re-enters through
    /// the new leader's AlterIsr once caught up).  A group whose ISR has no
    /// other live member is left untouched here (the caller re-elects the
    /// requester at a new epoch: it is the sole in-sync replica).
    struct LeaderExclusion { int32_t topicId; int32_t partition; int32_t node; };
    std::vector<LeaderExclusion> excludedLeaders;
    /// Controlled shutdown (2026-10-05): nodes stopping gracefully.  Unlike an
    /// exclusion this is a SOFT rule: an avoided node is never a preferred
    /// target, a partition it leads moves to another live in-sync replica (it
    /// leaves that ISR) — or stays with it when it is the sole in-sync replica
    /// (no unclean election) — and a DEAD leader's partition fails over to it
    /// only when no live, non-avoided in-sync replica exists (better than
    /// offline).
    std::vector<int32_t>         avoidLeaders;
    /// Fix round 2 N3 (Kafka leader.imbalance.per.broker.percentage): a
    /// preferred-leader election to node N fires only when MORE than this
    /// percentage of the partitions N is preferred for are led elsewhere
    /// (count-based, event-driven — evaluated on each pass).  0 = any
    /// imbalance (legacy behaviour).
    int                     imbalancePct = 0;
    int                     maxPreferredElectionsPerCycle = 1000; ///< Budget cap.
    /// Maximum BalancerRecords returned per rebalance() call.  Excess changes are
    /// deferred to the next cycle so one mass failover cannot stall the metadata log.
    int                     maxRecordsPerCycle = 4096;
};

// ---------------------------------------------------------------------------
// DynamicBalancer — stateless delta rebalance engine.
// ---------------------------------------------------------------------------
class DynamicBalancer {
public:
    /// Compute assignment deltas.  Returns only records where the assignment
    /// actually changes (leader or ISR).  Move-only BalancerRecord items.
    ///
    /// Policy in priority order:
    ///   1. Failover: dead-leader partitions → elect ISR survivor.
    ///   2. Preferred-leader: replicas[0] ∈ ISR but not leader → elect it (budget).
    ///   3. Co-partition groups: same-index partitions moved AS A UNIT.
    ///   4. Skip no-ops.
    std::vector<BalancerRecord> rebalance(const DynamicBalancerInputs& in);
};

} // namespace rune::metadata
