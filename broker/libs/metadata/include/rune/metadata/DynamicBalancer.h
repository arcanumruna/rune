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
