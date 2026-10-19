// StaticBalancer.h — Pure, deterministic, co-location-aware initial partition
// assignment.  No I/O, no global state; same inputs ⇒ byte-identical output.
//
// Co-partition groups (the key feature Kafka lacks): all member topics' same-index
// partitions are assigned to the SAME leader node so that co-located consumer
// groups see local leadership for every join-key bucket.

#pragma once

#include <rune/metadata/PartitionAssignment.h>
#include <rune/protocol/MetadataRecords.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace rune::metadata {

// ---------------------------------------------------------------------------
// NodeId alias (same logical type as used in PartitionAssignment)
// ---------------------------------------------------------------------------
using NodeId = int32_t;

// ---------------------------------------------------------------------------
// TopicSpec — input descriptor for a single topic.
// ---------------------------------------------------------------------------
struct TopicSpec {
    int32_t     topicId;           ///< Unique topic identifier.
    int32_t     partitionCount;    ///< Number of partitions.
    int16_t     rf;                ///< Replication factor (≥1, ≤ liveNodes.size()).
    std::string coPartitionGroup;  ///< Non-empty ⇒ member of this co-partition group.
    std::string partitionKey;      ///< Logical partition key (must match within a group).
};

// ---------------------------------------------------------------------------
// BalancerError — thrown on any validation failure.
// ---------------------------------------------------------------------------
class BalancerError : public std::runtime_error {
public:
    explicit BalancerError(const std::string& msg) : std::runtime_error(msg) {}
};

// ---------------------------------------------------------------------------
// BalancerRecord — an owned assignment record returned by the balancer.
//
// UpdatePartitionAssignmentRecord uses Int32ArrayView (zero-copy views into raw
// bytes).  The balancer stores the actual int32_t arrays here and sets the views
// to point into them.  Callers must keep the BalancerRecord alive as long as
// they hold the embedded rec.isr / rec.replicas views.
//
// Convenience accessors expose the arrays directly as std::vector<int32_t>
// so tests can compare without worrying about the view lifetime.
//
// MOVE-ONLY: copying a BalancerRecord would make rec.isr / rec.replicas dangle
// (the views would still point into the SOURCE's vectors, not the copy's).
// The copy constructor and copy-assignment are therefore deleted.  Vectors of
// move-only types work correctly; the balancer always moves records into the
// result vector.
// ---------------------------------------------------------------------------
struct BalancerRecord {
    rune::protocol::metadata::UpdatePartitionAssignmentRecord rec;

    /// Owned copies of the ISR and replica arrays.
    /// rec.isr / rec.replicas are views into these.
    std::vector<int32_t> isr;
    std::vector<int32_t> replicas;
    /// D15: a leaderless record's mark — appended as the trailer after `rec`
    /// (MetadataExtensions.h); Kind::None = no trailer.
    LeaderlessMark mark{};

    BalancerRecord()                                  = default;
    BalancerRecord(BalancerRecord&&)                  = default;
    BalancerRecord& operator=(BalancerRecord&&)       = default;

    // Copying is deleted: the views inside rec would dangle into the source's
    // vectors, not the copy's vectors.
    BalancerRecord(const BalancerRecord&)             = delete;
    BalancerRecord& operator=(const BalancerRecord&) = delete;
};

// ---------------------------------------------------------------------------
// StaticBalancer — all functionality exposed through a single static method.
// ---------------------------------------------------------------------------
class StaticBalancer {
public:
    /// Compute the initial partition→leader assignment for the given topics on
    /// the given live nodes.
    ///
    /// Algorithm guarantees:
    ///   - Co-location: all topics in the same co-partition group have
    ///     same-index partitions assigned to the SAME leader node.  This is
    ///     the primary constraint and always holds.
    ///   - Leadership balance (co-partition-stripe granularity): each stripe
    ///     index advances by 1 through the sorted node ring, so the per-node
    ///     leader count at the stripe level differs by ≤1.  Concretely:
    ///       * Singleton topics: per-node leader count differs by ≤1.
    ///       * Co-located group with partitionCount % nodeCount == 0: per-node
    ///         leader count differs by ≤1.
    ///       * Co-located group with partitionCount % nodeCount != 0: all M
    ///         topics in the group share the same leader per stripe index, so
    ///         the residual imbalance is multiplied by M.  The per-node leader
    ///         count spread is bounded by the group's member count M.  This is
    ///         an inherent, accepted consequence of the co-location constraint.
    ///   - Replica spread: no partition places two replicas on the same node.
    ///   - Determinism: same input order ⇒ byte-identical output (named groups
    ///     are processed in sorted group-name order; singletons in input order).
    ///   - Initial epochs: leaderEpoch=0, partitionEpoch=0 for every record.
    ///
    /// Throws BalancerError on:
    ///   - RF > liveNodes.size() for any topic.
    ///   - Co-partition group members with mismatched partitionCount.
    ///   - Co-partition group members with mismatched partitionKey.
    ///
    /// @param topics    Topic descriptors (named groups sorted by group name for
    ///                  determinism; singletons emitted in input order).
    /// @param liveNodes Live broker node IDs (sorted ascending internally).
    /// @return          One BalancerRecord per (topic, partition) pair, in
    ///                  topic-input-order then partition-ascending order.
    static std::vector<BalancerRecord> assign(
        const std::vector<TopicSpec>& topics,
        const std::vector<NodeId>&    liveNodes);
};

} // namespace rune::metadata
