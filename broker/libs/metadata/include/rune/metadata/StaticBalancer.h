// StaticBalancer.h — Pure, deterministic, co-location-aware initial partition
// assignment.  No I/O, no global state; same inputs ⇒ byte-identical output.
//
// Co-partition groups (the key feature Kafka lacks): all member topics' same-index
// partitions are assigned to the SAME leader node so that co-located consumer
// groups see local leadership for every join-key bucket.

#pragma once

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
// ---------------------------------------------------------------------------
struct BalancerRecord {
    rune::protocol::metadata::UpdatePartitionAssignmentRecord rec;

    /// Owned copies of the ISR and replica arrays.
    /// rec.isr / rec.replicas are views into these.
    std::vector<int32_t> isr;
    std::vector<int32_t> replicas;
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
    ///   - Even leadership: per-node leader counts differ by ≤1.
    ///   - Replica spread: no partition places two replicas on the same node.
    ///   - Co-location: all topics in the same co-partition group have
    ///     same-index partitions assigned to the SAME leader node.
    ///   - Determinism: same (topics, liveNodes) inputs ⇒ byte-identical output.
    ///   - Initial epochs: leaderEpoch=0, partitionEpoch=0 for every record.
    ///
    /// Throws BalancerError on:
    ///   - RF > liveNodes.size() for any topic.
    ///   - Co-partition group members with mismatched partitionCount.
    ///   - Co-partition group members with mismatched partitionKey.
    ///
    /// @param topics    Topic descriptors (order-independent for determinism).
    /// @param liveNodes Live broker node IDs (order determines striping seed).
    /// @return          One BalancerRecord per (topic, partition) pair, in
    ///                  topic-input-order then partition-ascending order.
    static std::vector<BalancerRecord> assign(
        const std::vector<TopicSpec>& topics,
        const std::vector<NodeId>&    liveNodes);
};

} // namespace rune::metadata
