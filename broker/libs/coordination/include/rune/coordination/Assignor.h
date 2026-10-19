#pragma once
// ---------------------------------------------------------------------------
// Assignor.h — Kafka-faithful partition assignors.
//
// Namespace: rune::coordination
// C++23. NO hot-path alloc beyond the result map.
//
// ## Overview (spec P4-3)
//
// PartitionAssignor is a pure interface that computes a partition assignment
// given the current members and the partition counts for each topic.
//
// assign() MUST be:
//   - Deterministic (same input → same output regardless of call order)
//   - Sort-stable   (sorts members + partitions internally)
//   - Complete      (every partition of a subscribed topic assigned exactly once)
//   - Valid         (no partition assigned to a non-subscriber)
//   - Balanced      (load within ±1 partition among subscribers to any topic)
//
// ## RangeAssignor — Kafka Range assignor
//
//   Per topic:
//     1. Collect only members that subscribe to this topic, sorted lexicographically.
//     2. numPartitions partitions, numMembers subscribers.
//     3. base = numPartitions / numMembers, extras = numPartitions % numMembers.
//     4. Member[i] gets partitions [start, start+range) where:
//          range = base + (i < extras ? 1 : 0)
//          start = sum of all previous members' ranges
//     This mirrors Kafka's exact RangeAssignor distribution.
//
// ## RoundRobinAssignor — Kafka RoundRobin assignor
//
//   1. Collect all (topic, partition) pairs across all subscribed topics of all
//      members; sort them lexicographically: topic first, then partition index.
//   2. Collect all subscribing members (the UNION of all members), sorted.
//   3. Iterate the (topic,partition) pairs in order; for each pair find the
//      next eligible member (round-robin) that subscribes to that topic.
//      If a member doesn't subscribe to a topic, skip it in the rotation for
//      that pair.
//   This mirrors Kafka's RoundRobinAssignor behaviour.
//
// ## topicPartitionCounts
//
//   Passed as a map<string topic, int32_t partitionCount>. If a member
//   subscribes to a topic not in this map, that topic is silently ignored.
//
// ## No hot-path alloc
//
//   Temporary vectors are reused; the final result map is the only allocation.
// ---------------------------------------------------------------------------

#include <rune/coordination/Types.h>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// TopicPartitionCounts — map from topic name to number of partitions.
// ---------------------------------------------------------------------------
using TopicPartitionCounts = std::map<std::string, int32_t>;

// ---------------------------------------------------------------------------
// MemberSubscription — a single member's id + the topics it subscribes to.
// ---------------------------------------------------------------------------
struct MemberSubscription {
    std::string              memberId;
    std::vector<std::string> subscribedTopics; ///< may contain topics not in counts map
};

// ---------------------------------------------------------------------------
// Assignment — result of assign(): member → list of assigned TopicPartitions.
// ---------------------------------------------------------------------------
using Assignment = std::map<std::string, std::vector<TopicPartition>>;

// ---------------------------------------------------------------------------
// PartitionAssignor — pure interface.
// ---------------------------------------------------------------------------
class PartitionAssignor {
public:
    virtual ~PartitionAssignor() = default;

    /// Compute a partition assignment.
    ///
    /// \param members    All members with their subscriptions. Must be non-empty.
    /// \param partCounts Map of topic → partition count. Topics not in this map
    ///                   are silently ignored even if subscribed.
    /// \returns Assignment map: memberId → vector<TopicPartition>.
    ///          Every member present in \p members appears as a key (even if
    ///          their assignment is empty). Every partition of every known
    ///          topic that at least one member subscribes to is assigned
    ///          exactly once to a subscribing member.
    [[nodiscard]] virtual Assignment assign(
        const std::vector<MemberSubscription>& members,
        const TopicPartitionCounts&            partCounts) const = 0;
};

// ---------------------------------------------------------------------------
// RangeAssignor — Kafka Range semantics.
// ---------------------------------------------------------------------------
class RangeAssignor final : public PartitionAssignor {
public:
    [[nodiscard]] Assignment assign(
        const std::vector<MemberSubscription>& members,
        const TopicPartitionCounts&            partCounts) const override;
};

// ---------------------------------------------------------------------------
// RoundRobinAssignor — Kafka RoundRobin semantics.
// ---------------------------------------------------------------------------
class RoundRobinAssignor final : public PartitionAssignor {
public:
    [[nodiscard]] Assignment assign(
        const std::vector<MemberSubscription>& members,
        const TopicPartitionCounts&            partCounts) const override;
};

} // namespace rune::coordination
