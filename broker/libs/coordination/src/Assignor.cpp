// ---------------------------------------------------------------------------
// Assignor.cpp — Kafka-faithful RangeAssignor + RoundRobinAssignor.
//
// Namespace: rune::coordination
// C++23. NO hot-path alloc beyond the result Assignment map.
// Deterministic: members and partitions sorted before assignment.
// ---------------------------------------------------------------------------

#include <rune/coordination/Assignor.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// Shared helpers (file-local)
// ---------------------------------------------------------------------------

/// Return a sorted list of member ids that subscribe to a given topic.
static std::vector<std::string> subscribersFor(
    const std::string&                     topic,
    const std::vector<MemberSubscription>& members)
{
    std::vector<std::string> subs;
    for (const auto& m : members) {
        for (const auto& t : m.subscribedTopics) {
            if (t == topic) { subs.push_back(m.memberId); break; }
        }
    }
    std::sort(subs.begin(), subs.end());
    return subs;
}

/// Return sorted member ids that subscribe to at least one topic in partCounts.
static std::vector<std::string> allSubscribingMembers(
    const std::vector<MemberSubscription>& members,
    const TopicPartitionCounts&            partCounts)
{
    // Build set of known topics.
    std::set<std::string> knownTopics;
    for (const auto& [t, _] : partCounts) knownTopics.insert(t);

    std::set<std::string> result;
    for (const auto& m : members) {
        for (const auto& t : m.subscribedTopics) {
            if (knownTopics.count(t)) { result.insert(m.memberId); break; }
        }
    }
    return std::vector<std::string>(result.begin(), result.end());
}

// ---------------------------------------------------------------------------
// RangeAssignor::assign
//
// Kafka Range algorithm per topic:
//   subscribers for topic sorted lexicographically.
//   base = N / S, extras = N % S.
//   member[i] gets [start, start + range) where
//     range = base + (i < extras ? 1 : 0)
//     start = sum of previous ranges.
// ---------------------------------------------------------------------------
Assignment RangeAssignor::assign(
    const std::vector<MemberSubscription>& members,
    const TopicPartitionCounts&            partCounts) const
{
    // Initialize result: every member gets an entry (even if empty).
    Assignment result;
    for (const auto& m : members) {
        result.emplace(m.memberId, std::vector<TopicPartition>{});
    }

    // Per-topic assignment.
    for (const auto& [topic, numPartitions] : partCounts) {
        if (numPartitions <= 0) continue;

        // Collect and sort subscribers for this topic.
        auto subs = subscribersFor(topic, members);
        if (subs.empty()) continue;

        int32_t S = static_cast<int32_t>(subs.size());
        int32_t base   = numPartitions / S;
        int32_t extras = numPartitions % S;
        int32_t start  = 0;

        for (int32_t i = 0; i < S; ++i) {
            int32_t range = base + (i < extras ? 1 : 0);
            auto& memberAssignment = result[subs[i]];
            for (int32_t p = start; p < start + range; ++p) {
                memberAssignment.push_back({topic, p});
            }
            start += range;
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// RoundRobinAssignor::assign
//
// Kafka RoundRobin algorithm:
//   1. Build sorted list of all (topic, partition) pairs across subscribed topics.
//   2. Sort all subscribed members lexicographically (union across topics).
//   3. For each pair, find the next eligible member (round-robin by index)
//      that subscribes to that topic; assign to it.
// ---------------------------------------------------------------------------
Assignment RoundRobinAssignor::assign(
    const std::vector<MemberSubscription>& members,
    const TopicPartitionCounts&            partCounts) const
{
    // Initialize result: every member gets an entry.
    Assignment result;
    for (const auto& m : members) {
        result.emplace(m.memberId, std::vector<TopicPartition>{});
    }

    // Collect all (topic, partition) pairs across known subscribed topics.
    // A topic must appear in partCounts AND be subscribed by at least one member.
    std::set<std::string> knownTopics;
    for (const auto& [t, _] : partCounts) knownTopics.insert(t);

    std::vector<TopicPartition> pairs;
    for (const auto& [topic, count] : partCounts) {
        if (count <= 0) continue;
        // At least one member must subscribe.
        bool anySubscriber = false;
        for (const auto& m : members) {
            for (const auto& t : m.subscribedTopics) {
                if (t == topic) { anySubscriber = true; break; }
            }
            if (anySubscriber) break;
        }
        if (!anySubscriber) continue;
        for (int32_t p = 0; p < count; ++p) {
            pairs.push_back({topic, p});
        }
    }
    // Sort pairs: topic first, then partition index (TopicPartition::operator< does this).
    std::sort(pairs.begin(), pairs.end());

    // Collect all subscribed members sorted (global, not per-topic).
    std::vector<std::string> allMembers = allSubscribingMembers(members, partCounts);

    // Build a fast membership-in-topic lookup.
    // subscriberSet[topic] = set of memberIds that subscribe to it.
    std::map<std::string, std::set<std::string>> subscriberSet;
    for (const auto& m : members) {
        for (const auto& t : m.subscribedTopics) {
            if (knownTopics.count(t)) {
                subscriberSet[t].insert(m.memberId);
            }
        }
    }

    // Round-robin: single monotonic cursor over allMembers.
    //
    // Kafka's canonical RoundRobinAssignor (Java source):
    //   for each (topic,partition) in sorted order:
    //     while current member does not subscribe to topic: advance cursor
    //     assign to current member; advance cursor
    //
    // Implementation: start at idx; scan forward (at most M positions) for
    // the next subscribed member; assign; advance cursor past the subscriber.
    // Non-subscribing members are skipped but the cursor lands on (and then
    // advances past) the first subscriber — matching Kafka exactly.
    int32_t idx = 0; // rotating cursor into allMembers
    int32_t M   = static_cast<int32_t>(allMembers.size());

    for (const auto& tp : pairs) {
        const auto& subs = subscriberSet[tp.topic];
        if (subs.empty()) continue;

        // Find next member (starting from idx) that subscribes to tp.topic.
        // Scan at most M positions (bounded, guaranteed to find one for
        // non-empty subs).
        for (int32_t attempt = 0; attempt < M; ++attempt) {
            int32_t candidate = (idx + attempt) % M;
            if (subs.count(allMembers[candidate])) {
                result[allMembers[candidate]].push_back(tp);
                // Advance idx to candidate+1 (past the assigned member),
                // matching Kafka's ring-iterator advance.
                idx = (candidate + 1) % M;
                break;
            }
        }
    }

    return result;
}

} // namespace rune::coordination
