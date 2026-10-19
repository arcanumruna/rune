#pragma once
// ---------------------------------------------------------------------------
// Types.h — Core type aliases for the rune::coordination library.
//
// Namespace: rune::coordination
// C++23
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>

namespace rune::coordination {

/// Consumer group identifier (UTF-8 string, matching Kafka's group.id semantics).
using GroupId = std::string;

/// Absolute log offset per partition (matches Kafka int64 offset).
using Offset = int64_t;

/// Consumer group generation (epoch bumped on every membership change).
using Generation = int32_t;

/// A (topic, partition) pair identifying a single partition within a topic.
/// Hashable so it can be used as a map key.
struct TopicPartition {
    std::string topic;
    int32_t     partition{0};

    bool operator==(const TopicPartition& o) const noexcept {
        return partition == o.partition && topic == o.topic;
    }
    bool operator<(const TopicPartition& o) const noexcept {
        if (topic != o.topic) return topic < o.topic;
        return partition < o.partition;
    }
};

} // namespace rune::coordination

// ---------------------------------------------------------------------------
// std::hash specialisation — allows TopicPartition as unordered_map key.
// ---------------------------------------------------------------------------
template<>
struct std::hash<rune::coordination::TopicPartition> {
    std::size_t operator()(const rune::coordination::TopicPartition& tp) const noexcept {
        // FNV-1a combine of partition + topic string hash.
        std::size_t h = std::hash<std::string>{}(tp.topic);
        h ^= std::hash<int32_t>{}(tp.partition) + 0x9e3779b9u + (h << 6) + (h >> 2);
        return h;
    }
};
