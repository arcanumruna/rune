#pragma once
// ReplicaRegistry.h — Per-partition PartitionReplica registry (GR-3).
//
// Maps (topicId, partition) → PartitionReplica, paralleling TopicRegistry.
// Owns the PartitionReplica instances; the TopicRegistry owns the Log instances.
// Both share the same non-owning Log* pointer.
//
// Threading: SINGLE-WRITER — all access must be on the AgentRunner duty-cycle thread.
// Constructed in BrokerNode::start() after TopicRegistry (so Log* are valid).

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

namespace rune::node {

class ReplicaRegistry {
public:
    ReplicaRegistry() = default;

    // Non-copyable, non-movable (owns PartitionReplica instances).
    ReplicaRegistry(const ReplicaRegistry&)            = delete;
    ReplicaRegistry& operator=(const ReplicaRegistry&) = delete;

    // -----------------------------------------------------------------------
    // add — register a pre-constructed PartitionReplica.
    // -----------------------------------------------------------------------
    void add(int32_t topicId, int32_t partition,
             std::unique_ptr<rune::replication::PartitionReplica> replica)
    {
        const Key key{topicId, partition};
        map_[key] = std::move(replica);
        ordered_.push_back(map_[key].get());
    }

    // -----------------------------------------------------------------------
    // lookup — non-owning pointer; nullptr if not found.
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::replication::PartitionReplica*
    lookup(int32_t topicId, int32_t partition) const noexcept
    {
        auto it = map_.find(Key{topicId, partition});
        return (it != map_.end()) ? it->second.get() : nullptr;
    }

    // -----------------------------------------------------------------------
    // lookupByPartitionId — for AeronPartitionTransport routing.
    // For the DR scope (single-topic cluster), partitionId alone is sufficient.
    // Scans in insertion order; O(n) but n is small (partitions per node).
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::replication::PartitionReplica*
    lookupByPartitionId(rune::replication::PartitionId pid) const noexcept
    {
        for (auto* r : ordered_) {
            if (r && r->partitionId() == pid) return r;
        }
        return nullptr;
    }

    // -----------------------------------------------------------------------
    // allReplicas — ordered view for duty-cycle doWork iteration.
    // -----------------------------------------------------------------------
    [[nodiscard]] const std::vector<rune::replication::PartitionReplica*>&
    allReplicas() const noexcept { return ordered_; }

    // -----------------------------------------------------------------------
    // replicaCount
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t replicaCount() const noexcept { return ordered_.size(); }

private:
    struct Key {
        int32_t topicId;
        int32_t partition;
        bool operator==(const Key& o) const noexcept {
            return topicId == o.topicId && partition == o.partition;
        }
    };
    struct KeyHash {
        std::size_t operator()(const Key& k) const noexcept {
            std::size_t h = static_cast<std::size_t>(k.topicId);
            h ^= (static_cast<std::size_t>(k.partition) + 0x9e3779b9u + (h << 6) + (h >> 2));
            return h;
        }
    };

    std::unordered_map<Key, std::unique_ptr<rune::replication::PartitionReplica>, KeyHash> map_;
    std::vector<rune::replication::PartitionReplica*> ordered_;  // stable insertion order
};

} // namespace rune::node
