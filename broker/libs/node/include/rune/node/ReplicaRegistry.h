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

#include <algorithm>
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
    // remove — quarantine (erase) the replica keyed by (topicId, partition).
    //
    // Called by ReplicationMetadataBridge::applyDeleteForKey when the committed
    // assignment map no longer includes this node for the partition (Note B /
    // spec §16 stray-replica quarantine).  After removal, lookup() returns
    // nullptr for that key so the deleted partition can never be routed to again.
    //
    // The replica's unique_ptr is destroyed here — the underlying Log and disk
    // data are NOT touched (they are owned by the TopicRegistry / the caller's
    // layer).  A replica id is never reused: if the same topic/partition is
    // later recreated with a new (topicId, partition) pair it will be a fresh
    // add() call; the old quarantined log is never resurrected.
    //
    // Complexity: O(n) for the ordered_ erase (n = total registered replicas;
    // typically small).  Hot path is add/lookup; remove is uncommon (assignment
    // change).
    // -----------------------------------------------------------------------
    void remove(int32_t topicId, int32_t partition)
    {
        const Key key{topicId, partition};
        auto it = map_.find(key);
        if (it == map_.end()) return;  // already absent — idempotent

        // Remove the raw pointer from ordered_ before destroying the unique_ptr
        // so ordered_ never holds a dangling pointer even transiently.
        auto* raw = it->second.get();
        auto  oit = std::find(ordered_.begin(), ordered_.end(), raw);
        if (oit != ordered_.end()) ordered_.erase(oit);

        // Destroy the owned PartitionReplica.
        map_.erase(it);
    }

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
