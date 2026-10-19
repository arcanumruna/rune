#pragma once
// ---------------------------------------------------------------------------
// ReplicaPositionTable.h — Soft per-replica position table.
//
// Stores the latest (leaderEpoch, logEndPosition) report for each
// (topicId, partition, nodeId) triple.  This is SOFT STATE — rebuilt on
// controller failover, never persisted to the consensus log.
//
// Keyed by a compact uint64 derived from (gpid << 16 | nodeId) so the table
// is a flat unordered_map with integer keys (no string hashing, no heap per
// key).  Bounded allocation: at most |replicas| * |partitions| entries.
//
// Consumed by the controller's failover-election path to select the most
// up-to-date replica as the next leader.
//
// Thread-safety: NOT thread-safe.  The controller drives this from a single
// thread (the metadata state-machine / control-plane duty cycle).
// ---------------------------------------------------------------------------
#include <rune/replication/PartitionReplica.h> // ReplicaPositionReport
#include <rune/metadata/Gpid.h>
#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>

namespace rune::metadata {

class ReplicaPositionTable {
public:
    /// Record the latest position for a replica.  "Latest wins" semantics:
    /// a newer report for the same (topicId, partition, nodeId) overwrites the old one.
    void report(const rune::replication::ReplicaPositionReport& r) {
        table_[key(r.topicId, r.partition, r.nodeId)] = r; // latest wins (soft)
    }

    /// Retrieve the latest report for a specific replica.
    /// Returns std::nullopt if no report has been received yet.
    std::optional<rune::replication::ReplicaPositionReport>
    get(int32_t topicId, int32_t partition, int32_t nodeId) const {
        auto it = table_.find(key(topicId, partition, nodeId));
        if (it == table_.end()) return std::nullopt;
        return it->second;
    }

    /// Fix round 2 N2: forget every report of `nodeId` (a new process
    /// incarnation: its log may be gone).  Cold.
    void dropNode(int32_t nodeId) {
        const auto low = static_cast<uint64_t>(static_cast<uint16_t>(nodeId));
        std::erase_if(table_, [low](const auto& kv) { return (kv.first & 0xffffu) == low; });
    }

    /// Return all known replica positions for a given (topicId, partition).
    /// Used by the election path to find the highest-LEO candidate.
    std::vector<rune::replication::ReplicaPositionReport>
    forPartition(int32_t topicId, int32_t partition) const {
        std::vector<rune::replication::ReplicaPositionReport> out;
        const int32_t g = gpidOf(topicId, partition);
        for (const auto& [k, v] : table_)
            if (static_cast<int32_t>(k >> 16) == g) out.push_back(v);
        return out;
    }

private:
    /// Compact key: (gpid << 16) | (nodeId as uint16_t).
    /// gpid uses the standard kMaxGpidPartitions encoding; nodeId is clamped
    /// to the low 16 bits (node ids are small in practice).
    static uint64_t key(int32_t topicId, int32_t partition, int32_t nodeId) {
        return (static_cast<uint64_t>(gpidOf(topicId, partition)) << 16)
             |  static_cast<uint64_t>(static_cast<uint16_t>(nodeId));
    }

    std::unordered_map<uint64_t, rune::replication::ReplicaPositionReport> table_;
};

} // namespace rune::metadata
