#pragma once
// CatalogPartition.h — the `__catalog` partition (catalog history spec §3, §5.6).
//
// "One partition, replicated to every broker.  Written with acks=all; minIsr is
//  a majority.  Unclean leader election is disabled.  Never compacted, never
//  retention-deleted.  Always disk-backed.  It is never transient.  It is the
//  single source of truth for the catalog."
//
// Ownership (RULING-2 A, hard rules 1/2): the catalog PartitionReplica, its
// disk Log and its replication plane are owned and driven by the ADMIN thread
// only (AdminWorker::doWork).  The replica is never in the control thread's
// ReplicaRegistry.  Its leadership comes from the controller-ISR path
// (MetadataService::decideCoordinatorPartition, ruling 7): the bridge relays the
// committed (-6, 0) assignment on the control→admin role ring, and the replica's
// ISR proposals (AlterIsr) travel admin→control on the admin→control ring
// (AdminIsrChannel) to the same controller channel the coordinator partitions
// use.  Single node: the consensus leader drives it (no controller path).
//
// This header stays free of NodeConfig.h (the bridge includes it; NodeConfig's
// rune::node::TopicSpec collides with rune::metadata::TopicSpec in tests).

#include <rune/node/AdminLogPartition.h>   // the generic helpers this header's names delegate to

namespace rune::node {

struct NodeConfig;

/// The catalog's topic id (== rune::protocol::client::kCatalogTopicId; pinned by
/// a static_assert in CatalogPartition.cpp).  Partition 0 is its only partition.
inline constexpr int32_t kCatalogTopicId   = kAdminLogCatalogTopicId;   // -6
inline constexpr int32_t kCatalogPartition = kAdminLogPartition;

/// minIsr of `__catalog`: a majority of the brokers.
[[nodiscard]] inline int32_t catalogMinIsr(std::size_t brokers) noexcept { return adminLogMinIsr(brokers); }

/// The `__catalog` PartitionConfig (makeAdminLogPartitionConfig for -6).
[[nodiscard]] inline rune::replication::PartitionConfig makeCatalogPartitionConfig(const NodeConfig& config,
                                                                                   std::size_t brokers) {
    return makeAdminLogPartitionConfig(config, brokers, kCatalogTopicId);
}

using CatalogFrameStats = AdminLogFrameStats;

/// Apply one replication frame received on the catalog plane to the catalog replica (admin thread) — the generic
/// dispatch with only -6 mapped.
inline void dispatchCatalogFrame(rune::replication::PartitionReplica& replica,
                                 int32_t topicId, int32_t partition, uint16_t templateId,
                                 const uint8_t* frame, std::size_t len, int64_t nowNs,
                                 CatalogFrameStats& stats) noexcept {
    dispatchAdminLogFrame(AdminLogReplicas{&replica, nullptr}, topicId, partition, templateId, frame, len, nowNs, stats);
}

} // namespace rune::node
