// AdminLogPartition.cpp — the PartitionConfig every admin-owned replicated log shares (authentication plan D39).
#include <rune/node/AdminLogPartition.h>
#include <rune/node/NodeConfig.h>

namespace rune::node {

rune::replication::PartitionConfig makeAdminLogPartitionConfig(const NodeConfig& config, std::size_t brokers,
                                                               int32_t topicId) {
    rune::replication::PartitionConfig c;
    c.topicId             = topicId;
    c.partitionId         = kAdminLogPartition;
    c.selfId              = static_cast<rune::replication::NodeId>(config.nodeId);
    c.minIsr              = adminLogMinIsr(brokers);
    // Majority durability with periodic fsync off (the default) is Kafka
    // parity: a power loss on a MAJORITY of brokers can lose committed admin-log
    // events (DUR-1 only protects followers).  Review M8 (closed in catalog Task 7):
    // the persisted `applied` checkpoint (ruling 8) refuses to boot when it
    // names an offset at or past the recovered LEO (BrokerNode::bootFrom*_,
    // the appliers' replayLocal) — never a shorter history replayed silently.
    c.durability          = rune::replication::DurabilityMode::Majority;
    c.durabilityTier      = 1;   // Majority: acked on the replicated HW
    c.replicaLagMaxMs     = config.replicaLagMaxMs;
    c.replicaLagMaxOffset = 1'000'000'000;   // TIME-based lag only (as the coordinator partitions)
    c.fetchIntervalNs     = config.fetchIntervalNs;
    c.isrCheckIntervalNs  = 10'000'000LL;
    c.maxFetchBytes       = rune::replication::kDefaultMaxFetchBytes;
    // Never compacted, never retention-deleted: an admin log is history.
    c.cleanupPolicy       = rune::replication::CleanupPolicy::Delete;
    c.retentionMs         = -1;
    c.retentionBytes      = -1;
    c.logStartOffset      = 0;
    c.segmentRollBytes    = config.offsetLogSegmentRollBytes;
    return c;
}

} // namespace rune::node
