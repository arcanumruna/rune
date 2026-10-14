// CatalogPartition.cpp — the `__catalog` PartitionConfig (catalog history spec §3).
#include <rune/node/CatalogPartition.h>
#include <rune/node/MetadataService.h>
#include <rune/node/NodeConfig.h>

#include <rune/protocol/client/Protocol.h>

namespace rune::node {

static_assert(kCatalogTopicId == rune::protocol::client::kCatalogTopicId,
              "the broker's __catalog topic id must equal the wire's");
static_assert(MetadataService::isControllerIsrInternalTopic(kCatalogTopicId),
              "__catalog is on the controller-ISR path (ruling 7)");

rune::replication::PartitionConfig makeCatalogPartitionConfig(const NodeConfig& config,
                                                              std::size_t brokers) {
    rune::replication::PartitionConfig c;
    c.topicId             = kCatalogTopicId;
    c.partitionId         = kCatalogPartition;
    c.selfId              = static_cast<rune::replication::NodeId>(config.nodeId);
    c.minIsr              = catalogMinIsr(brokers);
    // Majority durability with periodic fsync off (the default) is Kafka
    // parity: a power loss on a MAJORITY of brokers can lose committed catalog
    // events (DUR-1 only protects followers).  Review M8 (closed in Task 7):
    // the persisted `applied` checkpoint (ruling 8) refuses to boot when it
    // names an offset at or past the recovered LEO (BrokerNode::buildCatalog_,
    // CatalogApplier::replayLocal) — never a shorter history replayed silently.
    c.durability          = rune::replication::DurabilityMode::Majority;
    c.durabilityTier      = 1;   // Majority: acked on the replicated HW
    c.replicaLagMaxMs     = config.replicaLagMaxMs;
    c.replicaLagMaxOffset = 1'000'000'000;   // TIME-based lag only (as the coordinator partitions)
    c.fetchIntervalNs     = config.fetchIntervalNs;
    c.isrCheckIntervalNs  = 10'000'000LL;
    c.maxFetchBytes       = rune::replication::kDefaultMaxFetchBytes;
    // Never compacted, never retention-deleted: the catalog is history.
    c.cleanupPolicy       = rune::replication::CleanupPolicy::Delete;
    c.retentionMs         = -1;
    c.retentionBytes      = -1;
    c.logStartOffset      = 0;
    c.segmentRollBytes    = config.offsetLogSegmentRollBytes;
    return c;
}

} // namespace rune::node
