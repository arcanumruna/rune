#pragma once
// OwnerThreadTestHelpers.h — F1 test helpers over BrokerNode::onOwnerThreadForTest.
//
// Every read/mutation of a PartitionReplica / its Log in an integration test MUST
// run on the replica's OWNING thread (worker or control), never on the test
// thread.  These helpers wrap the common single-value probes so call sites stay
// as terse as the old raw-pointer reads while being race-free.
//
// All helpers throw (→ test failure) if the replica does not exist, except the
// *OrMinusOne variants, which return -1 (for polls that tolerate a replica that
// is not built yet).
//
// Test-only: must NOT be included from production headers.

#include <rune/node/BrokerNode.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>

#include <cstdint>
#include <span>
#include <vector>

namespace rune::node::test {

using rune::replication::Offset;
using rune::replication::PartitionReplica;
using rune::replication::Role;

inline Offset leoOnOwner(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica& r) { return r.logEndOffset(); });
}

/// LEO, or -1 when the node has no replica for (topicId, partition).
inline Offset leoOrMinusOne(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica* r) -> Offset { return r ? r->logEndOffset() : Offset{-1}; });
}

inline Offset hwOnOwner(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica& r) { return r.highWatermark(); });
}

/// Duty-thread-published cleaner HW snapshot, or -1 when there is no replica.
inline Offset cleanerHwOrMinusOne(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica* r) -> Offset { return r ? r->cleanerHighWatermark() : Offset{-1}; });
}

inline Role roleOnOwner(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica& r) { return r.role(); });
}

/// Live leader ISR size (isr().size()).
inline int isrSizeOnOwner(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica& r) { return static_cast<int>(r.isr().size()); });
}

/// Gateway-published ISR size snapshot (gatewayIsrSize()).
inline int gatewayIsrSizeOnOwner(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica& r) { return r.gatewayIsrSize(); });
}

inline bool hasReplica(BrokerNode& n, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [](PartitionReplica* r) { return r != nullptr; });
}

/// PartitionReplica::append on the owning thread (pushes inline on the owner's
/// own publication — exactly the single-writer path production uses).
inline Offset appendOnOwner(BrokerNode& n, std::span<const uint8_t> records,
                            bool acksAll, int32_t topicId = 0, int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [records, acksAll](PartitionReplica& r) { return r.append(records, acksAll); });
}

/// Raw Log::read on the owning thread (same Log object the TopicRegistry holds
/// for (topicId, partition) — BrokerNode wires replica->log() from it).
inline std::vector<uint8_t> readLogOnOwner(BrokerNode& n, int64_t startOffset,
                                           int32_t maxBytes, int32_t topicId = 0,
                                           int32_t partition = 0) {
    return n.onOwnerThreadForTest(topicId, partition,
        [startOffset, maxBytes](PartitionReplica& r) {
            return r.log()->read(startOffset, maxBytes);
        });
}

} // namespace rune::node::test
