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

#include <chrono>
#include <cstdint>
#include <iterator>
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

/// D11: the PARTITION leader of (topicId, partition) among `ns` — the node whose
/// replica reports Role::Leader on its owner thread while every other node's
/// replica is a Follower (a settled assignment, not a hand-over in flight).  It
/// is NOT the consensus leader (BrokerNode::leaderId()): partition leadership is
/// assigned by the metadata controller (initially the static assignment), so a
/// test that sends a Produce to the consensus leader's gateway gets an
/// ErrorResponse (Redirect / NotLeader) whenever another node wins the consensus
/// election — which happens under load.  Condition poll with a deadline (each
/// probe is an owner-thread round trip on a live node).  Returns the index into
/// `ns`, or -1.
template <class Nodes>
int settledPartitionLeader(const Nodes& ns, std::chrono::milliseconds within = std::chrono::seconds(30),
                           int32_t topicId = 0, int32_t partition = 0) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    const int n = static_cast<int>(std::size(ns));
    while (std::chrono::steady_clock::now() < deadline) {
        int leader = -1, followers = 0;
        for (int i = 0; i < n; ++i) {
            BrokerNode& b = *ns[static_cast<std::size_t>(i)];
            if (!hasReplica(b, topicId, partition)) break;
            const Role r = roleOnOwner(b, topicId, partition);
            if (r == Role::Leader) leader = (leader == -1) ? i : -2;
            else if (r == Role::Follower) ++followers;
        }
        if (leader >= 0 && followers == n - 1) return leader;
    }
    return -1;
}

} // namespace rune::node::test
