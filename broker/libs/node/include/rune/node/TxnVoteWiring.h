#pragma once
// TxnVoteWiring.h - checked writes Part 11: BrokerNode's half of the coordinator vote.  VoteCandidates is the
// CONTROL-thread set behind TransactionCoordinationService::VoteCandidateFn; wireTxnVote / releaseTxnCoordinator
// attach and detach the coordinator, the vote sink and the candidate predicate on the gateway.
//
// The set is a superset of the topics whose partition leader judges a commit (keyed topics, every source of a view):
// a false positive costs one yes round trip, a false negative would commit past a no.  It is never read from the
// catalog validator (admin thread).
#include <rune/gateway/ClientGateway.h>
#include <rune/node/TransactionCoordinationService.h>

#include <cstdint>
#include <memory>
#include <span>
#include <unordered_set>

namespace rune::node {

class VoteCandidates {
public:
    void noteWriteSpec(int32_t topicId, bool keyIndex) { if (keyIndex) topics_.insert(topicId); }
    void noteViewSources(std::span<const int32_t> sourceTopicIds) { topics_.insert(sourceTopicIds.begin(), sourceTopicIds.end()); }
    void forget(int32_t topicId) { topics_.erase(topicId); }
    [[nodiscard]] bool contains(int32_t topicId) const { return topics_.contains(topicId); }

private:
    std::unordered_set<int32_t> topics_;
};

inline void wireTxnVote(rune::gateway::ClientGateway& gateway, TransactionCoordinationService& txn, const VoteCandidates& candidates) {
    txn.setVoteCandidateFn([&candidates](int32_t topicId) { return candidates.contains(topicId); });
    gateway.setTxnVoteSink([](void* self, int32_t topicId, int32_t partition, int64_t producerId, int16_t producerEpoch,
                              int32_t coordinatorEpoch, const rune::gateway::TxnVoteResult& r) noexcept {
        static_cast<TransactionCoordinationService*>(self)->onTxnVoteResult(topicId, partition, producerId, producerEpoch, coordinatorEpoch, r);
    }, &txn);
}

/// The gateway holds raw pointers to the coordinator: detach them before it dies.
inline void releaseTxnCoordinator(rune::gateway::ClientGateway* gateway, std::unique_ptr<TransactionCoordinationService>& txn) {
    if (gateway) {
        gateway->setTxnVoteSink(nullptr, nullptr);
        gateway->setTxnService(nullptr);
    }
    txn.reset();
}

} // namespace rune::node
