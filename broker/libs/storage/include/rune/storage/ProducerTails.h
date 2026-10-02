#pragma once
// ---------------------------------------------------------------------------
// ProducerTails — the last data batch of every idempotent producer in the
// range a cleaner pass scanned (Kafka LogCleaner parity:
// isBatchLastRecordOfProducer / lastRecordsOfActiveProducers).
//
// Compaction must never remove a producer's LAST batch: the producer state a
// leader rebuilds from the log (failover, restart — ProducerStateManager::
// rebuildFromLog, the worker's RC build) learns (epoch, lastSequence) only from
// it.  Without it a resend of a STORED batch meets no / a stale entry and is
// answered OutOfOrderSequence ("NOT appended": the client re-sequences and
// writes it twice) or, at seq == the stale lastSequence + 1, accepted and
// appended again.  The cleaner retains the LAST record of that batch even when
// its key is superseded (re-framed as a single-record batch whose sequence is
// the batch's last sequence, epoch and producer id preserved).  Bounded: one
// record per producer, released by the next pass once the producer has a later
// batch below the pass boundary.
//
// Cleaner thread only (cold; the cleaner is the one sanctioned heap user).
// ---------------------------------------------------------------------------
#include <cstdint>
#include <unordered_map>

namespace rune::storage {

class ProducerTails {
public:
    /// Record an idempotent DATA batch of `producerId` ending at `lastOffset`.
    void note(int64_t producerId, int64_t lastOffset) {
        auto [it, inserted] = tails_.try_emplace(producerId, lastOffset);
        if (!inserted && lastOffset > it->second) it->second = lastOffset;
    }
    /// True iff `offset` is the last offset of `producerId`'s last data batch.
    [[nodiscard]] bool isTail(int64_t producerId, int64_t offset) const noexcept {
        const auto it = tails_.find(producerId);
        return it != tails_.end() && it->second == offset;
    }
    [[nodiscard]] std::size_t size() const noexcept { return tails_.size(); }
    void clear() noexcept { tails_.clear(); }

private:
    std::unordered_map<int64_t, int64_t> tails_;
};

} // namespace rune::storage
