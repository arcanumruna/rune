#pragma once
#include <rune/storage/Timestamps.h>
// ---------------------------------------------------------------------------
// ProducerTails — the last data batch of every idempotent producer in the
// range a cleaner pass scanned (Kafka LogCleaner parity:
// lastRecordsOfActiveProducers / isBatchLastRecordOfProducer + RETAIN_EMPTY).
//
// Compaction must never remove a producer's LAST batch METADATA: the producer
// state a leader rebuilds from the log (failover, restart — ProducerState-
// Manager::rebuildFromLog, the worker's RC build) learns (epoch, lastSequence)
// only from it.  Without it a resend of a STORED batch meets no / a stale entry
// and is answered OutOfOrderSequence ("NOT appended": the client re-sequences
// and writes it twice) or, at seq == the stale lastSequence + 1, appended
// again.  Like Kafka, the cleaner keeps only the METADATA: when the batch's
// last record would be dropped (superseded, expired tombstone) it emits a
// RECORD-LESS batch covering that offset (producer id, epoch, last sequence,
// maxTimestamp; recordCount 0, lastOffsetDelta 0) — no deleted key can come
// back (fix round 1, C2: retaining the superseded record resurrected keys once
// their tombstone expired).
//
// Expiry (I1, Kafka producer.id.expiration.ms): a producer whose last batch's
// maxTimestamp is older than nowMs - expirationMs is no longer retained (the
// rebuild then forgets it — see the UNKNOWN_PRODUCER_ID ticket).  nowMs < 0
// (legacy passes without a wall clock) never expires.
//
// Bounded (I1): at most `capacity` producers (the cleaner sizes it from its
// dedupe budget).  Past it new producers are not recorded and the map is
// OVERFLOWED: every unexpired data batch of an unrecorded producer then keeps
// its metadata (conservative — compaction of those headers degrades, never the
// contract).
//
// Cleaner thread only (cold; the cleaner is the one sanctioned heap user).
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <limits>
#include <unordered_map>

namespace rune::storage {

class ProducerTails {
public:
    static constexpr std::size_t kUnbounded = std::numeric_limits<std::size_t>::max();
    /// Default producer.id.expiration.ms for the cleaner: 7 days.
    static constexpr int64_t kDefaultExpirationMs = 7LL * 24 * 3600 * 1000;
    /// Bytes one tracked producer costs (node + bucket, approximate) — for
    /// sizing the cap from a byte budget.
    static constexpr std::size_t kBytesPerProducer = 64;

    explicit ProducerTails(std::size_t capacity = kUnbounded) : capacity_(capacity) {}

    void setCapacity(std::size_t capacity) noexcept { capacity_ = capacity; }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    /// Record an idempotent DATA batch of `producerId` ending at `lastOffset`
    /// with batch maxTimestamp `maxTimestampMs`.
    void note(int64_t producerId, int64_t lastOffset, int64_t maxTimestampMs) {
        auto it = tails_.find(producerId);
        if (it == tails_.end()) {
            if (tails_.size() >= capacity_) { overflowed_ = true; return; }
            tails_.emplace(producerId, Tail{lastOffset, maxTimestampMs});
            return;
        }
        if (lastOffset > it->second.lastOffset) it->second = Tail{lastOffset, maxTimestampMs};
    }
    /// Must the data batch of `producerId` ending at `lastOffset` (maxTimestamp
    /// `maxTimestampMs`) keep its producer metadata?
    [[nodiscard]] bool mustRetain(int64_t producerId, int64_t lastOffset, int64_t maxTimestampMs,
                                  int64_t nowMs, int64_t expirationMs) const noexcept {
        if (nowMs >= 0 && expirationMs >= 0 && toEpochMs(maxTimestampMs) < nowMs - expirationMs) return false;   // expired (ns or ms stamps)
        const auto it = tails_.find(producerId);
        if (it != tails_.end()) return it->second.lastOffset == lastOffset;
        return overflowed_;   // unrecorded (map full): keep it, conservatively
    }
    [[nodiscard]] bool isTail(int64_t producerId, int64_t offset) const noexcept {
        const auto it = tails_.find(producerId);
        return it != tails_.end() && it->second.lastOffset == offset;
    }
    [[nodiscard]] bool overflowed() const noexcept { return overflowed_; }
    [[nodiscard]] std::size_t size() const noexcept { return tails_.size(); }
    void clear() noexcept { tails_.clear(); overflowed_ = false; }

private:
    struct Tail {
        int64_t lastOffset;
        int64_t maxTimestampMs;
    };
    std::unordered_map<int64_t, Tail> tails_;
    std::size_t capacity_   = kUnbounded;
    bool        overflowed_ = false;
};

} // namespace rune::storage
