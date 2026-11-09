#pragma once
// LeaseRuntime.h - lease topics Task 10 (spec 9.1, 9.3, 9.4): the per-partition lease runtime of a partition LEADER.
//
// Owns the entry store and both wheels (LeaseTimers), folds the judge's LeaseEvents into the entries (onEvents) and turns due
// deadlines into one batch of Expire records per duty cycle (emit).  It exists only on the leader, while the lease index is ready,
// under one identity (leadership generation, lease rebuild epoch); the Worker destroys it when either changes (a demotion destroys
// every timer: nothing is carried over, spec 9.7).  Shared-nothing: owned and driven by the partition's worker thread only.
//
// This header does not include LeaseIndex.h (WriteState.h includes this header, LeaseIndex.h includes WriteState.h): emit()
// takes the lease index types by reference and LeaseRuntime.cpp includes them.
#include <rune/node/lease/LeaseFoldEvents.h>
#include <rune/node/lease/LeaseParkTable.h>
#include <rune/node/lease/LeaseWheel.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/RecordBatch.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {
struct LeaseIndexRef;
struct LeaseWriter;
}

namespace rune::node::lease {

struct LeaseRuntimeStats {
    uint64_t expiriesAppended   = 0;   ///< Expire records appended
    uint64_t expiryBatches      = 0;   ///< successful appendLeaseBatch calls
    uint64_t expiryDeferred     = 0;   ///< due Armed entries skipped because lastAppended >= judgedThrough (per poll visit)
    uint64_t expiryBacklog      = 0;   ///< records of batches refused with kAppendMinIsr / kAppendBackPressure (per attempt)
    uint64_t notLeaderStops     = 0;   ///< batches refused with kAppendNotLeader
    uint64_t ownRecordRejected  = 0;   ///< BrokerRejected events that returned an in-flight entry to Armed
    uint64_t brokerRejectedSeen = 0;   ///< BrokerRejected events seen, whatever their entry
    uint64_t encodeFailures     = 0;   ///< encodeLeaseValue returned 0, or the batch was refused as invalid
    uint64_t grantNotFree       = 0;   ///< a Granted event found its entry not Free (a bug)
    uint64_t oversizedDeferred  = 0;   ///< a lone record over the hard batch cap was skipped (only that row; the others go on)
    uint64_t appendErrors       = 0;   ///< appendLeaseBatch threw (storage failure); handled as a refusal
    uint64_t clearsAppended     = 0;   ///< Checkpoint(0) records appended for parked keepalives (Task 11a)
    uint64_t clearBatches       = 0;   ///< successful appendLeaseBatch calls of flushClears
    uint64_t clearDeferred      = 0;   ///< clear-wanted rids pushed back (gate closed, byte cap, oversized, refused append)
    uint64_t rollDeferredSkips  = 0;   ///< duty cycles whose appends were skipped because the log's roll was deferred (back-pressure)
    uint64_t rejectedStale      = 0;   ///< BrokerRejected events of an in-flight entry whose row moved on (version differs / row gone): entry Free
    uint64_t ridPastMaxKeys     = 0;   ///< Granted events whose row id is past maxKeys (dropped)
};

/// The outcome of one keepalive entry: parked = no answer now (the park table holds it); else `code` (0 = Ok) and `ttlMs`
/// (Ok: the granted TTL; Throttled: the retry-after in ms).
struct LeaseKaOutcome {
    bool     parked;
    uint16_t code;
    uint64_t ttlMs;
};

/// One per Worker, created cold on the first lease-leader cycle and never freed: the batch under construction and its scratch.
struct LeaseEmitWork {
    static constexpr std::size_t kValueBytes = 4 + 65535 + 1;
    explicit LeaseEmitWork(std::size_t reserveBytes) : builder(0, 0) {
        builder.reserveBody(reserveBytes);
        batch.reserve(reserveBytes + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead));
    }
    rune::protocol::RecordBatchBuilder builder;
    std::vector<uint8_t>               batch;
    std::array<uint8_t, kValueBytes>   valueTmp{};
    std::array<uint32_t, rune::protocol::kMaxLeaseExpiryBatch> rids{};
};

class LeaseRuntime {
public:
    /// Cold.  `maxKeys` sizes the entry store (spec->lease->maxKeys); both wheel cursors start at `leaseNowNs`, which is also the
    /// runtime's ready time (promotionDeadline).  `extendNs`: the leader heartbeat timeout (a promoted key's grace, spec 9.7).
    /// `topicId` / `partition` name the partition on the park table's slots.
    LeaseRuntime(uint64_t leaderGen, uint64_t rebuildEpoch, uint32_t maxKeys, int64_t leaseNowNs, int32_t topicId = 0, int32_t partition = 0,
                 int64_t extendNs = 0)
        : gen_(leaderGen), epoch_(rebuildEpoch), timers_(maxKeys, leaseNowNs), readyNs_(leaseNowNs), extendNs_(extendNs),
          topicId_(topicId), partition_(partition) {}
    LeaseRuntime(const LeaseRuntime&)            = delete;
    LeaseRuntime& operator=(const LeaseRuntime&) = delete;

    [[nodiscard]] int32_t topicId() const noexcept { return topicId_; }
    [[nodiscard]] int32_t partition() const noexcept { return partition_; }
    [[nodiscard]] bool sameIdentity(uint64_t gen, uint64_t epoch) const noexcept { return gen_ == gen && epoch_ == epoch; }
    [[nodiscard]] uint64_t leaderGeneration() const noexcept { return gen_; }
    /// Whether a duty pass has anything to do at `leaseNowNs` besides folding events: a deadline or checkpoint wheel due, or a
    /// parked keepalive waiting for its clear.  (Task 12's promotion pass is not a wheel; it runs at runtime creation.)
    [[nodiscard]] bool hasPendingWork(int64_t leaseNowNs) const noexcept {
        return timers_.deadline().nextCheckNs() <= leaseNowNs || timers_.checkpoint().nextCheckNs() <= leaseNowNs || clearHead_ != clearTail_;
    }
    void noteRollDeferredSkip() noexcept { ++stats_.rollDeferredSkips; }
    /// The byte size of the batch the last successful emit / flushClears appended.
    [[nodiscard]] std::size_t lastBatchBytes() const noexcept { return lastBatchBytes_; }

    /// Folds every event of one duty cycle, in order (the judge's LeaseEvents).  Granted arms the entry; the others step the state
    /// machine.  Cold growth of the entry store happens here, at most once per call (growIfOwed).
    /// A fold action that answers parked keepalives (Clearing's own Checkpoint, an end of the key, a replay) does so through `parks`.
    /// `ref` (may be null in a bare unit test: a rejected record is then taken as the entry's own) is the lease index the rejected
    /// record's version is compared against.
    void onEvents(std::span<const LeaseEvent> events, int64_t leaseNowNs, LeaseParkTable& parks, const LeaseIndexRef* ref = nullptr) noexcept;

    /// The deadline of a key promoted at this runtime's ready time (spec 9.7): readyNs + extendNs + (remaining, else ttl).  Saturating.
    [[nodiscard]] int64_t promotionDeadline(uint64_t ttlMs, uint64_t remainingMs) const noexcept {
        return satAdd(readyNs_, satAdd(extendNs_, msToNsSat(remainingMs > 0 ? remainingMs : ttlMs)));
    }
    /// The deadline of `rid` as TimeToLive reports it: a Free entry (row present, not linked yet) has its promotion deadline.
    [[nodiscard]] int64_t deadlineFor(uint32_t rid, uint64_t ttlMs, uint64_t remainingMs) const noexcept {
        if (!timers_.covers(rid) || timers_.entry(rid).state == LeaseEntryState::Free) return promotionDeadline(ttlMs, remainingMs);
        return timers_.entry(rid).deadlineNs;
    }

    /// One keepalive entry (spec 9.3, 9.6).  See LeaseKaOutcome.  A Throttled answer may still have armed a Free entry at its promotion
    /// deadline (idempotent and never early: the deadline is the same one a later keepalive or the promotion pass would set).  `req` is the request's park-table index, kLeaseNil until its first park
    /// (this call opens it).  `owedRoom`: one more owed return fits the worker's cap; without it (or a slot) a park is Throttled with
    /// no state change.  Allocates nothing (the entry store grows cold, as Granted does).
    [[nodiscard]] LeaseKaOutcome keepAlive(const LeaseIndexRef& ref, std::span<const uint8_t> key, int64_t version, int64_t judgedThrough,
                                           int64_t leaseNowNs, LeaseParkTable& parks, uint32_t& req, int64_t sessionId, int64_t correlationId,
                                           uint16_t index, bool owedRoom);

    /// Appends one batch of Checkpoint(0) records for the keys whose parked keepalives wait to clear their remaining time (the clear-wanted
    /// ring), and answers the ones whose row already has remaining 0.  `byteCap` bounds the batch (namespace / intake limit); `hardCap`
    /// is the most a lone record may take (a record over `byteCap` but within `hardCap` goes out alone).  Returns the records appended.
    uint32_t flushClears(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough, int64_t leaseNowNs,
                         std::size_t byteCap, std::size_t hardCap, rune::replication::PartitionReplica& replica, LeaseParkTable& parks);
    [[nodiscard]] uint32_t clearPending() const noexcept { return static_cast<uint32_t>(clearTail_ - clearHead_); }
    /// Test seams (Task 11a review fixes): the clear ring's capacity (a power of two <= the array, ring empty), and a rid's clear-wanted flag.
    void setClearRingCapForTest(uint32_t cap) noexcept {
        assert(clearHead_ == clearTail_ && cap != 0 && (cap & (cap - 1)) == 0 && cap <= kClearRingCap);
        clearMask_ = cap - 1;
    }
    [[nodiscard]] bool clearFlagged(uint32_t rid) const noexcept { return (timers_.entry(rid).pad & kLeaseClearWanted) != 0; }

    /// One Expire batch: polls the deadline wheel for at most `maxRecords` due Armed entries whose key the judge has caught up with
    /// (lastAppended < judgedThrough), encodes each from its row at the latest installed writer version, and appends the batch with
    /// replica.appendLeaseBatch.  `byteCap` bounds the whole batch (header included).  Returns the records appended (0: none, or the
    /// append was refused: the entries stay Armed and are due again next cycle).  Never retries within the cycle.
    uint32_t emit(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough, int64_t leaseNowNs,
                  std::size_t byteCap, std::size_t hardCap, uint32_t maxRecords, rune::replication::PartitionReplica& replica);

    [[nodiscard]] const LeaseRuntimeStats& stats() const noexcept { return stats_; }
    [[nodiscard]] LeaseTimers& timersForTest() noexcept { return timers_; }
    [[nodiscard]] const LeaseTimers& timers() const noexcept { return timers_; }

private:
    struct KaStep {
        bool     park;       ///< false: answered (code, ttlMs)
        bool     wantFlag;   ///< a park that waits for the clear path (Armed, not due, remaining > 0)
        uint16_t code;
        uint64_t ttlMs;
    };
    /// The keepalive decision for an entry that is linked (Free is armed by the caller first): leaseStep, then the RearmAnswerOk side effect.
    KaStep kaStep_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, bool gateOpen) noexcept;
    /// Puts `rid` on the clear ring (idempotent by the flag).  A full ring is compacted first; false = still full (the caller must not park).
    [[nodiscard]] bool flagClear_(uint32_t rid) noexcept;
    /// Drops every ring entry whose flag is clear (a stale or duplicate one) or whose rid has no parks (its flag is cleared too),
    /// keeping the first of each rid.  O(ring), cold.
    void compactClears_() noexcept;
    /// Whether flagClear_(rid) would succeed (compacts a full ring).
    [[nodiscard]] bool clearRoom_(uint32_t rid) noexcept;
    /// Decides the chain of `rid` ONCE (the decision depends only on the entry's state and the row values): answered as a whole, or left parked.
    void replay_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, LeaseParkTable& parks) noexcept;
    void foldStep_(uint32_t rid, LeaseEntryEvent event, const LeaseEvent& ev, int64_t leaseNowNs, LeaseParkTable& parks) noexcept;

    static constexpr uint32_t kClearRingCap = 2 * kLeaseParkTableEntries;   ///< a power of two; the live entries are flagged rids with parks, <= the park table
    uint64_t         gen_, epoch_;
    LeaseTimers      timers_;
    LeaseRuntimeStats stats_;
    int64_t          readyNs_, extendNs_;
    int32_t          topicId_, partition_;
    uint64_t         clearHead_ = 0, clearTail_ = 0;
    std::size_t      lastBatchBytes_ = 0;
    bool             ridPastWarned_  = false;
    uint32_t         clearMask_ = kClearRingCap - 1;   ///< the ring's capacity - 1 (a test seam lowers it)
    std::array<uint32_t, kClearRingCap> clearRing_{};
};

}  // namespace rune::node::lease
