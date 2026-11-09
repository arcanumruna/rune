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
#include <rune/node/lease/LeaseWheel.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/RecordBatch.h>

#include <array>
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
    /// Cold.  `maxKeys` sizes the entry store (spec->lease->maxKeys); both wheel cursors start at `leaseNowNs`.
    LeaseRuntime(uint64_t leaderGen, uint64_t rebuildEpoch, uint32_t maxKeys, int64_t leaseNowNs)
        : gen_(leaderGen), epoch_(rebuildEpoch), timers_(maxKeys, leaseNowNs) {}
    LeaseRuntime(const LeaseRuntime&)            = delete;
    LeaseRuntime& operator=(const LeaseRuntime&) = delete;

    [[nodiscard]] bool sameIdentity(uint64_t gen, uint64_t epoch) const noexcept { return gen_ == gen && epoch_ == epoch; }

    /// Folds every event of one duty cycle, in order (the judge's LeaseEvents).  Granted arms the entry; the others step the state
    /// machine.  Cold growth of the entry store happens here, at most once per call (growIfOwed).
    void onEvents(std::span<const LeaseEvent> events, int64_t leaseNowNs) noexcept;

    /// One Expire batch: polls the deadline wheel for at most `maxRecords` due Armed entries whose key the judge has caught up with
    /// (lastAppended < judgedThrough), encodes each from its row at the latest installed writer version, and appends the batch with
    /// replica.appendLeaseBatch.  `byteCap` bounds the whole batch (header included).  Returns the records appended (0: none, or the
    /// append was refused: the entries stay Armed and are due again next cycle).  Never retries within the cycle.
    uint32_t emit(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough, int64_t leaseNowNs,
                  std::size_t byteCap, uint32_t maxRecords, rune::replication::PartitionReplica& replica) noexcept;

    [[nodiscard]] const LeaseRuntimeStats& stats() const noexcept { return stats_; }
    [[nodiscard]] LeaseTimers& timersForTest() noexcept { return timers_; }
    [[nodiscard]] const LeaseTimers& timers() const noexcept { return timers_; }

private:
    uint64_t         gen_, epoch_;
    LeaseTimers      timers_;
    LeaseRuntimeStats stats_;
};

}  // namespace rune::node::lease
