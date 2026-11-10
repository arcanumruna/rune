#pragma once
// StatsCollect.h - checked writes Part 08: the counters GetStats serves, written into worker-owned fixed storage.  An id is
// present iff its counter exists on the partition (C16).  Later parts append their ids to the two collectors.
#include <rune/node/AutoInc.h>
#include <rune/node/VerdictSource.h>
#include <rune/node/ViewInstance.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/client/StatsMessages.h>
#include <rune/replication/PartitionReplica.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::node {

/// Stat entries {id u16, value i64} and constraint entries {id u32, rejections u64} written back to back into the caller's
/// storage.  Ids are added in ascending order.
class StatsSink {
public:
    StatsSink(std::span<uint8_t> statBytes, std::span<uint8_t> constraintBytes) noexcept
        : stats_(statBytes), constraints_(constraintBytes) {}

    /// false = full.
    bool add(rune::protocol::client::StatId id, int64_t value) noexcept {
        using R = rune::protocol::client::StatsResponse;
        if ((count_ + 1u) * R::kStatBytes > stats_.size()) return false;
        uint8_t* at = stats_.data() + count_ * R::kStatBytes;
        rune::protocol::client::detail::putLE16(at, 0, static_cast<uint16_t>(id));
        rune::protocol::client::detail::putLE64(at, 2, value);
        ++count_;
        return true;
    }
    /// false = full.
    bool addConstraint(uint32_t constraintId, uint64_t rejections) noexcept {
        using R = rune::protocol::client::StatsResponse;
        if ((constraintCount_ + 1u) * R::kConstraintBytes > constraints_.size()) return false;
        uint8_t* at = constraints_.data() + constraintCount_ * R::kConstraintBytes;
        rune::protocol::client::detail::putLE32(at, 0, static_cast<int32_t>(constraintId));
        rune::protocol::client::detail::putLE64(at + 4, 0, static_cast<int64_t>(rejections));
        ++constraintCount_;
        return true;
    }

    [[nodiscard]] uint16_t count() const noexcept { return count_; }
    [[nodiscard]] uint16_t constraintCount() const noexcept { return constraintCount_; }
    [[nodiscard]] std::span<const uint8_t> statBytes() const noexcept {
        return stats_.first(count_ * rune::protocol::client::StatsResponse::kStatBytes);
    }
    [[nodiscard]] std::span<const uint8_t> constraintBytes() const noexcept {
        return constraints_.first(constraintCount_ * rune::protocol::client::StatsResponse::kConstraintBytes);
    }

private:
    std::span<uint8_t> stats_;
    std::span<uint8_t> constraints_;
    uint16_t           count_           = 0;
    uint16_t           constraintCount_ = 0;
};

/// Ids 1-20 of one led topic partition; `state` is nullptr for an untyped topic.
inline void collectTopicPartitionStats(const PartitionWriteState* state, const rune::replication::PartitionReplica& replica,
                                       StatsSink& sink) noexcept {
    using rune::protocol::client::StatId;
    if (state == nullptr) return;
    if (const VerdictSource* const judge = state->verdicts; judge != nullptr) {
        const VerdictSourceStats& s = judge->stats();
        sink.add(StatId::rejectionsKeyExists, static_cast<int64_t>(s.rejectionsKeyExists));
        sink.add(StatId::rejectionsKeyNotFound, static_cast<int64_t>(s.rejectionsKeyNotFound));
        sink.add(StatId::rejectionsConstraint, static_cast<int64_t>(s.rejectionsConstraint));
        sink.add(StatId::verdictLag, replica.logEndOffset() - judge->judgedThrough());
        sink.add(StatId::verdictWatermark, state->rejected.watermark());
        sink.add(StatId::verdictMarkersWritten, static_cast<int64_t>(s.markersWritten));
        sink.add(StatId::rejectedNotCompacted, static_cast<int64_t>(state->rejected.countAtOrAfter(replica.lastCleanOffset())));
        for (uint16_t i = 0; i < s.perConstraintCount; ++i) sink.addConstraint(s.perConstraint[i].constraintId, s.perConstraint[i].rejections);
    }
    if (state->spec != nullptr && state->spec->autoIncrement) {
        const AutoIncState& a = state->autoInc;
        if (a.ready()) {
            const uint64_t reserved = a.reservedThroughN();
            sink.add(StatId::autoIncrementCounter, static_cast<int64_t>(autoIncId(a.nextN(), a.partitions(), a.partition())));
            sink.add(StatId::autoIncrementReservedThrough, reserved == 0 ? 0 : static_cast<int64_t>(autoIncId(reserved, a.partitions(), a.partition())));
        }
        sink.add(StatId::autoIncrementIdsBurned, static_cast<int64_t>(state->autoIncStats.idsBurned));
    }
    if (state->isVerdictSource()) {   // ids 11-20 (Part 11): the source's transaction votes
        const TxnJudgeStats& t = state->txn.stats;
        sink.add(StatId::txnVotesYes, static_cast<int64_t>(t.votesYes));
        sink.add(StatId::txnVotesNo, static_cast<int64_t>(t.votesNo));
        sink.add(StatId::txnVotesNoConstraintViolation, static_cast<int64_t>(t.votesNoConstraint));
        sink.add(StatId::txnVotesNoKeyExists, static_cast<int64_t>(t.votesNoKeyExists));
        sink.add(StatId::txnVotesNoKeyNotFound, static_cast<int64_t>(t.votesNoKeyNotFound));
        sink.add(StatId::txnVotesNoTxnTooLarge, static_cast<int64_t>(t.votesNoTooLarge));
        sink.add(StatId::txnVoteLatencyNsMax, static_cast<int64_t>(t.voteLatencyNsMax));
        sink.add(StatId::txnVoteLatencyNsTotal, static_cast<int64_t>(t.voteLatencyNsTotal));
        sink.add(StatId::foldPausedOnTxnLockNs, static_cast<int64_t>(t.foldPausedOnTxnLockNs));
        sink.add(StatId::txnMarkerWithoutVote, static_cast<int64_t>(t.markerWithoutVote));
    }
}

/// Ids 32-36 of one led view partition; 32 only for a unique view.
inline void collectViewPartitionStats(const ViewFoldCounters& counters, StatsSink& sink, bool uniqueView = false) noexcept {
    using rune::protocol::client::StatId;
    if (uniqueView) sink.add(StatId::uniqueConflicts, static_cast<int64_t>(counters.uniqueConflicts));
    sink.add(StatId::scanRows, static_cast<int64_t>(counters.scanRows));
    sink.add(StatId::scanPages, static_cast<int64_t>(counters.scanPages));
    sink.add(StatId::transientRecordsEvicted, static_cast<int64_t>(counters.transientRecordsEvicted));
    sink.add(StatId::transientSourceResets, static_cast<int64_t>(counters.transientSourceResets));
}

}  // namespace rune::node
