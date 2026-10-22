#pragma once
// TxnOffsetApply.h — #358: the outcome of applying one consumer-group offset of a COMMITTED transaction.
#include <cstdint>

namespace rune::coordination {

/// Applied: the offset record is committed (the offsets replica's high watermark passed it) - it survives a
/// failover of the offsets partition.  Replicating: appended, not committed yet - ask again (nothing is
/// re-appended).  Refused: not applied (not the group coordinator / offsets not loaded / lineage pending / the
/// append was refused / too many transactions waiting on the partition) - retry later.  A transaction completes
/// (CompleteCommit, EndTxn answered) only once every one of its offsets is Applied.
enum class TxnOffsetApply : uint8_t { Applied, Replicating, Refused };

/// Issue #359: why an apply was Refused (the group coordinator reports its last refusal; the transaction
/// coordinator logs it with the stuck-commit WARN).  Unknown: the sink gave no reason (rigs).
enum class TxnOffsetRefusal : uint8_t { Unknown, NotCoordinator, NotLoaded, LineagePending, AppendRefused, TooManyWaiters, NoOffsetSink };

[[nodiscard]] constexpr const char* toString(TxnOffsetRefusal r) noexcept {
    switch (r) {
        case TxnOffsetRefusal::NotCoordinator: return "NotCoordinator";
        case TxnOffsetRefusal::NotLoaded:      return "NotLoaded";
        case TxnOffsetRefusal::LineagePending: return "LineagePending";
        case TxnOffsetRefusal::AppendRefused:  return "AppendRefused";
        case TxnOffsetRefusal::TooManyWaiters: return "TooManyWaiters";
        case TxnOffsetRefusal::NoOffsetSink:   return "NoOffsetSink";   // #360
        case TxnOffsetRefusal::Unknown:        break;
    }
    return "Unknown";
}

/// Issue #438: whether an unfinished transaction holds a consumer-group (group, topic, partition).  A held key is
/// neither fetched nor plainly committed (UnstableOffsetCommit, retriable) until the holder's Complete* record is
/// committed.  Unknown: the transaction coordinator is not loaded on this node - treated as Held.
enum class TxnOffsetHold : uint8_t { Free, Held, Unknown };

/// PR #434 review: the committing transaction an offset apply belongs to - compared field by field.  The
/// coordinator fills all four (one value per transaction and coordinator load); a rig may name a transaction by a
/// plain number (`producerId` only).
struct TxnOffsetToken {
    int64_t producerId       = -1;
    int16_t producerEpoch    = -1;
    int64_t txnStartNs       = 0;
    int32_t coordinatorEpoch = -1;
    constexpr TxnOffsetToken() = default;
    constexpr TxnOffsetToken(int64_t pid, int16_t epoch, int64_t startNs, int32_t coordEpoch)
        : producerId(pid), producerEpoch(epoch), txnStartNs(startNs), coordinatorEpoch(coordEpoch) {}
    /// Test rigs: a transaction named by a number.
    constexpr TxnOffsetToken(uint64_t rigId) : producerId(static_cast<int64_t>(rigId)) {}   // NOLINT(implicit)
    constexpr bool operator==(const TxnOffsetToken&) const = default;
};

}  // namespace rune::coordination
