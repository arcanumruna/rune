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
