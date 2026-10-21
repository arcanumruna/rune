#pragma once
// TxnOffsetApply.h — #358: the outcome of applying one consumer-group offset of a COMMITTED transaction.
#include <cstdint>

namespace rune::coordination {

/// Applied: the offset record is committed (the offsets replica's high watermark passed it) - it survives a
/// failover of the offsets partition.  Replicating: appended, not committed yet - ask again (nothing is
/// re-appended).  Refused: not applied (not the group coordinator / offsets not loaded / lineage pending / the
/// append was refused) - retry later.  A transaction completes (CompleteCommit, EndTxn answered) only once every
/// one of its offsets is Applied.
enum class TxnOffsetApply : uint8_t { Applied, Replicating, Refused };

}  // namespace rune::coordination
