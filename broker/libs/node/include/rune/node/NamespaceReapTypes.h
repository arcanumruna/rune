#pragma once
// NamespaceReapTypes.h - namespaces Phase 2 Task 7: the vocabulary between the namespace reaper (admin thread) and the
// control thread, which owns the offset / transaction-state stores and the transaction coordinator.
#include <cstdint>

namespace rune::node {

/// What the reaper asks the control thread: AbortTxns = abort every open transaction of the namespace; TombstoneGroups =
/// one batch of consumer-offset tombstones; TombstoneTxnState = one batch of transaction-state tombstones.
enum class ReapOp : uint8_t { AbortTxns = 1, TombstoneGroups = 2, TombstoneTxnState = 3 };

/// The answer: Done = the step is complete (a tombstone call that found nothing left to write, all committed); More = a
/// batch was written, ask again; Refused = this node cannot do it now (not the coordinator / not loaded / below minIsr /
/// a transaction still ending) - ask again, nothing was skipped.
enum class ReapStatus : uint8_t { Done = 0, More = 1, Refused = 2 };

}  // namespace rune::node
