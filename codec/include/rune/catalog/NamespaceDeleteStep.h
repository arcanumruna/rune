#pragma once
// rune/catalog/NamespaceDeleteStep.h — the cleanup steps of a DELETING namespace (plan D18).  Each is recorded by one
// NamespaceDeleteStep (kind 36) event carrying exactly one bit; the namespace is erased once every bit is recorded
// and every topic and view it owned has been dropped.
#include <cstdint>

namespace rune::catalog {

enum NsDeleteStepBit : uint8_t {
    TxnsAborted        = 1,   // open transactions of the namespace aborted
    GroupsTombstoned   = 2,   // consumer groups of the namespace tombstoned
    TxnStateTombstoned = 4,   // transaction-coordinator state of the namespace tombstoned
    SecurityPurged     = 8,   // the namespace's grants and users purged from __security
};
inline constexpr uint8_t kNsDeleteAllSteps = 0x0F;

/// A step byte is valid when it is exactly one known bit.
[[nodiscard]] constexpr bool validDeleteStep(uint8_t s) noexcept {
    return s != 0 && (s & (s - 1)) == 0 && (s & ~kNsDeleteAllSteps) == 0;
}

} // namespace rune::catalog
