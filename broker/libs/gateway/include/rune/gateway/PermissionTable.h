#pragma once
// PermissionTable.h - what an authenticated session may do (authentication plan D30; spec §2).
//
// Task 11 ships ONLY the allow-all table and the two query shapes the gateway's gate and the handlers will call:
// allows(op, type, objectId) and allowsCluster(op).  Task 12 completes it (compile from the SecurityState: one op
// bitmask per topic / view id, cluster bits, group / transactional-id patterns, the write bits Task 13 ships to the
// workers).  Until then a login grants allow-all - Checkpoint C is after Task 13, so this is never shipped.
//
// Control-thread owned, per session (hard rule 1).  The check is a pure function of the table: no allocation.

#include <rune/security/SecurityEvent.h>

#include <cstdint>

namespace rune::gateway {

class PermissionTable {
public:
    using Operation    = rune::security::Operation;
    using ResourceType = rune::security::ResourceType;

    /// Every operation on every object (dev / in-process anonymous sessions, and - until Task 12 - every login).
    [[nodiscard]] static PermissionTable allowAll() noexcept { return PermissionTable{}; }

    [[nodiscard]] bool allows(Operation, ResourceType, int32_t /*objectId*/) const noexcept { return true; }
    [[nodiscard]] bool allowsCluster(Operation) const noexcept { return true; }
};

} // namespace rune::gateway
