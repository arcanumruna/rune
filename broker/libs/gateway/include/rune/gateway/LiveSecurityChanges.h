#pragma once
// LiveSecurityChanges.h - what a permission refresh does to ONE session (authentication Task 14, spec §6 "Live changes",
// D25).  Pure: the old and the new table, the session's live subscriptions and the object index in, the list of things
// to end out.  The gateway executes it (Unsubscribe to the owning worker + ErrorResponse{NotAuthorized} on the
// subscription's correlation id; the admin-log reads; the group members) - nothing silent.
//
// A subscription survives a refresh that keeps its READ; a GAINED grant ends nothing.  Control thread, cold.
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/SessionTable.h>

#include <cstdint>
#include <vector>

namespace rune::gateway {

struct EndSubscription {
    int64_t  correlationId;
    int32_t  topicId;
    int32_t  partition;
    int64_t  subscriptionId;
    uint16_t code;   ///< 45 NotAuthorized
};

struct LiveActions {
    std::vector<EndSubscription> ends;          ///< sorted by subscription id
    bool endCatalogReads  = false;              ///< lost DESCRIBE ON CLUSTER while holding `__catalog` reads
    bool endSecurityReads = false;              ///< lost ADMIN ON CLUSTER while holding `__security` reads
    bool endGroups        = false;              ///< lost a group READ grant: the gateway removes the members whose group is no longer readable
    [[nodiscard]] bool empty() const noexcept { return ends.empty() && !endCatalogReads && !endSecurityReads && !endGroups; }
};

/// What a refresh does to one session: every live subscription whose object lost READ (-6: DESCRIBE CLUSTER, -7: ADMIN
/// CLUSTER, a group member: the group's READ) ends NotAuthorized.  A subscription of a topic that no longer exists is
/// left to the drop path.  Pure; the gateway executes it.
[[nodiscard]] LiveActions refreshActions(const PermissionTable& before, const PermissionTable& after, const Session& s,
                                         const ObjectIndex&);

} // namespace rune::gateway
