#include <rune/gateway/LiveSecurityChanges.h>

#include <rune/protocol/client/Protocol.h>

#include <algorithm>

namespace rune::gateway {

namespace {
using rune::security::Operation;
using rune::security::ResourceType;

/// true when `before` held a READ grant on a `type` resource that `after` no longer holds verbatim.  Conservative: a
/// grant swapped for a wider pattern also answers true - the executor then re-checks against the new table.
bool readGrantLost(const PermissionTable& before, const PermissionTable& after, ResourceType type) {
    const auto b = before.grants();
    const auto a = after.grants();
    for (const rune::security::GrantSpec& g : b) {
        if (g.op != Operation::Read || g.resource.type != type) continue;
        if (std::find(a.begin(), a.end(), g) == a.end()) return true;
    }
    return false;
}
/// A group READ grant lost: the executor re-checks every member's group.
bool groupReadGrantLost(const PermissionTable& before, const PermissionTable& after) {
    return readGrantLost(before, after, ResourceType::Group);
}
/// Issue #212: a topic READ grant lost - the executor re-checks every member's subscribed topics.
bool topicReadGrantLost(const PermissionTable& before, const PermissionTable& after) {
    return readGrantLost(before, after, ResourceType::Topic);
}
} // namespace

LiveActions refreshActions(const PermissionTable& before, const PermissionTable& after, const Session& s,
                           const ObjectIndex& index) {
    LiveActions out;
    const uint8_t read = rune::security::opBit(Operation::Read);
    for (const auto& [key, subId] : s.subIdByKey) {
        if (key.topicId < 0) continue;                  // admin-log reads are handled below
        if (index.at(key.topicId) == nullptr) continue; // a dropped object: the drop path ends it
        if (!after.allows(key.topicId, read))
            out.ends.push_back({key.correlationId, key.topicId, key.partition, subId,
                                static_cast<uint16_t>(rune::protocol::client::ErrorCode::NotAuthorized)});
    }
    std::sort(out.ends.begin(), out.ends.end(),
              [](const EndSubscription& l, const EndSubscription& r) { return l.subscriptionId < r.subscriptionId; });
    out.endCatalogReads  = !s.catalogReadCorrs.empty() && !after.allowsCluster(rune::security::opBit(Operation::Describe));
    out.endSecurityReads = !s.securityReadCorrs.empty() && !after.allowsCluster(rune::security::opBit(Operation::Admin));
    out.endGroups        = groupReadGrantLost(before, after);
    out.endGroupTopics   = topicReadGrantLost(before, after);
    return out;
}

} // namespace rune::gateway
