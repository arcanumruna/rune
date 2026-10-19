#pragma once
// Authorizer.h - the spec §2.1 table, in ONE place (authentication Task 12).  Every gateway handler asks it, and it is
// the only place a NotAuthorized code or detail text is chosen (D23): `not authorized: <OP> on <TYPE> <requested name
// or id>`, built ONLY from the request, so a hidden object and a missing one answer identically.
//
// Pure functions of (table, request): no allocation on the granted path (the Denial is built only on a refusal).
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace rune::gateway {

struct Denial {
    uint16_t code = 45;   // ErrorCode::NotAuthorized
    std::string detail;   // D23
};

/// Id-addressed (Produce / Subscribe / group topics / txn partitions / views): `op` on the object `topicId`.  D22: a
/// non-live id is NotAuthorized unless the session holds DESCRIBE ON CLUSTER (then no denial: the request's existing
/// "unknown topic" answer follows).  `shownType` is the type named in the detail text (the request's, never the
/// object's: the text must not reveal what the id is).
[[nodiscard]] std::optional<Denial> authorizeTopicId(const PermissionTable&, int32_t topicId, rune::security::Operation,
                                                     const ObjectIndex&,
                                                     rune::security::ResourceType shownType = rune::security::ResourceType::Topic);
/// Name-addressed: `op` on (type, name) - matched against the requested name whether or not the object exists.
[[nodiscard]] std::optional<Denial> authorizeName(const PermissionTable&, rune::security::ResourceType, std::string_view,
                                                  rune::security::Operation);
/// GetTopicSchema: a name may be a topic or a view - allowed when either type allows it; the text always says TOPIC.
[[nodiscard]] std::optional<Denial> authorizeTopicOrViewName(const PermissionTable&, std::string_view,
                                                             rune::security::Operation);
[[nodiscard]] std::optional<Denial> authorizeCluster(const PermissionTable&, rune::security::Operation);

} // namespace rune::gateway
