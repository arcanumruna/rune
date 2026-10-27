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
/// A view plan's source: the caller must hold READ on the id, full stop.  Unlike authorizeTopicId there is NO D22 pass-through:
/// the admin thread resolves the id later against a catalog that may be ahead of this thread's ObjectIndex, so a
/// not-yet-indexed id (a CREATE TOPIC in flight) must not pass for a DESCRIBE ON CLUSTER holder.
[[nodiscard]] std::optional<Denial> authorizeViewSource(const PermissionTable&, int32_t topicId);
/// Name-addressed: `op` on (type, name) - matched against the requested name whether or not the object exists.
[[nodiscard]] std::optional<Denial> authorizeName(const PermissionTable&, rune::security::ResourceType, std::string_view,
                                                  rune::security::Operation);
/// GetTopicSchema: a name may be a topic or a view.  Issue #128: a live name is checked against its own kind's grants
/// only (the ObjectIndex knows which); a name not live here is allowed when either type allows it (D22 parity: the
/// request's own "unknown" answer follows).  The text always says TOPIC.  `nsId` (namespaces Task 10): the name's
/// resolved namespace (NsResolve.h; kHiddenNsId never holds an object, so a hidden name is judged as a missing one).
[[nodiscard]] std::optional<Denial> authorizeTopicOrViewName(const PermissionTable&, std::string_view,
                                                             rune::security::Operation, const ObjectIndex&,
                                                             uint32_t nsId = 0);
[[nodiscard]] std::optional<Denial> authorizeCluster(const PermissionTable&, rune::security::Operation);
/// #211: `op` on at least one resource of `type` (no resource is named: a non-transactional InitProducerId needs
/// WRITE on some topic, Kafka's rule).  Detail: `not authorized: <OP> on any <TYPE>`.
[[nodiscard]] std::optional<Denial> authorizeAnyGrant(const PermissionTable&, rune::security::ResourceType,
                                                      rune::security::Operation);

} // namespace rune::gateway
