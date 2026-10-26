#pragma once
// rune/security/Permissions.h — effective permissions (auth spec §2): allow-only, the union of a
// user's direct grants and its roles' grants; exact / prefix pattern matching; D15 (any granted
// operation implies DESCRIBE), D16 (CLUSTER grants), D17 (valid pairs, in validGrant).
// Namespaces (plan D9, D10): a grant's pattern names objects of the grantee's OWN namespace only, so an
// EffectivePerms carries the namespace and opsOn(perms, …) gives 0 on an object of any other namespace.
#include <rune/security/SecurityState.h>

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace rune::security {

[[nodiscard]] bool matches(const ResourcePattern& p, ResourceType type, std::string_view name) noexcept;
[[nodiscard]] std::vector<GrantSpec> effectiveGrants(const SecurityState&, uint64_t userId);   // direct ∪ roles, sorted, unique
/// The op bits `grants` give on object (type, name): exact / prefix matches, CLUSTER grants (D16),
/// and the implied DESCRIBE (D15).  Pure; the broker compiles tables with it, the model checks with its own.
[[nodiscard]] uint8_t opsOn(std::span<const GrantSpec> grants, ResourceType type, std::string_view name) noexcept;
[[nodiscard]] uint8_t clusterOps(std::span<const GrantSpec> grants) noexcept;

/// A user's effective permissions, scoped to its namespace.
struct EffectivePerms {
    uint32_t               nsId    = 0;       // the user's namespace
    bool                   nsAdmin = false;   // ADMIN on NAMESPACE "<own namespace name>"
    std::vector<GrantSpec> grants;            // effectiveGrants(): direct ∪ roles, sorted, unique
};
/// `nsName`: the name of the user's namespace (the codec has no catalog; the caller supplies it).  An unknown user
/// yields nsAdmin false and no grants.
[[nodiscard]] EffectivePerms effectivePerms(const SecurityState&, uint64_t userId, std::string_view nsName);
/// The op bits `perms` give on object (type, name) of namespace `objectNsId`: opsOn() over the grants if the object is
/// in the grantee's own namespace, 0 otherwise.  A foreign object has no permission bits (plan D9).
[[nodiscard]] uint8_t opsOn(const EffectivePerms& perms, ResourceType type, uint32_t objectNsId, std::string_view name) noexcept;

} // namespace rune::security
