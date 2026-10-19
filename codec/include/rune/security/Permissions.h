#pragma once
// rune/security/Permissions.h — effective permissions (auth spec §2): allow-only, the union of a
// user's direct grants and its roles' grants; exact / prefix pattern matching; D15 (any granted
// operation implies DESCRIBE), D16 (CLUSTER grants), D17 (valid pairs, in validGrant).
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

} // namespace rune::security
