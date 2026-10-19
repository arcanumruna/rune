#pragma once
// SecurityTypes.h — the FROZEN security enums, defined once (review A, I5).  They are bytes of the
// SecurityDdl wire (Grant / Revoke: granteeKind, op, resourceType, patternKind) AND of the __security
// event encoding (rune-codec rune::security aliases these) AND of the rune-ql security plans (rune-ql
// aliases these).  Values are part of the on-disk and on-wire formats: never renumber.
#include <cstdint>

namespace rune::protocol {

enum class SecurityOperation    : uint8_t { Read = 0, Write = 1, Create = 2, Alter = 3, Drop = 4, Describe = 5, Admin = 6 };
enum class SecurityResourceType : uint8_t { Cluster = 0, Topic = 1, View = 2, Group = 3, TransactionalId = 4 };
enum class SecurityPatternKind  : uint8_t { Exact = 0, Prefix = 1 };
enum class SecurityGranteeKind  : uint8_t { User = 0, Role = 1 };

[[nodiscard]] constexpr bool validSecurityOperation(uint8_t v) noexcept    { return v <= static_cast<uint8_t>(SecurityOperation::Admin); }
[[nodiscard]] constexpr bool validSecurityResourceType(uint8_t v) noexcept { return v <= static_cast<uint8_t>(SecurityResourceType::TransactionalId); }
[[nodiscard]] constexpr bool validSecurityPatternKind(uint8_t v) noexcept  { return v <= static_cast<uint8_t>(SecurityPatternKind::Prefix); }
[[nodiscard]] constexpr bool validSecurityGranteeKind(uint8_t v) noexcept  { return v <= static_cast<uint8_t>(SecurityGranteeKind::Role); }

} // namespace rune::protocol
