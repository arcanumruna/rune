#pragma once
// rune/security/SecurityStatementText.h — the canonical, password-free text of a security statement, rendered by the
// LEADER from the op itself (issue #228): the __security audit log never records the client's free-text statement.
// Byte-for-byte the text rune::ql::redactedText produces for the same statement.
#include <cstdint>
#include <string>
#include <string_view>

namespace rune::security {

/// `kind` is rune::protocol::client::SecurityOpKind, `change` UserChangeKind; granteeKind / op / resourceType /
/// patternKind are the frozen rune::protocol::Security* enum bytes.  Returns "" for an unknown kind, change or enum.
[[nodiscard]] std::string securityStatementText(uint8_t kind, std::string_view name, uint8_t change,
                                                uint8_t passwordIndex, uint8_t terminateSessions, uint8_t force,
                                                uint8_t granteeKind, uint8_t op, uint8_t resourceType,
                                                uint8_t patternKind, std::string_view pattern,
                                                std::string_view roleName);

} // namespace rune::security
