#pragma once
// rune/security/SecurityQueries.h — history / as-of / grants-for over a recorded __security log
// (spec §3: SHOW SECURITY HISTORY, SHOW USERS|ROLES|GRANTS).  Pure functions of the log or a
// state; every state they produce is replayed through the verifying SecurityReplayer, so a query
// never shows a state the log does not verifiably produce.  Rows NEVER carry verifier bytes.
#include <rune/security/SecurityReplayer.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::security {

/// One event of a recorded log, at its __security offset.
struct RecordedEvent { int64_t offset = -1; SecurityEvent event; };

/// kind: User = events about a user (CreateUser, AlterUser, DropUser, Bootstrap, GrantRole, RevokeRole,
///       and Grant / Revoke whose grantee is a user); Role = CreateRole, DropRole and Grant / Revoke whose
///       grantee is a role; nullopt = every event.
/// name: matches objectName (the user or role the event is about — every object EVER named so);
///       empty = every name.
/// sinceMs: timestampMs >= sinceMs.
struct HistoryFilter { std::optional<GranteeKind> kind; std::string name; std::optional<int64_t> sinceMs; };
/// The matching events, in log (offset) order.
[[nodiscard]] std::vector<RecordedEvent> history(std::span<const RecordedEvent> log, const HistoryFilter&);

/// Offset o: the state after every event at offset <= o (inclusive).
/// Time t: events with timestampMs <= t (inclusive), in offset order, STOPPING at the first later one
///         (time is informational; the offset is the order).
struct AsOf { enum class By : uint8_t { Offset, Time } by = By::Offset; int64_t value = 0; };
/// Replays `log` (which must start at offset 0, gap-free) through a fresh SecurityReplayer up to the
/// as-of point — an as-of point past a bad event surfaces that event's error.
[[nodiscard]] std::expected<SecurityState, ReplayError> stateAsOf(std::span<const RecordedEvent> log, const AsOf&);

/// A user's (or role's) grants: the direct ones, those it holds through each role (by role name, sorted),
/// and the effective union.  A role has only direct (= effective) grants.
struct GrantsView { std::vector<GrantSpec> direct; std::vector<std::pair<std::string, std::vector<GrantSpec>>> viaRoles;
                    std::vector<GrantSpec> effective; };
/// nullopt = no such live user / role.
[[nodiscard]] std::optional<GrantsView> grantsFor(const SecurityState&, GranteeKind, std::string_view name);

/// SHOW USERS / SHOW ROLES rows, ordered by name.  `passwords` counts the active verifiers; their
/// bytes are never exposed.
struct UserRow { std::string name; bool enabled; bool mustChangePassword; uint8_t passwords; std::vector<std::string> roles; uint32_t version; };
struct RoleRow { std::string name; std::vector<std::string> members; std::size_t grants; uint32_t version; };
[[nodiscard]] std::vector<UserRow> users(const SecurityState&);
[[nodiscard]] std::vector<RoleRow> roles(const SecurityState&);

} // namespace rune::security
