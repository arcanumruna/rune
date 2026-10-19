#pragma once
// src/security/ApplyV1.h — the FROZEN formatVer-1 apply rules (auth spec §3, plan Task 3).  A
// future formatVer adds ApplyV2 beside this and a dispatch arm in apply(); this file and
// ApplyV1.cpp are never edited again once golden logs pin them (Task 4).
#include <rune/security/SecurityState.h>

#include <cstdint>
#include <expected>
#include <variant>
#include <vector>

namespace rune::security {

/// A validated, not-yet-committed change — one state per family, each holding only what committing needs.
struct StagedPutUser    { UserObject obj; bool created = false; };
struct StagedPutRole    { RoleObject obj; bool created = false; };
struct StagedRemoveUser { Tombstone tomb; };
struct StagedRemoveRole { Tombstone tomb; std::vector<uint64_t> members; };   // members lose the role
using StagedChange = std::variant<StagedPutUser, StagedPutRole, StagedRemoveUser, StagedRemoveRole>;
struct Staged { StagedChange change; Applied applied; };   // applied.fingerprint = the resulting object's

struct ApplyV1 {
    /// Check every rule of the table against `s` and build the change.  Never mutates `s`.
    static std::expected<Staged, ApplyError> run(const SecurityState& s, const SecurityEvent& e, int64_t offset);
    /// Install a change `run` produced on this very state.
    static Applied commit(SecurityState& s, const SecurityEvent& e, int64_t offset, Staged&& st);
};

} // namespace rune::security
