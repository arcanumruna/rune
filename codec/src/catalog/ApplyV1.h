#pragma once
// src/catalog/ApplyV1.h — the FROZEN formatVer-1 apply rules (spec §6).  A future
// formatVer adds ApplyV2 beside this and a dispatch arm in apply(); this file and
// ApplyV1.cpp are never edited again once golden logs pin them (Task 3).
#include <rune/catalog/CatalogState.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <variant>

namespace rune::catalog {

/// The object a validated put installs: exactly one of a topic or a view.
using StagedObject = std::variant<TopicObject, ViewObject>;
/// A validated, not-yet-committed change — one state per family, each holding only
/// what committing it needs.
struct StagedCreate { StagedObject object; };                                  // CreateTopic, CreateView
struct StagedUpdate { StagedObject object; int32_t retiredSinkTopicId = -1; };  // the rest of the puts; AlterView: the old sink
struct StagedRemove { uint64_t objectId; Tombstone tomb; int32_t droppedTopicId; };   // DropTopic, DropView
using Staged = std::variant<StagedCreate, StagedUpdate, StagedRemove>;

/// The live topic `e` names (objectId, namespace, name and version chain agree), or the Precondition.  The later
/// formatVers' object rules reuse it; it is the helper ApplyV1's own table runs.
[[nodiscard]] std::expected<const TopicObject*, ApplyError> liveTopicOf(const CatalogState& s, const CatalogEvent& e,
                                                                        int64_t offset);
/// Precondition if `t` is untyped or its schema version is exhausted; nullopt = fine.
[[nodiscard]] std::optional<ApplyError> schemaVersionErrorOf(const CatalogEvent& e, int64_t offset, const TopicObject& t);

struct ApplyV1 {
    /// Check every rule of the table against `s` and build the change.  Never mutates `s`.
    static std::expected<Staged, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    /// The fingerprint of the object (or tombstone) the change produces.
    static uint64_t fingerprintOfStaged(const Staged& st) noexcept;
    /// Install a change `run` produced on this very state.  `fingerprint` = fingerprintOfStaged(st).
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, Staged&& st);
};

} // namespace rune::catalog
