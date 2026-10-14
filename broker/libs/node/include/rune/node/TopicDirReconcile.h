#pragma once
// libs/node/include/rune/node/TopicDirReconcile.h
//
// Catalog history Task 16 (spec §3, §5.3): boot's topic-directory reconcile.
//
// `__catalog` is the single source of truth for which topics exist, so the topic
// directories under dataDir are reconciled against the BOOTED catalog — the live
// topics (and live view sinks) it names and the topic ids it dropped.  Nothing
// else decides: there are no topic-id markers, no placeholders and no recovery
// catalogs any more.
//
//   1. dataDir/dropped/<id>.<name> — a dropped topic's data, left there by a purge
//      the admin thread could not finish — is REMOVED, but only when <id> is a
//      topic id the catalog marks dropped.  An entry the catalog does not mark
//      dropped (a live topic's id, an unknown id, an unparseable name) is not ours
//      to delete: it is left in place and logged.
//   2. every dataDir/topics/<name> that no live topic claims — exactly or ignoring
//      ASCII letter case (two names that differ only in case share one directory
//      on a case-insensitive filesystem) — is QUARANTINED: moved, atomically, to
//      dataDir/quarantine/<name>-<nowUnixSeconds>[-N].  A topic dir is NEVER
//      deleted.  Symlinks are never followed; a directory that does not have the
//      layout of a topic dir (<digits>/<regular files>) is left alone.
//
// Idempotent: a crash anywhere re-runs to the same end state (a quarantine is one
// rename; a purge is a remove_all that a re-run finishes).
//
// The retired `dataDir/__topic_schemas` and `dataDir/__views` dirs are never read
// and never touched (no migration, spec §1); warnRetiredCatalogDirs logs them once
// per boot.
//
// A boot whose catalog replay HALTED, and a broker whose local catalog is empty
// while its dataDir holds topic dirs (the catalog was wiped or the disk is new),
// must NOT reconcile at boot: the catalog is not a trustworthy statement of what
// exists.  The caller skips the reconcile entirely (halted) or defers it until the
// applier has caught up to the catalog leader's committed prefix (empty).
#include <rune/node/NodeConfig.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::node {

struct TopicDirReconciliation {
    std::vector<std::filesystem::path> quarantined;   // destinations under dataDir/quarantine/
    std::vector<std::filesystem::path> purged;        // dataDir/dropped/ entries removed
};

/// `liveTopics`: every topic the catalog holds live (view sinks included); only
/// their names are read.  `droppedIds`: the topic ids the catalog dropped.
/// Never throws (every failure is logged and leaves the entry in place).
[[nodiscard]] TopicDirReconciliation
reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& liveTopics,
                   std::span<const int32_t> droppedIds, int64_t nowUnixSeconds);

/// Logs, once, a retired `__topic_schemas` / `__views` dir that holds data: it is
/// ignored (never read, never modified) — `__catalog` is the catalog.
void warnRetiredCatalogDirs(const std::string& dataDir);

/// INTERIM (until Task 17 removes RUNE_TOPICS): a static RUNE_TOPICS topic and a
/// catalog topic / view sink can never share a broker — the static topic would
/// open (or quarantine) the catalog topic's dir, or take its id: silent data loss.
/// Returns the boot refusal naming the first collision — by name (exact or an
/// ASCII case variant: one directory) first, then by topic id (a static topic's id
/// is its RUNE_TOPICS position) — or nullopt.  BrokerNode::start throws it.
[[nodiscard]] std::optional<std::string> staticCatalogCollision(const std::vector<TopicSpec>& statics,
                                                                const std::vector<TopicSpec>& catalogTopics);

} // namespace rune::node
