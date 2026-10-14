#pragma once
// libs/node/include/rune/node/RuntimeTopicRecovery.h
//
// Boot-time recovery of RUNTIME-defined topics.
//
// A topic created at runtime — by DefineTopic, or as a CreateView sink — exists
// as a runtime replica only because the admin→control createTopic path built it
// (TopicRegistry::createTopic + BrokerNode::createRuntimeUserReplicas).  Its log
// survives a restart, but nothing at boot re-creates the topic: boot builds
// ONLY config_.topics.  The durable record of each runtime topic lives in the
// admin catalogs instead:
//   - __topic_schemas : one TopicSchema per (topicId, version) — DefineTopic
//   - __views         : one ViewRecord per view — the view's sink topic
//
// (Catalog history Task 9: the RECOVERY from those catalogs is retired — no
// production code reads them; it lives on test-only in tests/support/
// LegacyRuntimeTopicRecovery.h.  What remains here is boot's topic-dir
// reconcile, the topic-id marker helpers and the interim catalog claim scan.)
// recoverRuntimeTopicSpecs() read both catalogs straight off disk (BEFORE the
// TopicRegistry is built, on the single startup thread) and synthesizes exactly
// the TopicSpec each original createTopic call used, so the caller can append
// them to config_.topics and every existing boot path (TopicRegistry,
// buildReplication, worker placement, MetadataService, read-committed rebuild)
// recreates them like static topics.  A transient topic (TopicSchema
// persistence trailer) is recreated transient: metadata survives, data does not.
//
// Bound: each catalog is read to its recovered LOG END.  The admin catalogs are
// single-owner, and every DDL record is HW-gated before the client is acked, so
// the on-disk tail is committed (Phase 1 is single-node; multi-node catalog
// replication is the deferred §11 boundary).
#include <rune/node/NodeConfig.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace rune::node {

/// Final review M1: dataDir/<this> latches catalog damage across boots (see
/// reconcileTopicDirs).  A regular file; its content is the damage reason.
inline constexpr const char* kCatalogDamagedSentinel = ".catalog-damaged";

/// The two-phase topic-id marker in dataDir/topics/<name>/ (B10 / D4) — written by
/// the retired runtime create path; boot's reconcileTopicDirs still reads (and
/// heals) markers that older builds left.  Moved here from TopicRegistry with its
/// last writer (catalog history Task 9); Task 16 retires it with this module.
/// One line, written atomically (temp file + rename):
///   "pending <id>\n"   — a runtime create made this dir; its record never confirmed;
///   "committed <id>\n" — its catalog record committed.
inline constexpr const char*      kTopicIdMarkerFile      = "topic-id";
inline constexpr const char*      kTopicIdMarkerTmpFile   = "topic-id.tmp";
inline constexpr std::string_view kTopicIdMarkerPending   = "pending";
inline constexpr std::string_view kTopicIdMarkerCommitted = "committed";

/// Atomically (kTopicIdMarkerTmpFile + rename) writes "<state> <id>\n" as
/// topicDir/kTopicIdMarkerFile.  Never follows a symlink: refuses when topicDir or
/// the marker path is one (or not a dir / not a regular file); the temp is created
/// O_CREAT|O_EXCL|O_NOFOLLOW.  false (logged, temp removed) on failure.
bool writeTopicIdMarker(const std::string& topicDir, std::string_view state, int32_t id);
/// Removes topicDir/topic-id and topic-id.tmp (unlink only).  Refuses (false) when
/// topicDir is a symlink or not a directory.  true iff a marker entry was removed.
bool clearTopicIdMarker(const std::string& topicDir);

/// INTERIM (catalog history Task 9, until Task 16's reconcile against the booted
/// catalog): the topics and view sinks LIVE in the local `dataDir/__catalog` at its
/// applied checkpoint — the topic dirs boot's reconcile must claim (a dropped
/// topic's dir is NOT claimed: it is quarantined, so a reused name starts empty).
/// A read-only replay into a scratch state (no effects, nothing else touched); a
/// replay error stops it at that event (the applier halts on the same one).  No
/// checkpoint = nothing applied = {}.  Throws on an unreadable checkpoint (boot
/// refuses, as buildCatalog_ would).
[[nodiscard]] std::vector<TopicSpec> catalogLiveTopicSpecs(const std::string& dataDir);

/// "__orphan.<topicId>" — the recovery placeholder name for a catalog-less id.
[[nodiscard]] std::string orphanPlaceholderName(int32_t topicId);

struct TopicDirReconciliation {
    std::vector<std::filesystem::path> deleted;       // orphan-placeholder dirs (canonical paths)
    std::vector<std::filesystem::path> quarantined;   // destinations under dataDir/quarantine/
    std::vector<std::filesystem::path> promoted;      // claimed dirs whose marker went pending -> committed
    std::vector<std::filesystem::path> markersCleared; // claimed dirs whose stale (other-id) marker was removed
};

/// B10 / user decision D4 (fix round 1): reconcile dataDir/topics/ against the
/// full catalog (`topics` = static + recovered, incl. placeholders) and the
/// placeholder ids RECOVERY created (`placeholderIds`, RecoveredRuntimeTopics —
/// structural, never a name match).  For each direct child of
/// canonical(dataDir/topics) that no topic claims (exactly or ignoring ASCII case)
/// and that has the recognised layout
/// <name>/[topic-id][topic-id.tmp] + <digits>/<regular files>:
///   * its marker is exactly "pending <id>", <id> is in placeholderIds, no
///     topic-id.tmp is present, and every <partition>/*.log is 0 bytes (final
///     review C1: an uncommitted topic is unwritable, so a never-committed orphan
///     holds nothing) — a runtime create whose catalog record never committed ->
///     DELETED (a failed delete quarantines what remains);
///   * otherwise -> MOVED to dataDir/quarantine/<name>-<nowUnixSeconds>[-N], never
///     deleted.  That covers no marker (static / older builds), a "committed"
///     marker (its record committed, so an unclaimed one means CATALOG DAMAGE —
///     a CRC-truncated or missing catalog; logged at error), any other id, and
///     an unreadable or malformed marker.
/// Fix round 2b: a non-empty `catalogDamage` (RecoveredRuntimeTopics) disables
/// EVERY delete this boot — placeholder dirs are quarantined, with one warn line.
/// Final review M1: the damage is latched in dataDir/kCatalogDamagedSentinel, which
/// keeps disabling deletes on later boots (whose recovery reads the truncated or
/// recreated catalog as intact) until a reconcile leaves no unclaimed topic dir in
/// place; then it is removed (or remove it by hand).
/// FIRST (fix round 1b), before any delete decision: every catalog-claimed runtime
/// topic dir (exact name, non-placeholder) whose marker reads "pending <its id>" is
/// rewritten "committed <id>" (atomic) — a crash between the record's commit and
/// the control thread's marker rewrite heals at the next boot.  A claimed dir whose
/// marker names ANOTHER id (stale, from a reused dir) has the marker removed.
/// Symlinks are never followed (a symlinked topics/ or quarantine/ root, child,
/// partition dir or file makes the entry "not ours": left alone).  Each action is
/// logged at warn with its path(s).  Never throws.  Call on the startup thread
/// after the recovered specs are appended and BEFORE the TopicRegistry opens logs.
[[nodiscard]] TopicDirReconciliation
reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& topics,
                   const std::vector<int32_t>& placeholderIds, int64_t nowUnixSeconds,
                   std::string_view catalogDamage = {});

} // namespace rune::node
