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
// recoverRuntimeTopicSpecs() reads both catalogs straight off disk (BEFORE the
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

/// Returns the TopicSpecs of every runtime topic recorded in
/// `<dataDir>/__topic_schemas` and `<dataDir>/__views`, sorted by topicId.
///
/// Ids below `firstRuntimeId` (the number of static config topics) belong to
/// static topics and are skipped.  TopicRegistry assigns ids sequentially, so
/// the recovered ids must be exactly firstRuntimeId, firstRuntimeId+1, ... —
/// a hole would recreate later topics under the wrong ids, so it is filled (below).
///
/// Missing catalog directories mean "nothing to recover" and are not created.
///
/// An id with no catalog record (a DefineTopic/CreateView whose record never
/// committed — an orphan) is recreated as the inert, hidden placeholder
/// orphanPlaceholderName(id): ZERO partitions (no replica, no log, no directory,
/// no metadata entry — produce/subscribe find no partition), transient, and a
/// reserved "__" name no DDL can address.  Later topics therefore keep their ids,
/// and the orphan's original name is free again after the restart — once its
/// directory is gone (deleted, or quarantined when the delete fails); a dir that
/// reconcile must leave alone (symlinked topics/ root, unrecognised layout) is
/// reused by a later same-name create, which clears its stale marker.  All
/// placeholders of one boot are logged in a single warn line.  A gap can ALSO come
/// from a damaged catalog (a CRC-truncated or missing catalog log): the placeholder
/// is identical, and reconcileTopicDirs keeps such a committed topic's data
/// (quarantine) — only a "pending"-marked dir is ever deleted.
///
/// @throws std::runtime_error on a record that does not parse or an id claimed by
///         both catalogs — boot must not guess.
struct RecoveredRuntimeTopics {
    std::vector<TopicSpec> specs;            // sorted by topicId, gaps filled
    /// The ids recovery filled with an __orphan.<id> placeholder, ascending.  This is
    /// the ONLY source of truth for "is a placeholder" (B10 fix round 1, I2): never
    /// infer it from a name — a static RUNE_TOPICS name may spell "__orphan.<id>".
    std::vector<int32_t>   placeholderIds;
    /// Fix round 2b: non-empty (the reason) when a catalog log was cut by
    /// Log::recover() (CRC / torn tail / chain gap) or exactly one of the two
    /// catalogs is missing — a gap may then be a LOST committed record, so
    /// reconcileTopicDirs must delete nothing (pass it through).
    std::string            catalogDamage;
    /// Final review I3: every topic the catalogs name — ALL ids, also those below
    /// firstRuntimeId (a static topic typed by DefineTopic, or a runtime topic an
    /// added RUNE_TOPICS entry shifted below the boundary) — id + name only, the
    /// latest record's name per id.  Input to refuseStaticNamesCollidingWithRecovered.
    std::vector<TopicSpec> catalogTopics;
};
[[nodiscard]] RecoveredRuntimeTopics recoverRuntimeTopics(const std::string& dataDir,
                                                          int32_t firstRuntimeId);

/// recoverRuntimeTopics(...).specs — for callers that do not reconcile topic dirs.
[[nodiscard]] std::vector<TopicSpec> recoverRuntimeTopicSpecs(const std::string& dataDir,
                                                              int32_t firstRuntimeId);

/// Final review M1: dataDir/<this> latches catalog damage across boots (see
/// reconcileTopicDirs).  A regular file; its content is the damage reason.
inline constexpr const char* kCatalogDamagedSentinel = ".catalog-damaged";

/// Final review I3: throws (refusing to boot, naming both entries and the catalog
/// topic's id) when a static RUNE_TOPICS name differs only by ASCII case from a
/// topic the catalogs name, or equals it under a DIFFERENT id: both would own one
/// dataDir/topics/<name>/ (a case-fold match opens two Logs over the same files on a
/// case-insensitive filesystem; an exact match is deduplicated by name and shifts
/// every later recovered id).  The same name AND id is one topic (a static topic
/// typed by DefineTopic) — accepted.  A static's id is its position in `statics`.
/// `catalogTopics` = RecoveredRuntimeTopics::
/// catalogTopics.  Call after recoverRuntimeTopics, before anything consumes the
/// merged list (reconcile, TopicRegistry).
void refuseStaticNamesCollidingWithRecovered(const std::vector<TopicSpec>& statics,
                                             const std::vector<TopicSpec>& catalogTopics);

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
