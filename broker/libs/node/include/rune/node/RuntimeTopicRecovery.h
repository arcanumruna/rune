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
/// and the orphan's original name is free again after the restart.  All
/// placeholders of one boot are logged in a single warn line.
///
/// @throws std::runtime_error on a record that does not parse or an id claimed by
///         both catalogs — boot must not guess.
[[nodiscard]] std::vector<TopicSpec> recoverRuntimeTopicSpecs(const std::string& dataDir,
                                                              int32_t firstRuntimeId);

/// "__orphan.<topicId>" — the recovery placeholder name for a catalog-less id.
[[nodiscard]] std::string orphanPlaceholderName(int32_t topicId);

struct TopicDirReconciliation {
    std::vector<std::filesystem::path> deleted;       // orphan-placeholder dirs (canonical paths)
    std::vector<std::filesystem::path> quarantined;   // destinations under dataDir/quarantine/
};

/// B10 / user decision D4: reconcile dataDir/topics/ against the full catalog
/// (`topics` = static + recovered, incl. __orphan.<id> placeholders).  For each
/// direct child of canonical(dataDir/topics) that no topic claims (exactly or
/// ignoring ASCII case) and that has the recognised layout
/// <name>/[topic-id] + <digits>/<regular files>:
///   * its topic-id marker names an __orphan.<id> placeholder -> DELETED;
///   * otherwise (no marker, other id, unreadable) -> MOVED to
///     dataDir/quarantine/<name>-<nowUnixSeconds>[-N], never deleted.
/// Symlinks are never followed (a symlinked topics/ or quarantine/ root, child,
/// partition dir or file makes the entry "not ours": left alone).  Each action is
/// logged at warn with its path(s).  Never throws.  Call on the startup thread
/// after the recovered specs are appended and BEFORE the TopicRegistry opens logs.
[[nodiscard]] TopicDirReconciliation
reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& topics, int64_t nowUnixSeconds);

} // namespace rune::node
