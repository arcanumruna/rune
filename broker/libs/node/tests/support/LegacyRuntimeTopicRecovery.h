#pragma once
// LegacyRuntimeTopicRecovery.h — TEST-ONLY (catalog history Task 9 fix round 1):
// the retired boot recovery from __topic_schemas / __views (see the .cpp).
#include <rune/node/NodeConfig.h>
#include <rune/node/RuntimeTopicRecovery.h>

#include <cstdint>
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

} // namespace rune::node
