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
#include <string>
#include <vector>

namespace rune::node {

/// Returns the TopicSpecs of every runtime topic recorded in
/// `<dataDir>/__topic_schemas` and `<dataDir>/__views`, sorted by topicId.
///
/// Ids below `firstRuntimeId` (the number of static config topics) belong to
/// static topics and are skipped.  TopicRegistry assigns ids sequentially, so
/// the recovered ids must be exactly firstRuntimeId, firstRuntimeId+1, ... —
/// anything else would recreate topics under the wrong ids.
///
/// Missing catalog directories mean "nothing to recover" and are not created.
///
/// @throws std::runtime_error on a record that does not parse, an id claimed by
///         both catalogs, or a non-contiguous id sequence — boot must not guess.
[[nodiscard]] std::vector<TopicSpec> recoverRuntimeTopicSpecs(const std::string& dataDir,
                                                              int32_t firstRuntimeId);

} // namespace rune::node
