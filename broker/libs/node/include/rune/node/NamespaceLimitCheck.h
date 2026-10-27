#pragma once
// NamespaceLimitCheck.h - the namespace count, durability and feature limits, checked at DDL against the applied
// catalog (namespaces Phase 2 Task 9, D23).  Control plane only (the AdminWorker / SecurityAdmin thread): it counts a
// namespace's objects in one pass over the catalog (cold, DDL rate) and never runs on the produce / fetch / query path.
//
// Sentinels (NamespaceLimits.h): for every u64 limit 0 = unlimited / no floor.  `featureBits` 0 = no restriction (every
// feature allowed): the cluster default is 0, so a namespace that never named a feature set keeps every feature; a
// non-zero value is exactly the allowed set.
//
// "at -> over": a create is refused when count >= limit (the limit is the most objects the namespace may hold).
// Lowering a limit below usage is accepted (SetLimits never deletes): the existing objects stay, new creates refuse.

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace rune::node {

struct NsObjectCounts {
    uint64_t topics = 0, partitions = 0, views = 0, schemas = 0;
};

/// One pass over the live objects of `nsId`.  A topic with a schema is one schema; a view's sink is not a topic.
[[nodiscard]] inline NsObjectCounts countNsObjects(const rune::catalog::CatalogState& s, uint32_t nsId) {
    NsObjectCounts c;
    for (const rune::catalog::TopicObject* t : s.topics()) {
        if (t->nsId != nsId) continue;
        ++c.topics;
        c.partitions += static_cast<uint64_t>(t->partitions > 0 ? t->partitions : 0);
        if (t->schema) ++c.schemas;
    }
    for (const rune::catalog::ViewObject* v : s.views()) if (v->nsId == nsId) ++c.views;
    return c;
}

/// True when the namespace may use `featureBit` (bit 0 of the record = unrestricted).
[[nodiscard]] constexpr bool nsFeatureAllowed(const rune::protocol::NamespaceLimits& l, uint32_t featureBit) noexcept {
    return l.featureBits == 0 || (l.featureBits & featureBit) != 0;
}

/// A produce's wire acks byte (0 = Leader/None, 1 = All) against `lowestAcks` (0 none, 1 leader, >= 2 all).
[[nodiscard]] constexpr bool nsAcksAllowed(const rune::protocol::NamespaceLimits& l, uint8_t wireAcks) noexcept {
    return l.lowestAcks < 2 || wireAcks >= 1;
}

namespace detail {
[[nodiscard]] inline std::string over(const char* what, uint64_t have, uint64_t limit) {
    return std::string("namespace limit exceeded: ") + what + " (" + std::to_string(have) + " of " + std::to_string(limit) + ")";
}
}  // namespace detail

/// The refusal (detail text) the namespace's limits give the catalog event `e`, nullopt when it is within every limit.
/// `brokerCount` resolves replicationFactor 0 ("every broker"); `clusterMinIsr` is the effective min.insync of every
/// topic (topics carry no per-topic value in this codebase).
[[nodiscard]] inline std::optional<std::string>
nsLimitRefusal(const rune::protocol::NamespaceLimits& L, const rune::catalog::CatalogState& s,
               const rune::catalog::CatalogEvent& e, int32_t brokerCount, int32_t clusterMinIsr) {
    namespace cat = rune::catalog;
    using rune::protocol::kNsFeatureCompaction;
    using rune::protocol::kNsFeatureViews;
    if (const auto* o = std::get_if<cat::CreateTopicOps>(&e.ops)) {
        const NsObjectCounts c = countNsObjects(s, e.nsId);
        if (L.maxTopics != 0 && c.topics >= L.maxTopics) return detail::over("topics", c.topics, L.maxTopics);
        const auto parts = static_cast<uint64_t>(o->partitions > 0 ? o->partitions : 0);
        if (L.maxPartitionsPerTopic != 0 && parts > L.maxPartitionsPerTopic)
            return detail::over("partitions per topic", parts, L.maxPartitionsPerTopic);
        if (L.maxPartitionsTotal != 0 && c.partitions + parts > L.maxPartitionsTotal)
            return detail::over("partitions", c.partitions + parts, L.maxPartitionsTotal);
        if (o->schema && L.maxSchemas != 0 && c.schemas >= L.maxSchemas) return detail::over("schemas", c.schemas, L.maxSchemas);
        const auto rf = static_cast<uint64_t>(o->replicationFactor == 0 ? brokerCount : o->replicationFactor);
        if (L.minReplicationFactor != 0 && rf < L.minReplicationFactor)
            return "namespace limit exceeded: replication factor " + std::to_string(rf) + " is below the minimum " +
                   std::to_string(L.minReplicationFactor);
        if (L.maxReplicationFactor != 0 && rf > L.maxReplicationFactor)
            return "namespace limit exceeded: replication factor " + std::to_string(rf) + " is above the maximum " +
                   std::to_string(L.maxReplicationFactor);
        if (L.lowestMinInsync != 0 && static_cast<uint64_t>(clusterMinIsr > 0 ? clusterMinIsr : 0) < L.lowestMinInsync)
            return "namespace limit exceeded: min.insync " + std::to_string(clusterMinIsr) + " is below the minimum " +
                   std::to_string(L.lowestMinInsync);
        if (o->config.cleanupPolicy != 0 && !nsFeatureAllowed(L, kNsFeatureCompaction))
            return "namespace limit exceeded: compaction is not enabled for this namespace";
        return std::nullopt;
    }
    if (std::holds_alternative<cat::CreateSchemaOps>(e.ops)) {   // adopting a schema onto an untyped topic
        const NsObjectCounts c = countNsObjects(s, e.nsId);
        if (L.maxSchemas != 0 && c.schemas >= L.maxSchemas) return detail::over("schemas", c.schemas, L.maxSchemas);
        return std::nullopt;
    }
    if (std::holds_alternative<cat::AlterSchemaOps>(e.ops)) {   // a new schema version
        const cat::TopicObject* t = s.topicByName(e.nsId, e.objectName);
        if (t != nullptr && t->schema && L.maxVersionsPerSchema != 0 &&
            static_cast<uint64_t>(t->schema->schemaVersion) >= L.maxVersionsPerSchema)
            return detail::over("schema versions", t->schema->schemaVersion, L.maxVersionsPerSchema);
        return std::nullopt;
    }
    if (std::holds_alternative<cat::CreateViewOps>(e.ops)) {
        if (!nsFeatureAllowed(L, kNsFeatureViews)) return std::string("namespace limit exceeded: views are not enabled for this namespace");
        const NsObjectCounts c = countNsObjects(s, e.nsId);
        if (L.maxViews != 0 && c.views >= L.maxViews) return detail::over("views", c.views, L.maxViews);
        return std::nullopt;
    }
    return std::nullopt;
}

}  // namespace rune::node
