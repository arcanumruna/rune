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
#include <rune/protocol/client/DataMessages.h>

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

/// Task 13 (D27): the topics of `nsId` whose retention a namespace maximum clamps: a user topic with unbounded (-1) or
/// larger-than-maximum time or size retention.  One pass, cold (the SetLimits answer).  A maximum of 0 = unlimited.
[[nodiscard]] inline uint64_t countRetentionClamped(const rune::catalog::CatalogState& s, uint32_t nsId,
                                                    const rune::protocol::NamespaceLimits& L) {
    uint64_t n = 0;
    for (const rune::catalog::TopicObject* t : s.topics()) {
        if (t->nsId != nsId) continue;
        const bool msClamped    = L.maxRetentionMs != 0 && (t->config.retentionMs < 0 ||
                                  static_cast<uint64_t>(t->config.retentionMs) > L.maxRetentionMs);
        const bool bytesClamped = L.maxRetentionBytes != 0 && (t->config.retentionBytes < 0 ||
                                  static_cast<uint64_t>(t->config.retentionBytes) > L.maxRetentionBytes);
        if (msClamped || bytesClamped) ++n;
    }
    return n;
}

/// True when the namespace may use `featureBit` (bit 0 of the record = unrestricted).
[[nodiscard]] constexpr bool nsFeatureAllowed(const rune::protocol::NamespaceLimits& l, uint32_t featureBit) noexcept {
    return l.featureBits == 0 || (l.featureBits & featureBit) != 0;
}

/// The refusal of a view in a namespace that does not allow views.
[[nodiscard]] inline std::optional<std::string> viewsRefusal(const rune::protocol::NamespaceLimits& l) {
    if (nsFeatureAllowed(l, rune::protocol::kNsFeatureViews)) return std::nullopt;
    return std::string("namespace limit exceeded: views are not enabled for this namespace");
}

/// A produce's wire acks byte (bit 0 = All, bit 1 = View) against `lowestAcks` (0 none, 1 leader, >= 2 all).
[[nodiscard]] constexpr bool nsAcksAllowed(const rune::protocol::NamespaceLimits& l, uint8_t wireAcks) noexcept {
    return l.lowestAcks < 2 || rune::protocol::client::acksAll(wireAcks);
}

namespace detail {
[[nodiscard]] inline std::string over(const char* what, uint64_t have, uint64_t limit) {
    return std::string("namespace limit exceeded: ") + what + " (" + std::to_string(have) + " of " + std::to_string(limit) + ")";
}
}  // namespace detail

/// D27: explicit (>= 0) time / size retention above the namespace maximum (0 = unlimited).
[[nodiscard]] inline std::optional<std::string> retentionRefusal(const rune::protocol::NamespaceLimits& L, int64_t retentionMs,
                                                                 int64_t retentionBytes) {
    if (L.maxRetentionMs != 0 && retentionMs >= 0 && static_cast<uint64_t>(retentionMs) > L.maxRetentionMs)
        return detail::over("retention.ms", static_cast<uint64_t>(retentionMs), L.maxRetentionMs);
    if (L.maxRetentionBytes != 0 && retentionBytes >= 0 && static_cast<uint64_t>(retentionBytes) > L.maxRetentionBytes)
        return detail::over("retention.bytes", static_cast<uint64_t>(retentionBytes), L.maxRetentionBytes);
    return std::nullopt;
}

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
        // D27: an explicit retention above the namespace maximum is refused (-1, unbounded, is the unset default: it is
        // clamped by the cleaner instead).
        if (auto why = retentionRefusal(L, o->config.retentionMs, o->config.retentionBytes)) return why;
        return std::nullopt;
    }
    if (const auto* o = std::get_if<cat::AlterTopicConfigOps>(&e.ops)) {
        for (const cat::ConfigChange& c : o->changes) {
            if (c.key == cat::ConfigKey::RetentionMs)
                if (auto why = retentionRefusal(L, c.value, -1)) return why;
            if (c.key == cat::ConfigKey::RetentionBytes)
                if (auto why = retentionRefusal(L, -1, c.value)) return why;
        }
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
        if (auto why = viewsRefusal(L)) return why;
        const NsObjectCounts c = countNsObjects(s, e.nsId);
        if (L.maxViews != 0 && c.views >= L.maxViews) return detail::over("views", c.views, L.maxViews);
        return std::nullopt;
    }
    return std::nullopt;
}

}  // namespace rune::node
