// src/catalog/ApplyV3.cpp — the formatVer-3 apply rules (checked writes).
//
//   CreateTopic / CreateSchema   the staged topic's schema takes the event's keyFeatures; keyIndex needs a non-empty
//                                keyField and a Persistence::Disk topic (else Precondition)
//   autoIncrement                a CreateTopic only; a UInt64 / Int64 / UInt32 key, Disk, key.index, Modulo, cleanup.policy
//                                compact, no retention, and the reserve and max ranges (else Precondition); Modulo needs it
//   AlterTopicConfig             an autoincrement topic keeps retention.ms and retention.bytes at -1
//   every other kind             the formatVer-2 rules
#include "ApplyV3.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace rune::catalog {
namespace {

/// The topic a staged object-kind change installs (nullptr: a namespace kind, a drop, a view).
template <class St>
auto* stagedTopic(St& st) noexcept {
    using Topic = std::conditional_t<std::is_const_v<St>, const TopicObject, TopicObject>;
    auto* obj = std::get_if<Staged>(&st);
    if (obj == nullptr) return static_cast<Topic*>(nullptr);
    if (auto* c = std::get_if<StagedCreate>(obj)) return std::get_if<TopicObject>(&c->object);
    if (auto* u = std::get_if<StagedUpdate>(obj)) return std::get_if<TopicObject>(&u->object);
    return static_cast<Topic*>(nullptr);
}

ApplyError precondition(const CatalogEvent& e, int64_t offset, const std::string& what) {
    return ApplyError{offset, ApplyError::Code::Precondition, 0, 0, std::string(kindName(e.kind)) + ": " + what};
}

/// The autoincrement rules over a staged topic; empty = the topic is fine.
std::string autoIncrementError(const TopicObject& t) {
    using rune::protocol::Partitioner;
    const auto& f = t.schema->keyFeatures;
    if (!f.autoIncrement) return f.partitioner == Partitioner::Modulo ? "the modulo partitioner needs autoincrement" : std::string{};
    const rune::codec::Descriptor d = rune::codec::deserialize(t.schema->evolvingBlob);
    const auto* key = d.find(t.schema->keyField);
    if (key == nullptr || (key->type != rune::codec::PrimType::UInt64 && key->type != rune::codec::PrimType::Int64 &&
                           key->type != rune::codec::PrimType::UInt32))
        return "autoincrement needs a uint64, int64 or uint32 key";
    if (t.persistence != Persistence::Disk) return "autoincrement needs a persisted topic";
    if (!f.keyIndex) return "autoincrement needs key.index";
    if (f.partitioner != Partitioner::Modulo) return "autoincrement needs the modulo partitioner";
    if (t.config.cleanupPolicy != 1) return "autoincrement needs cleanup.policy = compact";
    if (t.config.retentionMs != -1 || t.config.retentionBytes != -1)
        return "autoincrement topics keep every record: retention.ms and retention.bytes must be -1";
    if (f.autoIncrementReserve < 1 || f.autoIncrementReserve > rune::protocol::kAutoIncrementReserveMax)
        return "autoincrement.reserve must be between 1 and " + std::to_string(rune::protocol::kAutoIncrementReserveMax);
    const uint64_t cap = key->type == rune::codec::PrimType::UInt32 ? 0xFFFF'FFFFull : rune::protocol::kAutoIncrementMaxDefault;
    const uint64_t low = 2 * static_cast<uint64_t>(t.partitions) - 1;
    if (f.autoIncrementMax < low || f.autoIncrementMax > cap)
        return "autoincrement.max must be between " + std::to_string(low) + " and " + std::to_string(cap);
    return {};
}

} // namespace

std::expected<StagedV2, ApplyError> ApplyV3::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    auto staged = ApplyV2::run(s, e, offset);
    if (!staged) return staged;
    if (e.kind == EventKind::AlterTopicConfig) {
        const TopicObject* topic = stagedTopic(*staged);
        if (topic != nullptr && topic->schema && topic->schema->keyFeatures.autoIncrement &&
            (topic->config.retentionMs != -1 || topic->config.retentionBytes != -1))
            return std::unexpected(precondition(e, offset, "autoincrement topics keep every record: retention.ms and retention.bytes must be -1"));
        return staged;
    }
    const SchemaDef* def = nullptr;
    if (const auto* t = std::get_if<CreateTopicOps>(&e.ops)) { if (t->schema) def = &*t->schema; }
    else if (const auto* c = std::get_if<CreateSchemaOps>(&e.ops)) def = &c->schema;
    if (def == nullptr) return staged;
    TopicObject* topic = stagedTopic(*staged);
    if (topic == nullptr || !topic->schema) return staged;
    topic->schema->keyFeatures = def->keyFeatures;
    if (def->keyFeatures.keyIndex && (topic->schema->keyField.empty() || topic->persistence != Persistence::Disk))
        return std::unexpected(ApplyError{offset, ApplyError::Code::Precondition, 0, 0,
                                          std::string(kindName(e.kind)) + ": key.index needs a key field and a persisted topic"});
    if (e.kind == EventKind::CreateSchema && def->keyFeatures.autoIncrement)
        return std::unexpected(precondition(e, offset, "autoincrement needs a new topic"));
    if (const std::string why = autoIncrementError(*topic); !why.empty()) return std::unexpected(precondition(e, offset, why));
    return staged;
}

void ApplyV3::carryKeyFeatures(const CatalogState& s, StagedV2& st) noexcept {
    TopicObject* topic = stagedTopic(st);
    if (topic == nullptr || !topic->schema) return;
    const TopicObject* prior = s.topicByObjectId(topic->objectId);
    if (prior != nullptr && prior->schema) topic->schema->keyFeatures = prior->schema->keyFeatures;
}

rune::protocol::TopicKeyFeatures ApplyV3::keyFeaturesOf(const StagedV2& st) noexcept {
    const TopicObject* topic = stagedTopic(st);
    return topic != nullptr && topic->schema ? topic->schema->keyFeatures : rune::protocol::TopicKeyFeatures{};
}

uint64_t ApplyV3::keyFeaturesFingerprint(uint64_t fingerprint, const rune::protocol::TopicKeyFeatures& features) noexcept {
    if (features == rune::protocol::TopicKeyFeatures{}) return fingerprint;
    uint8_t block[rune::protocol::kTopicKeyFeaturesWireLen];
    rune::protocol::putTopicKeyFeatures(block, features);
    return detail::fnvBytes(detail::fnvU64(rune::codec::detail::kFnvSeed, fingerprint), block, sizeof block);
}

} // namespace rune::catalog
