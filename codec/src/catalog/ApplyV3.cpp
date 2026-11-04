// src/catalog/ApplyV3.cpp — the formatVer-3 apply rules (checked writes).
//
//   CreateTopic / CreateSchema   the staged topic's schema takes the event's keyFeatures; keyIndex needs a non-empty
//                                keyField and a Persistence::Disk topic (else Precondition)
//   every other kind             the formatVer-2 rules
#include "ApplyV3.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>

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

} // namespace

std::expected<StagedV2, ApplyError> ApplyV3::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    auto staged = ApplyV2::run(s, e, offset);
    if (!staged) return staged;
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
