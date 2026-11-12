// src/catalog/ApplyV5.cpp — the formatVer-5 apply rules (schema retire, #419).
//
//   RetireSchemaVersions   the gates of every object kind (a valid event, an unused requestId, the namespace exists and is
//                          not DELETING), then: the live topic (objectId, namespace, name and version chain); the topic
//                          is typed; its schema version is not exhausted; `below` is past the current floor (the floor only
//                          moves forward, so a repeated statement is refused) and not past the current schema version (the
//                          current version is never retired).  The result is a StagedUpdate with only `retiredBelow` and
//                          the catalog version changed: schemaVersion, blob, tags and config are untouched.
//   every other kind       the formatVer-4 rules
#include "ApplyV5.h"
#include "Wire.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>

#include <string>
#include <utility>
#include <variant>

namespace rune::catalog {
namespace {

using Code = ApplyError::Code;

std::unexpected<ApplyError> fail(int64_t offset, Code code, std::string detail) {
    return std::unexpected(ApplyError{offset, code, 0, 0, std::move(detail)});
}
std::string prefix(const CatalogEvent& e) { return std::string(kindName(e.kind)) + ": "; }

std::expected<StagedV2, ApplyError> retire(const CatalogState& s, const CatalogEvent& e, int64_t offset,
                                           const RetireSchemaVersionsOps& o) {
    if (auto v = wire::validateEvent(e))
        return fail(offset, Code::BadOps, prefix(e) + "invalid event: " + std::string(encodeErrorName(*v)));
    if (!e.principal.requestId.none() && s.request(e.principal.authenticated, e.principal.requestId))
        return fail(offset, Code::Precondition, prefix(e) + "requestId " + std::to_string(e.principal.requestId.hi) + ":" +
                                                    std::to_string(e.principal.requestId.lo) + " is already applied");
    const NamespaceRecord* ns = s.namespaces().byId(e.nsId);
    if (ns == nullptr)
        return fail(offset, Code::BadNsId, prefix(e) + "namespace " + std::to_string(e.nsId) + " does not exist");
    if (std::holds_alternative<NsDeleting>(ns->state))   // only the drops pass a DELETING namespace
        return fail(offset, Code::Precondition, prefix(e) + "namespace is DELETING");

    auto t = liveTopicOf(s, e, offset);
    if (!t) return std::unexpected(std::move(t.error()));
    if (!(*t)->schema) return fail(offset, Code::Precondition, prefix(e) + "not a typed topic");
    if (auto err = schemaVersionErrorOf(e, offset, **t)) return std::unexpected(std::move(*err));
    const SchemaState& cur = *(*t)->schema;
    if (o.below <= cur.retiredBelow)
        return fail(offset, Code::Precondition,
                    prefix(e) + "versions below " + std::to_string(o.below) + " are already retired (retired below " +
                        std::to_string(cur.retiredBelow) + ")");
    if (o.below > cur.schemaVersion)
        return fail(offset, Code::Precondition,
                    prefix(e) + "version " + std::to_string(o.below) + " is above the current version " +
                        std::to_string(cur.schemaVersion));
    TopicObject next = **t;
    next.schema->retiredBelow = o.below;
    next.version = e.newVersion;
    return StagedV2{Staged{StagedUpdate{std::move(next)}}};
}

} // namespace

std::expected<StagedV2, ApplyError> ApplyV5::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (const auto* o = std::get_if<RetireSchemaVersionsOps>(&e.ops)) return retire(s, e, offset, *o);
    return ApplyV4::run(s, e, offset);
}

uint16_t ApplyV5::retiredBelowOf(const StagedV2& st) noexcept {
    const auto* obj = std::get_if<Staged>(&st);
    if (obj == nullptr) return 1;
    const TopicObject* t = nullptr;
    if (const auto* c = std::get_if<StagedCreate>(obj)) t = std::get_if<TopicObject>(&c->object);
    else if (const auto* u = std::get_if<StagedUpdate>(obj)) t = std::get_if<TopicObject>(&u->object);
    return t != nullptr && t->schema ? t->schema->retiredBelow : uint16_t{1};
}

uint64_t ApplyV5::retireFingerprint(uint64_t fingerprint, uint16_t retiredBelow) noexcept {
    if (retiredBelow == 1) return fingerprint;
    uint64_t h = detail::fnvU64(rune::codec::detail::kFnvSeed, fingerprint);
    h = rune::codec::detail::fnv1a_byte(h, 'R');
    return rune::codec::detail::fnv1a_u16le(h, retiredBelow);
}

} // namespace rune::catalog
