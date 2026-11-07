// src/catalog/ApplyV4.cpp — the formatVer-4 apply rules (lease topics).
//
//   CreateTopic with a lease   the lease CreateTopic rules (each a Precondition): a typed, keyed topic; default key
//                              features (the key index is the lease view); compacted with no retention; the lease view
//                              name '<t>.leases' fits and is free in the namespace.  The commit installs the topic AND
//                              its lease view (objectId + 1, viewTopicId + 1): two object ids and two topic ids.
//   AlterTopicConfig lease.*   the lease keys (7..12) need a lease topic; each lands in a copy of the LeaseSpec, which
//                              must pass validateLeaseSpec; the other keys run the formatVer-3 rules
//   every other kind           the formatVer-3 rules
//
// The state-driven lease rules (guardBefore / guardStaged / excludeLeaseViews / commitLeaseViewDrop) run for every
// formatVer from CatalogState.cpp.
#include "ApplyV4.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/PrimType.h>
#include <rune/codec/Serialize.h>

#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace rune::catalog {
namespace {

static_assert(rune::codec::kMaxLeaseKeyBytes == rune::protocol::kMaxLeaseKeyBytes,
              "the codec's lease key width and the protocol's must agree");

namespace rp = rune::protocol;
using Code = ApplyError::Code;

constexpr std::string_view kLeaseViewSuffix = ".leases";
constexpr uint16_t kFirstUserTag = 4;   // tags 0..3 are the lease system columns (L5)

std::unexpected<ApplyError> fail(int64_t offset, const CatalogEvent& e, const std::string& what) {
    return std::unexpected(ApplyError{offset, Code::Precondition, 0, 0, std::string(kindName(e.kind)) + ": " + what});
}

template <class St>
auto* stagedTopic(St& st) noexcept {
    using Topic = std::conditional_t<std::is_const_v<St>, const TopicObject, TopicObject>;
    auto* obj = std::get_if<Staged>(&st);
    if (obj == nullptr) return static_cast<Topic*>(nullptr);
    if (auto* c = std::get_if<StagedCreate>(obj)) return std::get_if<TopicObject>(&c->object);
    if (auto* u = std::get_if<StagedUpdate>(obj)) return std::get_if<TopicObject>(&u->object);
    return static_cast<Topic*>(nullptr);
}

std::string leaseViewName(const std::string& topicName) { return topicName + std::string(kLeaseViewSuffix); }

/// The pinned layout of a lease topic's schema (a permanent replay rule): the four system columns first, in declaration
/// order, at tags 0..3; no other lease_ field; the key is a user field (empty = fine).
std::string leaseColumnsError(const SchemaDef& schema) {
    using rune::codec::PrimType;
    const rune::codec::Descriptor d = rune::codec::deserialize(schema.descriptorBlob);
    struct Col { const char* name; PrimType type; };
    constexpr Col kCols[] = {{"lease_version", PrimType::Int64}, {"lease_ttl_ms", PrimType::UInt64},
                             {"lease_holder", PrimType::UInt64}, {"lease_remaining_ms", PrimType::UInt64}};
    for (std::size_t i = 0; i < std::size(kCols); ++i) {
        if (i >= d.fields.size() || d.fields[i].id != i || d.fields[i].name != kCols[i].name || d.fields[i].type != kCols[i].type)
            return "a lease topic starts with the four lease system columns";
    }
    for (std::size_t i = std::size(kCols); i < d.fields.size(); ++i)
        if (d.fields[i].name.starts_with("lease_")) return "lease_ field names are reserved";
    for (const Col& c : kCols)
        if (schema.keyField == c.name) return "the key of a lease topic is a user field";
    return {};
}

/// The lease CreateTopic rules over the event (empty = fine).
std::string leaseCreateError(const CatalogState& s, const CatalogEvent& e, const CreateTopicOps& o) {
    if (!o.schema || o.schema->keyField.empty()) return "a lease topic needs a typed, keyed topic";
    // ApplyV3 runs first and refuses a modulo partitioner without autoincrement and autoincrement without key.index,
    // so the key index is the one key feature left to refuse here.
    if (o.schema->keyFeatures.keyIndex) return "a lease topic's key index is its lease view";
    if (const std::string why = leaseColumnsError(*o.schema); !why.empty()) return why;
    if (o.config.cleanupPolicy != 1 || o.config.retentionMs != -1 || o.config.retentionBytes != -1)
        return "a lease topic is compacted and keeps every live lease";
    if (e.objectName.size() + kLeaseViewSuffix.size() > kMaxTopicNameBytes)
        return "the lease view name '" + leaseViewName(e.objectName) + "' is too long";
    const std::string view = leaseViewName(e.objectName);
    if (s.nameTaken(e.nsId, view) || s.liveNameFoldingOnto(e.nsId, view) != nullptr) return "'" + view + "' is taken";
    if (s.nextObjectId() >= std::numeric_limits<uint64_t>::max() - 1) return "object ids are exhausted";
    if (s.nextTopicId() >= std::numeric_limits<int32_t>::max() - 1) return "topic ids are exhausted";
    return {};
}

/// Applies one lease.* key to `spec`; empty = applied, else the Precondition text.
std::string applyLeaseKey(rp::LeaseSpec& spec, const ConfigChange& c) {
    auto u64 = [&](const char* name, uint64_t& dst) -> std::string {
        if (c.value < 0) return std::string(name) + " " + std::to_string(c.value) + " < 0";
        dst = static_cast<uint64_t>(c.value);
        return {};
    };
    auto u32 = [&](const char* name, uint32_t& dst) -> std::string {
        if (c.value < 0 || c.value > static_cast<int64_t>(std::numeric_limits<uint32_t>::max()))
            return std::string(name) + " " + std::to_string(c.value) + " does not fit 32 bits";
        dst = static_cast<uint32_t>(c.value);
        return {};
    };
    switch (c.key) {
    case ConfigKey::LeaseTtlMs:                return u64("lease.ttl", spec.ttlMs);
    case ConfigKey::LeaseMinTtlMs:             return u64("lease.min_ttl", spec.minTtlMs);
    case ConfigKey::LeaseMaxTtlMs:             return u64("lease.max_ttl", spec.maxTtlMs);
    case ConfigKey::LeaseCheckpoint:
        if (c.value != 0 && c.value != 1) return "lease.checkpoint must be 0 or 1";
        spec.checkpoint = static_cast<uint8_t>(c.value);
        return {};
    case ConfigKey::LeaseCheckpointIntervalMs: return u32("lease.checkpoint_interval", spec.checkpointIntervalMs);
    case ConfigKey::LeaseCheckpointRate:       return u32("lease.checkpoint_rate", spec.checkpointRate);
    default:                                   return "key " + std::to_string(static_cast<uint8_t>(c.key)) + " is not a lease key";
    }
}

bool isLeaseKey(ConfigKey k) noexcept { return static_cast<uint8_t>(k) > static_cast<uint8_t>(rp::kLastConfigKeyV3); }

std::expected<StagedV2, ApplyError> alterConfig(const CatalogState& s, const CatalogEvent& e, int64_t offset,
                                                const AlterTopicConfigOps& o) {
    std::vector<ConfigChange> leaseChanges;
    AlterTopicConfigOps rest;
    for (const ConfigChange& c : o.changes) (isLeaseKey(c.key) ? leaseChanges : rest.changes).push_back(c);
    if (leaseChanges.empty()) return ApplyV3::run(s, e, offset);

    // ApplyV1 refuses an empty change list ("no changes"), and a lease-only event still has to pass every gate the
    // other keys pass (the event's validity, the requestId, the namespace gate, liveTopic, the version chain).  So the
    // event runs through the formatVer-3 rules with ONE no-op change: retention.ms set to its current value.
    if (rest.changes.empty()) {
        const TopicObject* cur = s.topicByObjectId(e.objectId);
        rest.changes.push_back(ConfigChange{ConfigKey::RetentionMs, cur != nullptr ? cur->config.retentionMs : 0});
    }
    CatalogEvent copy = e;
    copy.ops = rest;
    auto staged = ApplyV3::run(s, copy, offset);
    if (!staged) return staged;

    TopicObject* topic = stagedTopic(*staged);
    if (topic == nullptr || !topic->lease) return fail(offset, e, "lease.* keys need a lease topic");
    uint32_t seen = 0;
    rp::LeaseSpec spec = *topic->lease;
    for (const ConfigChange& c : leaseChanges) {
        const auto k = static_cast<uint8_t>(c.key);
        if (seen & (1u << k)) return fail(offset, e, "key " + std::to_string(k) + " set twice");
        seen |= 1u << k;
        if (const std::string why = applyLeaseKey(spec, c); !why.empty()) return fail(offset, e, why);
    }
    if (const auto why = rp::validateLeaseSpec(spec)) return fail(offset, e, std::string(*why));
    topic->lease = spec;
    return staged;
}

} // namespace

std::expected<StagedV2, ApplyError> ApplyV4::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (const auto* o = std::get_if<AlterTopicConfigOps>(&e.ops)) return alterConfig(s, e, offset, *o);
    auto staged = ApplyV3::run(s, e, offset);
    if (!staged) return staged;
    const auto* o = std::get_if<CreateTopicOps>(&e.ops);
    if (o == nullptr || !o->lease) return staged;
    TopicObject* topic = stagedTopic(*staged);
    if (topic == nullptr) return staged;   // unreachable: a CreateTopic stages a topic
    if (const std::string why = leaseCreateError(s, e, *o); !why.empty()) return fail(offset, e, why);
    topic->lease = o->lease;
    return staged;
}

Applied ApplyV4::commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st) {
    Applied a = ApplyV3::commit(s, e, offset, fingerprint, std::move(st));
    const auto* o = std::get_if<CreateTopicOps>(&e.ops);
    if (o == nullptr || !o->lease) return a;
    ViewObject v;
    v.objectId = e.objectId + 1;
    v.name = leaseViewName(e.objectName);
    v.version = 1;
    v.nsId = e.nsId;
    v.kind = ViewKind::LeaseIndex;
    v.leaseTopicObjectId = e.objectId;
    v.def.viewTopicId = o->topicId + 1;
    v.def.sinkPartitions = o->partitions;
    v.def.persistence = o->persistence;   // keyField, sourceNames and every blob stay empty
    s.viewNames_.emplace(NsKey{v.nsId, v.name}, v.objectId);
    s.viewTopicIds_.emplace(v.def.viewTopicId, v.objectId);
    a.leaseViewObjectId = v.objectId;
    a.leaseViewTopicId = v.def.viewTopicId;
    s.views_.emplace(v.objectId, std::move(v));
    ++s.nextObjectId_;
    ++s.nextTopicId_;
    return a;
}

std::optional<ApplyError> ApplyV4::guardBefore(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (e.kind == EventKind::DropView || e.kind == EventKind::AlterView) {
        const ViewObject* v = s.viewByObjectId(e.objectId);
        if (v != nullptr && v->nsId == e.nsId && v->kind == ViewKind::LeaseIndex)
            return fail(offset, e, e.kind == EventKind::DropView ? "drop the lease topic" : "a lease view cannot be altered").error();
        return std::nullopt;
    }
    if (const auto* o = std::get_if<AlterSchemaOps>(&e.ops)) {
        const TopicObject* t = s.topicByObjectId(e.objectId);
        if (t == nullptr || t->nsId != e.nsId || !t->lease) return std::nullopt;
        for (const ResolvedAlterOp& op : o->ops) {
            if ((op.type == rune::codec::AlterOpType::Drop || op.type == rune::codec::AlterOpType::Rename) && op.tag < kFirstUserTag)
                return fail(offset, e, "the lease system columns cannot be dropped or renamed").error();
            // for a Rename, `name` is the NEW name
            if ((op.type == rune::codec::AlterOpType::Add || op.type == rune::codec::AlterOpType::Rename) &&
                op.name.starts_with("lease_"))
                return fail(offset, e, "lease_ field names are reserved").error();
        }
    }
    return std::nullopt;
}

std::optional<ApplyError> ApplyV4::guardStaged(const CatalogEvent& e, const StagedV2& st, int64_t offset) {
    if (e.kind != EventKind::AlterTopicConfig) return std::nullopt;
    const TopicObject* t = stagedTopic(st);
    if (t != nullptr && t->lease && (t->config.retentionMs != -1 || t->config.retentionBytes != -1))
        return fail(offset, e, "a lease topic is compacted and keeps every live lease").error();
    return std::nullopt;
}

void ApplyV4::excludeLeaseViews(const CatalogState& s, StagedV2& st) {
    auto* del = std::get_if<StagedNsDeleting>(&st);
    if (del == nullptr) return;
    auto* d = std::get_if<NsDeleting>(&del->record.state);
    if (d == nullptr) return;
    for (auto it = d->pendingViews.begin(); it != d->pendingViews.end();) {
        const ViewObject* v = s.viewByObjectId(*it);
        it = (v != nullptr && v->kind == ViewKind::LeaseIndex) ? d->pendingViews.erase(it) : std::next(it);
    }
}

void ApplyV4::commitLeaseViewDrop(CatalogState& s, const CatalogEvent& e, Applied& applied) {
    if (e.kind != EventKind::DropTopic) return;
    const ViewObject* found = s.leaseViewOf(e.objectId);
    if (found == nullptr) return;
    ViewObject v = *found;   // the erase below invalidates `found`
    s.tombstones_.push_back(Tombstone{v.objectId, v.name, v.version + 1, /*wasView=*/true, v.nsId});
    s.viewNames_.erase(s.viewNames_.find(NsKeyView{v.nsId, v.name}));
    s.viewTopicIds_.erase(v.def.viewTopicId);
    s.views_.erase(v.objectId);
    if (const auto it = s.namespaces_.byId_.find(v.nsId); it != s.namespaces_.byId_.end())
        if (auto* d = std::get_if<NsDeleting>(&it->second.state)) d->pendingViews.erase(v.objectId);
    applied.leaseViewObjectId = v.objectId;
    applied.leaseViewTopicId = v.def.viewTopicId;
}

std::optional<rp::LeaseSpec> ApplyV4::leaseOf(const StagedV2& st) noexcept {
    const TopicObject* t = stagedTopic(st);
    return t != nullptr ? t->lease : std::nullopt;
}

uint64_t ApplyV4::leaseFingerprint(uint64_t fingerprint, const std::optional<rp::LeaseSpec>& lease) noexcept {
    if (!lease) return fingerprint;
    uint8_t block[rp::kLeaseSpecBytes];
    rp::encodeLeaseSpec(*lease, block);
    return detail::fnvBytes(detail::fnvU64(rune::codec::detail::kFnvSeed, fingerprint), block, sizeof block);
}

uint64_t ApplyV4::leaseViewFingerprint(uint64_t viewFingerprint) noexcept {
    return detail::fnvBytes(detail::fnvU64(rune::codec::detail::kFnvSeed, viewFingerprint), "lease-view", 10);
}

} // namespace rune::catalog
