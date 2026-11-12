// src/catalog/CatalogState.cpp — the catalog state's reads, apply() (formatVer dispatch +
// fingerprint verification), the digest, and the F20 resolve / unresolve of ALTER ops.
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/catalog/NsName.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include "ApplyV1.h"
#include "ApplyV2.h"
#include "ApplyV3.h"
#include "ApplyV4.h"
#include "ApplyV5.h"

#include <algorithm>
#include <exception>
#include <string>
#include <utility>

namespace rune::catalog {

// ── reads ─────────────────────────────────────────────────────────────────────
namespace {
template <class Map, class Index, class Key>
const typename Map::mapped_type* viaIndex(const Map& objects, const Index& index, const Key& key) {
    auto it = index.find(key);
    if (it == index.end()) return nullptr;
    auto ot = objects.find(it->second);
    return ot == objects.end() ? nullptr : &ot->second;
}
template <class Map>
const typename Map::mapped_type* byObjectId(const Map& objects, uint64_t id) {
    auto it = objects.find(id);
    return it == objects.end() ? nullptr : &it->second;
}
} // namespace

const TopicObject* CatalogState::topicByName(uint32_t nsId, std::string_view n) const {
    return viaIndex(topics_, topicNames_, NsKeyView{nsId, n});
}
const TopicObject* CatalogState::topicByObjectId(uint64_t id) const { return byObjectId(topics_, id); }
const TopicObject* CatalogState::topicByTopicId(int32_t topicId) const { return viaIndex(topics_, topicIds_, topicId); }
const ViewObject*  CatalogState::viewByName(uint32_t nsId, std::string_view n) const {
    return viaIndex(views_, viewNames_, NsKeyView{nsId, n});
}
const ViewObject*  CatalogState::viewByObjectId(uint64_t id) const { return byObjectId(views_, id); }
const ViewObject*  CatalogState::viewByViewTopicId(int32_t topicId) const { return viaIndex(views_, viewTopicIds_, topicId); }

const std::string* CatalogState::liveNameFoldingOnto(uint32_t nsId, std::string_view n) const {
    for (const auto* index : {&topicNames_, &viewNames_}) {
        // the namespace's names are one contiguous run of the (nsId, name)-ordered index
        for (auto it = index->lower_bound(NsKeyView{nsId, std::string_view{}}); it != index->end() && it->first.nsId == nsId; ++it)
            if (it->first.name != n && topicNamesFoldEqual(it->first.name, n)) return &it->first.name;
    }
    return nullptr;
}

const ViewObject* CatalogState::leaseViewOf(uint64_t topicObjectId) const {
    for (const auto& [id, v] : views_)
        if (v.kind == ViewKind::LeaseIndex && v.leaseTopicObjectId == topicObjectId) return &v;
    return nullptr;
}

const ViewObject* CatalogState::viewReading(uint32_t nsId, std::string_view topicName) const {
    for (const auto& [id, v] : views_)
        if (v.nsId == nsId &&
            std::find(v.def.sourceNames.begin(), v.def.sourceNames.end(), topicName) != v.def.sourceNames.end())
            return &v;
    return nullptr;
}
std::vector<const TopicObject*> CatalogState::topics() const {
    std::vector<const TopicObject*> out;
    out.reserve(topics_.size());
    for (const auto& [id, t] : topics_) out.push_back(&t);
    return out;
}
std::vector<const ViewObject*> CatalogState::views() const {
    std::vector<const ViewObject*> out;
    out.reserve(views_.size());
    for (const auto& [id, v] : views_) out.push_back(&v);
    return out;
}
std::optional<RequestRecord> CatalogState::request(std::string_view principal, const RequestId& requestId) const {
    if (requestId.none()) return std::nullopt;
    auto it = requests_.find(requestId);
    if (it == requests_.end()) return std::nullopt;
    for (const RequestEntry& en : it->second)
        if (en.principal == principal) return en.rec;
    return std::nullopt;
}

std::optional<RequestRecord> CatalogState::requestOfAnyPrincipal(const RequestId& requestId) const {
    if (requestId.none()) return std::nullopt;
    auto it = requests_.find(requestId);
    if (it == requests_.end()) return std::nullopt;
    const RequestEntry* best = nullptr;
    for (const RequestEntry& en : it->second)
        if (!best || en.rec.offset < best->rec.offset) best = &en;
    return best ? std::optional<RequestRecord>(best->rec) : std::nullopt;
}

uint64_t layeredFingerprint(const TopicObject& t) noexcept {
    return ApplyV5::retireFingerprint(
        ApplyV4::leaseFingerprint(
            ApplyV3::keyFeaturesFingerprint(fingerprintOf(t), t.schema ? t.schema->keyFeatures : rune::protocol::TopicKeyFeatures{}),
            t.lease),
        t.schema ? t.schema->retiredBelow : uint16_t{1});
}

uint64_t layeredFingerprint(const ViewObject& v) noexcept {
    return v.kind == ViewKind::LeaseIndex ? ApplyV4::leaseViewFingerprint(fingerprintOf(v)) : fingerprintOf(v);
}

uint64_t CatalogState::digest() const noexcept {
    // An object of a namespace other than `default` hashes with its namespace mixed in, so the same definition in
    // two namespaces is a different state; `default` objects hash exactly as they did before namespaces existed.
    auto fp = [](uint64_t objectFingerprint, uint32_t nsId) {
        return nsId == kDefaultNsId ? objectFingerprint : objectFingerprintV2(objectFingerprint, nsId);
    };
    uint64_t h = rune::codec::detail::kFnvSeed;
    h = detail::fnvU64(h, static_cast<uint64_t>(appliedOffset_));
    h = detail::fnvU64(h, nextObjectId_);
    h = rune::codec::detail::fnv1a_u32le(h, static_cast<uint32_t>(nextTopicId_));
    // Live objects in objectId order: merge the two objectId-ordered maps.
    auto t = topics_.begin();
    auto v = views_.begin();
    while (t != topics_.end() || v != views_.end()) {
        if (v == views_.end() || (t != topics_.end() && t->first < v->first)) {
            h = detail::fnvU64(h, fp(layeredFingerprint(t->second), t->second.nsId));
            ++t;
        } else {
            h = detail::fnvU64(h, fp(layeredFingerprint(v->second), v->second.nsId));
            ++v;
        }
    }
    for (const Tombstone& d : tombstones_) h = detail::fnvU64(h, fp(fingerprintOf(d), d.nsId));
    // Namespaces, only when they left a trace: the never-reused id counter and every record that is not an untouched
    // `default`.
    if (namespaces_.nextNsId() > 1) h = rune::codec::detail::fnv1a_u32le(h, namespaces_.nextNsId());
    namespaces_.forEach([&](const NamespaceRecord& r) {
        if (r.nsId == kDefaultNsId && r.version == 0) return;
        h = detail::fnvU64(h, r.fingerprint);
        // The pending sets derive from the objects already in the digest; the recorded steps do not.
        if (const auto* d = std::get_if<NsDeleting>(&r.state)) h = rune::codec::detail::fnv1a_byte(h, d->stepMask);
    });
    if (namespaces_.tombstoneFold() != 0) h = detail::fnvU64(h, namespaces_.tombstoneFold());
    return h;
}

// ── apply ─────────────────────────────────────────────────────────────────────
namespace {
/// The staged change of `e` under its formatVer's rules, and the fingerprint it must carry.
struct Prepared { StagedV2 staged; uint64_t fingerprint; };

std::expected<StagedV2, ApplyError> runRules(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (e.formatVer == kFormatV5) return ApplyV5::run(s, e, offset);
    if (e.formatVer == kFormatV4) return ApplyV4::run(s, e, offset);
    if (e.formatVer == kFormatV3) return ApplyV3::run(s, e, offset);
    if (e.formatVer == kFormatV2) return ApplyV2::run(s, e, offset);
    auto v1 = ApplyV1::run(s, e, offset);   // formatVer 1: nsId is 0 (the event validation refuses anything else)
    if (!v1) return std::unexpected(std::move(v1.error()));
    return StagedV2{std::move(*v1)};
}

uint64_t stagedFingerprint(const CatalogEvent& e, const StagedV2& st) noexcept {
    if (e.formatVer == kFormatV5) return ApplyV5::fingerprintOfStaged(e, st);
    if (e.formatVer == kFormatV4) return ApplyV4::fingerprintOfStaged(e, st);
    if (e.formatVer == kFormatV3) return ApplyV3::fingerprintOfStaged(e, st);
    if (e.formatVer == kFormatV2) return ApplyV2::fingerprintOfStaged(e, st);
    return ApplyV1::fingerprintOfStaged(std::get<Staged>(st));
}

std::expected<Prepared, ApplyError> prepare(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    // The lease rules that depend on the state, not on the formatVer (no-ops on a log without a lease view).
    if (auto err = ApplyV4::guardBefore(s, e, offset)) return std::unexpected(std::move(*err));
    auto staged = runRules(s, e, offset);
    if (!staged) return std::unexpected(std::move(staged.error()));
    if (auto err = ApplyV4::guardStaged(e, *staged, offset)) return std::unexpected(std::move(*err));
    ApplyV4::excludeLeaseViews(s, *staged);
    ApplyV3::carryKeyFeatures(s, *staged);
    const uint64_t base = stagedFingerprint(e, *staged);
    const uint16_t floor = ApplyV5::retiredBelowOf(*staged);
    return Prepared{std::move(*staged),
                    ApplyV5::retireFingerprint(
                        ApplyV4::leaseFingerprint(ApplyV3::keyFeaturesFingerprint(base, ApplyV3::keyFeaturesOf(*staged)),
                                                  ApplyV4::leaseOf(*staged)),
                        floor)};
}

std::expected<void, ApplyError> checkFormat(const CatalogEvent& e, int64_t offset) {
    if (e.formatVer < kFormatV1 || e.formatVer > kFormatV5)
        return std::unexpected(ApplyError{offset, ApplyError::Code::UnsupportedFormat, 0, 0,
                                          "formatVer " + std::to_string(e.formatVer) + " has no apply rules"});
    return {};
}
} // namespace

std::expected<Applied, ApplyError> apply(CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (auto f = checkFormat(e, offset); !f) return std::unexpected(std::move(f.error()));
    if (offset <= s.appliedOffset())
        return std::unexpected(ApplyError{offset, ApplyError::Code::Precondition, 0, 0,
                                          "offset " + std::to_string(offset) + " is not past the applied offset " +
                                              std::to_string(s.appliedOffset())});
    auto prepared = prepare(s, e, offset);
    if (!prepared) return std::unexpected(std::move(prepared.error()));
    if (prepared->fingerprint != e.fingerprint)
        return std::unexpected(ApplyError{offset, ApplyError::Code::FingerprintMismatch, e.fingerprint, prepared->fingerprint,
                                          std::string(kindName(e.kind)) + " '" + e.objectName + "'"});
    Applied applied = [&] {
        if (e.formatVer == kFormatV5)
            return ApplyV5::commit(s, e, offset, prepared->fingerprint, std::move(prepared->staged));
        if (e.formatVer == kFormatV4)
            return ApplyV4::commit(s, e, offset, prepared->fingerprint, std::move(prepared->staged));
        if (e.formatVer == kFormatV3)
            return ApplyV3::commit(s, e, offset, prepared->fingerprint, std::move(prepared->staged));
        if (e.formatVer == kFormatV2)
            return ApplyV2::commit(s, e, offset, prepared->fingerprint, std::move(prepared->staged));
        return ApplyV1::commit(s, e, offset, prepared->fingerprint, std::move(std::get<Staged>(prepared->staged)));
    }();
    ApplyV4::commitLeaseViewDrop(s, e, applied);   // a DropTopic takes the topic's lease view along (every formatVer)
    return applied;
}

std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState& s, const CatalogEvent& e) {
    if (auto f = checkFormat(e, -1); !f) return std::unexpected(std::move(f.error()));
    auto prepared = prepare(s, e, -1);
    if (!prepared) return std::unexpected(std::move(prepared.error()));
    return prepared->fingerprint;
}

std::string_view applyErrorName(ApplyError::Code c) noexcept {
    switch (c) {
    case ApplyError::Code::UnsupportedFormat:   return "UnsupportedFormat";
    case ApplyError::Code::Precondition:        return "Precondition";
    case ApplyError::Code::FingerprintMismatch: return "FingerprintMismatch";
    case ApplyError::Code::BadOps:              return "BadOps";
    case ApplyError::Code::BadNsId:             return "BadNsId";
    }
    return "?";
}

// ── F20: resolve / unresolve ALTER ops ────────────────────────────────────────
namespace {
namespace cd = rune::codec;

/// The evolving descriptor's LIVE fields (tag, name), tracked through an op list.
struct LiveFields {
    struct Field { uint16_t tag; std::string name; };
    std::vector<Field> fields;

    Field* byName(std::string_view n) {
        for (auto& f : fields) if (f.name == n) return &f;
        return nullptr;
    }
    Field* byTag(uint16_t t) {
        for (auto& f : fields) if (f.tag == t) return &f;
        return nullptr;
    }
    void erase(const Field* f) { fields.erase(fields.begin() + (f - fields.data())); }
};

std::expected<LiveFields, std::string> liveFieldsOf(const SchemaState& schema) {
    try {
        const cd::Descriptor d = cd::deserialize(std::span<const uint8_t>(schema.evolvingBlob.data(),
                                                                          schema.evolvingBlob.size()));
        LiveFields live;
        for (const auto& f : d.fields)
            if (f.untilVersion == 0xFFFF) live.fields.push_back({f.id, f.name});
        return live;
    } catch (const std::exception& x) {
        return std::unexpected(std::string("the evolving descriptor does not deserialize: ") + x.what());
    }
}
/// Field names in an error are client-supplied: escaped so a rejected one cannot forge log lines.
std::string quoted(std::string_view s) { return "'" + escapeNameForLog(s) + "'"; }

/// An ADD's width rule: a FixedStr has a width, nothing else does.
std::optional<std::string> addWidthError(std::string_view name, cd::PrimType type, uint16_t fixedLen) {
    if (type == cd::PrimType::FixedStr && fixedLen == 0) return "ADD " + quoted(name) + ": a FixedStr needs a width";
    if (type != cd::PrimType::FixedStr && fixedLen != 0) return "ADD " + quoted(name) + ": only a FixedStr has a width";
    return std::nullopt;
}
} // namespace

std::expected<std::vector<ResolvedAlterOp>, std::string>
resolveAlterOps(const SchemaState& schema, std::span<const cd::AlterOp> ops) {
    auto live = liveFieldsOf(schema);
    if (!live) return std::unexpected(std::move(live.error()));
    std::vector<ResolvedAlterOp> out;
    out.reserve(ops.size());
    uint32_t nextTag = schema.nextTag;
    for (const cd::AlterOp& op : ops) {
        ResolvedAlterOp r;
        r.type = op.type;
        switch (op.type) {
        case cd::AlterOpType::Add: {
            if (op.name.empty()) return std::unexpected(std::string("ADD: the field name is empty"));
            if (live->byName(op.name)) return std::unexpected("ADD " + quoted(op.name) + ": collides with a live field");
            if (auto w = addWidthError(op.name, op.primType, op.fixedLen)) return std::unexpected(std::move(*w));
            if (nextTag >= 0xFFFF) return std::unexpected("ADD " + quoted(op.name) + ": field tags are exhausted");
            r.tag = static_cast<uint16_t>(nextTag++);
            r.name = op.name;
            r.primType = op.primType;
            r.fixedLen = op.fixedLen;
            live->fields.push_back({r.tag, r.name});
            break;
        }
        case cd::AlterOpType::Drop: {
            auto* f = live->byName(op.name);
            if (f == nullptr) return std::unexpected("DROP " + quoted(op.name) + ": not a live field");
            r.tag = f->tag;
            live->erase(f);
            break;
        }
        case cd::AlterOpType::Rename: {
            auto* f = live->byName(op.name);
            if (f == nullptr) return std::unexpected("RENAME " + quoted(op.name) + ": not a live field");
            if (op.newName.empty()) return std::unexpected("RENAME " + quoted(op.name) + ": the new name is empty");
            auto* other = live->byName(op.newName);
            if (other != nullptr && other != f)
                return std::unexpected("RENAME " + quoted(op.name) + ": " + quoted(op.newName) + " collides with a live field");
            r.tag = f->tag;
            r.name = op.newName;
            f->name = op.newName;
            break;
        }
        case cd::AlterOpType::SetCompatibility:
            r.mode = op.mode;
            break;
        default:
            return std::unexpected("unknown ALTER op type " + std::to_string(static_cast<int>(op.type)));
        }
        out.push_back(std::move(r));
    }
    return out;
}

std::expected<std::vector<cd::AlterOp>, std::string>
unresolveAlterOps(const SchemaState& schema, std::span<const ResolvedAlterOp> ops) {
    auto live = liveFieldsOf(schema);
    if (!live) return std::unexpected(std::move(live.error()));
    const ResolvedAlterOp canonical{};   // every field an op's type does not use keeps this value
    std::vector<cd::AlterOp> out;
    out.reserve(ops.size());
    uint32_t nextTag = schema.nextTag;
    for (const ResolvedAlterOp& r : ops) {
        const std::string at = "tag " + std::to_string(r.tag);
        cd::AlterOp op{};
        op.type = r.type;
        switch (r.type) {
        case cd::AlterOpType::Add: {
            if (nextTag >= 0xFFFF) return std::unexpected("ADD " + quoted(r.name) + ": field tags are exhausted");
            if (r.tag != nextTag)
                return std::unexpected("ADD " + quoted(r.name) + ": " + at + " is not the next tag " + std::to_string(nextTag));
            if (r.mode != canonical.mode) return std::unexpected("ADD " + quoted(r.name) + ": carries a compatibility mode");
            if (r.name.empty()) return std::unexpected(std::string("ADD: the field name is empty"));
            if (live->byName(r.name)) return std::unexpected("ADD " + quoted(r.name) + ": collides with a live field");
            if (auto w = addWidthError(r.name, r.primType, r.fixedLen)) return std::unexpected(std::move(*w));
            ++nextTag;
            op.name = r.name;
            op.primType = r.primType;
            op.fixedLen = r.fixedLen;
            live->fields.push_back({r.tag, r.name});
            break;
        }
        case cd::AlterOpType::Drop: {
            if (!r.name.empty() || r.primType != canonical.primType || r.fixedLen != 0 || r.mode != canonical.mode)
                return std::unexpected("DROP " + at + ": carries fields a DROP does not use");
            auto* f = live->byTag(r.tag);
            if (f == nullptr) return std::unexpected("DROP " + at + ": not a live field");
            op.name = f->name;
            live->erase(f);
            break;
        }
        case cd::AlterOpType::Rename: {
            if (r.primType != canonical.primType || r.fixedLen != 0 || r.mode != canonical.mode)
                return std::unexpected("RENAME " + at + ": carries fields a RENAME does not use");
            auto* f = live->byTag(r.tag);
            if (f == nullptr) return std::unexpected("RENAME " + at + ": not a live field");
            if (r.name.empty()) return std::unexpected("RENAME " + at + ": the new name is empty");
            auto* other = live->byName(r.name);
            if (other != nullptr && other != f)
                return std::unexpected("RENAME " + at + ": " + quoted(r.name) + " collides with a live field");
            op.name = f->name;
            op.newName = r.name;
            f->name = r.name;
            break;
        }
        case cd::AlterOpType::SetCompatibility:
            if (r.tag != 0 || !r.name.empty() || r.primType != canonical.primType || r.fixedLen != 0)
                return std::unexpected(std::string("SET COMPATIBILITY: carries fields it does not use"));
            op.mode = r.mode;
            break;
        default:
            return std::unexpected("unknown ALTER op type " + std::to_string(static_cast<int>(r.type)));
        }
        out.push_back(std::move(op));
    }
    return out;
}

} // namespace rune::catalog
