#pragma once
// WriteState.h — checked writes Part 02: what a worker knows about a typed topic's writes.
//
// TopicWriteSpec is the per-topic spec (the key facts and one materialized Validator + Descriptor per schema version),
// owned by the worker that holds it (shared-nothing; installed by cold ops).  PartitionWriteState is the per-partition
// handle next to RcSlot: a typed partition's state points at its topic's spec, an untyped partition has none.
// checkTypedRecord is the one per-record rule set; checkTypedBatch is the bounded walk over it (hot path: no
// allocation, no lock, no virtual call).
#include <rune/catalog/CatalogState.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/node/VerdictState.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/RejectedOffsetIndex.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace rune::node {

inline constexpr uint16_t kNoKeyField  = 0xFFFF;
inline constexpr uint32_t kNoKeyOffset = 0xFFFFFFFF;

/// The WriteSpecUpdate tail: [topicId i32][keyFieldId u16][keyType u8][keyWidth u8][flags u8: bit0 legacyValidate].
struct TopicWriteSpecWire {
    int32_t              topicId        = -1;
    uint16_t             keyFieldId     = kNoKeyField;   ///< the key field's durable tag; kNoKeyField = a keyless topic
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int32;
    uint8_t              keyWidth       = 0;             ///< 0 = the key cannot be checked (a key type from before the topic key rule)
    bool                 legacyValidate = false;         ///< the catalog's SchemaDef::validate: validates below kCheckedWrites
    bool operator==(const TopicWriteSpecWire&) const = default;
};
inline constexpr std::size_t kTopicWriteSpecWireLen = 9;

/// Writes kTopicWriteSpecWireLen bytes; 0 when `out` is too small.
inline std::size_t encodeTopicWriteSpec(const TopicWriteSpecWire& w, std::span<uint8_t> out) noexcept {
    if (out.size() < kTopicWriteSpecWireLen) return 0;
    std::memcpy(out.data(), &w.topicId, 4);
    std::memcpy(out.data() + 4, &w.keyFieldId, 2);
    out[6] = static_cast<uint8_t>(w.keyType);
    out[7] = w.keyWidth;
    out[8] = w.legacyValidate ? 1 : 0;
    return kTopicWriteSpecWireLen;
}

[[nodiscard]] inline std::optional<TopicWriteSpecWire> decodeTopicWriteSpec(std::span<const uint8_t> in) noexcept {
    if (in.size() != kTopicWriteSpecWireLen) return std::nullopt;
    TopicWriteSpecWire w;
    std::memcpy(&w.topicId, in.data(), 4);
    std::memcpy(&w.keyFieldId, in.data() + 4, 2);
    w.keyType        = static_cast<rune::codec::PrimType>(in[6]);
    w.keyWidth       = in[7];
    w.legacyValidate = (in[8] & 1) != 0;
    return w;
}

/// Cold: nullopt for an untyped topic.  The key field is the live field named by the catalog's keyField.
[[nodiscard]] inline std::optional<TopicWriteSpecWire> writeSpecWireOf(const rune::catalog::TopicObject& t) {
    if (!t.schema) return std::nullopt;
    TopicWriteSpecWire w;
    w.topicId        = t.topicId;
    w.legacyValidate = t.schema->validate;
    if (t.schema->keyField.empty()) return w;
    const rune::codec::Descriptor d = rune::codec::deserialize(t.schema->evolvingBlob);
    for (const auto& f : d.fields) {
        if (f.untilVersion != 0xFFFF || f.name != t.schema->keyField) continue;
        w.keyFieldId = f.id;
        w.keyType    = f.type;
        w.keyWidth   = rune::codec::isTopicKeyType(f.type, rune::codec::fieldWidth(f)) ? static_cast<uint8_t>(rune::codec::fieldWidth(f)) : 0;
        break;
    }
    return w;
}

struct SpecVersion {
    rune::codec::Validator  validator;
    rune::codec::Descriptor descriptor;
    uint32_t                keyValueOffset = kNoKeyOffset;   ///< record byte offset of the key field's value (4 + its block offset)
};

class TopicWriteSpec {
public:
    int32_t               topicId        = -1;
    uint16_t              keyFieldId     = kNoKeyField;
    rune::codec::PrimType keyType        = rune::codec::PrimType::Int32;
    uint8_t               keyWidth       = 0;
    bool                  legacyValidate = false;

    [[nodiscard]] const SpecVersion* versionFor(uint16_t version) const noexcept {
        return version < versions_.size() ? versions_[version].get() : nullptr;
    }
    [[nodiscard]] const rune::codec::Validator* validatorFor(uint16_t version) const noexcept {
        const SpecVersion* sv = versionFor(version);
        return sv != nullptr ? &sv->validator : nullptr;
    }
    [[nodiscard]] const rune::codec::Descriptor* descriptorFor(uint16_t version) const noexcept {
        const SpecVersion* sv = versionFor(version);
        return sv != nullptr ? &sv->descriptor : nullptr;
    }
    [[nodiscard]] uint16_t maxVersion() const noexcept { return maxVersion_; }
    [[nodiscard]] bool hasFacts() const noexcept { return hasFacts_; }

    /// Cold.  Installs (or replaces) one schema version's validator and descriptor.
    void installVersion(uint16_t version, rune::codec::Descriptor materialized) {
        if (versions_.size() <= version) versions_.resize(static_cast<std::size_t>(version) + 1);
        auto sv = std::make_unique<SpecVersion>(SpecVersion{rune::codec::Validator(materialized), std::move(materialized), kNoKeyOffset});
        setKeyOffset_(*sv);
        versions_[version] = std::move(sv);
        if (version > maxVersion_) maxVersion_ = version;
    }

    /// Cold.  Takes the key facts and recomputes every installed version's key offset.
    void setFacts(const TopicWriteSpecWire& w) {
        topicId        = w.topicId;
        keyFieldId     = w.keyFieldId;
        keyType        = w.keyType;
        keyWidth       = w.keyWidth;
        legacyValidate = w.legacyValidate;
        hasFacts_      = true;
        for (auto& sv : versions_)
            if (sv) setKeyOffset_(*sv);
    }

private:
    void setKeyOffset_(SpecVersion& sv) const noexcept {
        sv.keyValueOffset = kNoKeyOffset;
        if (!hasFacts_ || keyFieldId == kNoKeyField) return;
        for (const auto& f : sv.descriptor.fields)
            if (f.id == keyFieldId) { sv.keyValueOffset = 4 + f.offset; return; }
    }

    std::vector<std::unique_ptr<SpecVersion>> versions_;   // index = schema version; grown only by installVersion
    uint16_t maxVersion_ = 0;
    bool     hasFacts_   = false;
};

/// One owned typed partition's write state, next to RcSlot.  Later parts append their per-partition members here.
class VerdictSource;

struct PartitionWriteState {
    const TopicWriteSpec*              spec = nullptr;
    rune::storage::RejectedOffsetIndex rejected;       ///< reserved when the partition becomes a verdict source (VerdictSource.cpp)
    ConstraintSetTimeline              constraintSets;
    MarkerLedger                       markers;
    VerdictSource*                     verdicts = nullptr;   ///< the judge, once this worker leads and hosts verdict views
    int64_t                            judgedThrough = 0;    ///< the judge's cursor, published at the end of each of its passes
    uint64_t                           mismatchesLogged = 0; ///< rejected-index determinism alarms already reported
    bool                               growOwed = false;     ///< the judge saw the rejected index cross 75 %: the worker's cold maintenance grows it

    /// A judge is attached or the latest logged set is non-empty.
    [[nodiscard]] bool isVerdictSource() const noexcept;
    /// The latest logged set has a member that can reject (Unique, Check or TestReject).
    [[nodiscard]] bool isConstrainedSource() const noexcept;
    /// Every offset below it has a verdict this node knows; INT64_MAX when no offset needs one.
    [[nodiscard]] int64_t verdictKnownThrough() const noexcept {
        if (constraintSets.size() == 0) return INT64_MAX;
        if (verdicts != nullptr) return judgedThrough;
        const int64_t k = rejected.coveredThrough();
        return constraintSets.latest().empty() && k > constraintSets.lastChangeOffset() ? INT64_MAX : k;
    }
};

enum class TypedCheck : uint8_t { Accept, Reject, VersionAhead };

namespace detail {
[[nodiscard]] inline uint16_t le16(const char* p) noexcept {
    return static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
}
}  // namespace detail

/// The one per-record rule set.  `level` is the cluster metadata feature level: at kCheckedWrites every typed topic
/// validates fully (UTF-8, wire key = key field); below it only a topic created with `validate` checks, as before.
/// A tombstone (empty or null value) is never decoded: its key must be the key field's width.  A record of a version
/// above the highest installed one is VersionAhead (retriable: the version's SchemaUpdate has not landed here yet).
[[nodiscard]] inline TypedCheck checkTypedRecord(const TopicWriteSpec& spec, const rune::protocol::RecordSpan& r, int16_t level) noexcept {
    const bool checked = level >= rune::replication::MetadataFeature::kCheckedWrites;
    if (!checked && !spec.legacyValidate) return TypedCheck::Accept;
    const bool keyRule = checked && spec.keyFieldId != kNoKeyField && spec.keyWidth != 0;
    if (r.value.empty()) {
        if (spec.keyFieldId == kNoKeyField) return TypedCheck::Reject;
        return (keyRule && r.key.size() != spec.keyWidth) ? TypedCheck::Reject : TypedCheck::Accept;
    }
    if (r.value.size() < 4) return TypedCheck::Reject;
    const uint16_t v = detail::le16(r.value.data() + 2);
    const SpecVersion* sv = spec.versionFor(v);
    if (sv == nullptr) return v > spec.maxVersion() ? TypedCheck::VersionAhead : TypedCheck::Reject;
    const auto check = checked ? rune::codec::StringCheck::Utf8 : rune::codec::StringCheck::None;
    if (!sv->validator.validate({reinterpret_cast<const uint8_t*>(r.value.data()), r.value.size()}, check).ok) return TypedCheck::Reject;
    if (keyRule && (sv->keyValueOffset == kNoKeyOffset || r.key.size() != spec.keyWidth ||
                    std::memcmp(r.key.data(), r.value.data() + sv->keyValueOffset, spec.keyWidth) != 0))
        return TypedCheck::Reject;
    return TypedCheck::Accept;
}

/// The bounded walk over checkTypedRecord: the first record that is not accepted decides; a batch whose framing does
/// not fit is Reject.  Below the level a topic without `validate` is not walked at all.
[[nodiscard]] inline TypedCheck checkTypedBatch(const TopicWriteSpec& spec, std::span<const uint8_t> batch, int16_t level) noexcept {
    if (level < rune::replication::MetadataFeature::kCheckedWrites && !spec.legacyValidate) return TypedCheck::Accept;
    TypedCheck result = TypedCheck::Accept;
    const bool walked = rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan& r) {
        if (result == TypedCheck::Accept) result = checkTypedRecord(spec, r, level);
    });
    return walked ? result : TypedCheck::Reject;
}

}  // namespace rune::node
