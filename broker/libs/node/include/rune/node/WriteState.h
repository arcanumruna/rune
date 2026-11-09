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
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/node/AutoInc.h>
#include <rune/node/lease/LeaseFoldEvents.h>
#include <rune/node/lease/LeaseRuntime.h>
#include <rune/node/VerdictState.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/RejectedOffsetIndex.h>

#include <algorithm>
#include <cassert>
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

/// The WriteSpecUpdate tail: [topicId i32][keyFieldId u16][keyType u8][keyWidth u8][flags u8: bit0 legacyValidate, bit1 keyIndex,
/// bit2 autoIncrement], then, with autoIncrement only, [partitioner u8][autoIncrementMax u64][autoIncrementReserve u32]
/// [partitionCount i32] (26 bytes in all); or, with bit3 (lease; never with autoIncrement), the 39-byte LeaseSpec block
/// (rune::protocol::encodeLeaseSpec) at byte 9, then [leaseViewTopicId i32] at byte 48 (52 bytes in all).  Bits 4-7 are refused.
struct TopicWriteSpecWire {
    int32_t              topicId        = -1;
    uint16_t             keyFieldId     = kNoKeyField;   ///< the key field's durable tag; kNoKeyField = a keyless topic
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int32;
    uint8_t              keyWidth       = 0;             ///< 0 = the key cannot be checked (a key type from before the topic key rule)
    bool                 legacyValidate = false;         ///< the catalog's SchemaDef::validate: validates below kCheckedWrites
    bool                 keyIndex       = false;         ///< the topic's key.index feature
    bool                 autoIncrement  = false;         ///< the topic's autoincrement feature; the fields below ride the wire only with it
    rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a;
    uint64_t             autoIncrementMax     = rune::protocol::kAutoIncrementMaxDefault;       ///< the effective cap
    uint32_t             autoIncrementReserve = rune::protocol::kAutoIncrementReserveDefault;
    int32_t              partitionCount = 0;
    std::optional<rune::protocol::LeaseSpec> lease;      ///< a lease topic's spec; rides the wire as flags bit 3 + 39 bytes
    int32_t              leaseViewTopicId = -1;          ///< a lease topic: the sink topic id of its lease view (`<topic>.leases`), from the catalog; rides after the LeaseSpec block
    bool operator==(const TopicWriteSpecWire&) const = default;
};
inline constexpr std::size_t kTopicWriteSpecWireLen = 9;
inline constexpr std::size_t kTopicWriteSpecAutoIncWireLen = 26;
inline constexpr std::size_t kTopicWriteSpecLeaseWireLen = 9 + rune::protocol::kLeaseSpecBytes + 4;   // 52

[[nodiscard]] constexpr std::size_t topicWriteSpecWireLen(const TopicWriteSpecWire& w) noexcept {
    return w.lease ? kTopicWriteSpecLeaseWireLen : w.autoIncrement ? kTopicWriteSpecAutoIncWireLen : kTopicWriteSpecWireLen;
}

/// Writes topicWriteSpecWireLen(w) bytes; 0 when `out` is too small.
inline std::size_t encodeTopicWriteSpec(const TopicWriteSpecWire& w, std::span<uint8_t> out) noexcept {
    assert(!(w.lease && w.autoIncrement) && "a lease topic has no autoincrement");
    const std::size_t len = topicWriteSpecWireLen(w);
    if (out.size() < len) return 0;
    std::memcpy(out.data(), &w.topicId, 4);
    std::memcpy(out.data() + 4, &w.keyFieldId, 2);
    out[6] = static_cast<uint8_t>(w.keyType);
    out[7] = w.keyWidth;
    out[8] = static_cast<uint8_t>((w.legacyValidate ? 1 : 0) | (w.keyIndex ? 2 : 0) | (w.autoIncrement ? 4 : 0) | (w.lease ? 8 : 0));
    if (w.lease) {
        rune::protocol::encodeLeaseSpec(*w.lease, out.data() + 9);
        std::memcpy(out.data() + 9 + rune::protocol::kLeaseSpecBytes, &w.leaseViewTopicId, 4);
    }
    if (w.autoIncrement) {
        out[9] = static_cast<uint8_t>(w.partitioner);
        std::memcpy(out.data() + 10, &w.autoIncrementMax, 8);
        std::memcpy(out.data() + 18, &w.autoIncrementReserve, 4);
        std::memcpy(out.data() + 22, &w.partitionCount, 4);
    }
    return len;
}

[[nodiscard]] inline std::optional<TopicWriteSpecWire> decodeTopicWriteSpec(std::span<const uint8_t> in) noexcept {
    if (in.size() != kTopicWriteSpecWireLen && in.size() != kTopicWriteSpecAutoIncWireLen &&
        in.size() != kTopicWriteSpecLeaseWireLen)
        return std::nullopt;
    const bool autoInc  = (in[8] & 4) != 0;
    const bool hasLease = (in[8] & 8) != 0;
    if ((in[8] & 0xF0) != 0 || (autoInc && hasLease) || autoInc != (in.size() == kTopicWriteSpecAutoIncWireLen) ||
        hasLease != (in.size() == kTopicWriteSpecLeaseWireLen))
        return std::nullopt;
    TopicWriteSpecWire w;
    std::memcpy(&w.topicId, in.data(), 4);
    std::memcpy(&w.keyFieldId, in.data() + 4, 2);
    w.keyType        = static_cast<rune::codec::PrimType>(in[6]);
    w.keyWidth       = in[7];
    w.legacyValidate = (in[8] & 1) != 0;
    w.keyIndex       = (in[8] & 2) != 0;
    if (hasLease) {
        rune::protocol::LeaseSpec spec{};
        if (!rune::protocol::decodeLeaseSpec(in.data() + 9, rune::protocol::kLeaseSpecBytes, spec)) return std::nullopt;
        w.lease = spec;
        std::memcpy(&w.leaseViewTopicId, in.data() + 9 + rune::protocol::kLeaseSpecBytes, 4);
    }
    if (autoInc) {
        if (in[9] > static_cast<uint8_t>(rune::protocol::Partitioner::Modulo)) return std::nullopt;
        w.autoIncrement = true;
        w.partitioner   = static_cast<rune::protocol::Partitioner>(in[9]);
        std::memcpy(&w.autoIncrementMax, in.data() + 10, 8);
        std::memcpy(&w.autoIncrementReserve, in.data() + 18, 4);
        std::memcpy(&w.partitionCount, in.data() + 22, 4);
    }
    return w;
}

/// Cold: nullopt for an untyped topic.  The key field is the live field named by the catalog's keyField.
/// `leaseViewTopicId`: a lease topic's lease view sink id, looked up in the catalog by the caller (never derived from the topic id).
[[nodiscard]] inline std::optional<TopicWriteSpecWire> writeSpecWireOf(const rune::catalog::TopicObject& t, int32_t leaseViewTopicId = -1) {
    if (!t.schema) return std::nullopt;
    TopicWriteSpecWire w;
    w.topicId        = t.topicId;
    w.legacyValidate = t.schema->validate;
    w.keyIndex       = t.schema->keyFeatures.keyIndex;
    w.lease          = t.lease;
    if (t.lease) w.leaseViewTopicId = leaseViewTopicId;
    if (const auto& f = t.schema->keyFeatures; f.autoIncrement) {
        w.autoIncrement        = true;
        w.partitioner          = f.partitioner;
        w.autoIncrementMax     = f.autoIncrementMax;
        w.autoIncrementReserve = f.autoIncrementReserve;
        w.partitionCount       = t.partitions;
    }
    if (t.schema->keyField.empty()) return w;
    const rune::codec::Descriptor d = rune::codec::deserialize(t.schema->evolvingBlob);
    for (const auto& f : d.fields) {
        if (f.untilVersion != 0xFFFF || f.name != t.schema->keyField) continue;
        w.keyFieldId = f.id;
        w.keyType    = f.type;
        const bool keyOk = t.lease ? rune::codec::isLeaseTopicKeyType(f.type, rune::codec::fieldWidth(f))   // up to 128 bytes
                                   : rune::codec::isTopicKeyType(f.type, rune::codec::fieldWidth(f));
        w.keyWidth   = keyOk ? static_cast<uint8_t>(rune::codec::fieldWidth(f)) : 0;
        break;
    }
    return w;
}

/// L5: a lease topic's first four columns are the system columns the lease hook reads and writes at fixed record offsets.
[[nodiscard]] inline bool leaseLayoutOk(const rune::codec::Descriptor& d) noexcept {
    using rune::codec::PrimType;
    struct Col { const char* name; PrimType type; uint32_t offset; };
    static constexpr Col kCols[4] = {{"lease_version", PrimType::Int64, 0},
                                     {"lease_ttl_ms", PrimType::UInt64, 8},
                                     {"lease_holder", PrimType::UInt64, 16},
                                     {"lease_remaining_ms", PrimType::UInt64, 24}};
    if (d.fields.size() < 4) return false;
    for (uint16_t i = 0; i < 4; ++i) {
        const auto& f = d.fields[i];
        if (f.id != i || f.name != kCols[i].name || f.type != kCols[i].type || f.offset != kCols[i].offset) return false;
    }
    return true;
}

struct SpecVersion {
    rune::codec::Validator  validator;
    rune::codec::Descriptor descriptor;
    uint32_t                keyValueOffset = kNoKeyOffset;   ///< record byte offset of the key field's value (4 + its block offset)
    uint32_t                installSeq = 0;   ///< TopicWriteSpec::installSeq_ at this install (never 0, never reused): the lease cover cache's key
};

class TopicWriteSpec {
public:
    int32_t               topicId        = -1;
    uint16_t              keyFieldId     = kNoKeyField;
    rune::codec::PrimType keyType        = rune::codec::PrimType::Int32;
    uint8_t               keyWidth       = 0;
    bool                  legacyValidate = false;
    bool                  keyIndex       = false;
    bool                  autoIncrement  = false;
    rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a;
    uint64_t              autoIncrementMax     = rune::protocol::kAutoIncrementMaxDefault;
    uint32_t              autoIncrementReserve = rune::protocol::kAutoIncrementReserveDefault;
    int32_t               partitionCount = 0;
    std::optional<rune::protocol::LeaseSpec> lease;   ///< a lease topic's spec (set by setFacts)
    /// A lease topic: bumped when leaseLayoutSource() gains a field id (an AddField, through installVersion or FilterSchema,
    /// whichever delivers the id first), so the lease index, whose row layout has one column per source field, is rebuilt
    /// from the log (WorkerViewSet::rebuildKeyIndex_).  That is
    /// sufficient: a Drop or a Rename keeps every field id in the layout (a dropped column just projects null), and the
    /// judge and the replay take each record's writer from this spec (leaseWriterOf), holding (PrepareStatus::Pending)
    /// while a version or a field id of the layout is not there yet, never accepting a record they cannot apply.
    uint32_t              leaseLayoutGen = 0;
    /// The topic's evolving descriptor (every field ever declared), from the FilterSchema op; the key index decodes each writer version through it.
    std::optional<rune::codec::Descriptor> evolving;

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
    /// A lease topic installs nothing (false) when the descriptor breaks the system-column layout (leaseLayoutOk); the
    /// version still counts as seen (maxVersion_ rises), so a record at it is answered Reject, never the retriable VersionAhead.
    bool installVersion(uint16_t version, rune::codec::Descriptor materialized) {
        if (lease && !leaseLayoutOk(materialized)) {
            if (version > maxVersion_) maxVersion_ = version;
            markRefused_(version, true);
            return false;
        }
        markRefused_(version, false);
        // A lease index has one row column per field id of leaseLayoutSource(): a version holding an id the merged source lacks
        // makes its layout stale.  This is the single bump for SchemaUpdate (FilterSchema bumps through the same test).
        if (lease && layoutLacks(materialized)) ++leaseLayoutGen;
        if (versions_.size() <= version) versions_.resize(static_cast<std::size_t>(version) + 1);
        auto sv = std::make_unique<SpecVersion>(SpecVersion{rune::codec::Validator(materialized), std::move(materialized), kNoKeyOffset, ++installSeq_});
        setKeyOffset_(*sv);
        versions_[version] = std::move(sv);
        if (version > maxVersion_) maxVersion_ = version;
        return true;
    }

    /// A lease topic: the version was refused by installVersion / setFacts (its system columns are wrong).  A definite answer:
    /// the judge refuses its records (InvalidRequest) instead of waiting for an install that will never come.
    [[nodiscard]] bool versionRefused(uint16_t version) const noexcept { return version < refused_.size() && refused_[version] != 0; }

    /// Cold.  True when `d` holds a field id the lease layout source (leaseLayoutSource) does not have yet.
    [[nodiscard]] bool layoutLacks(const rune::codec::Descriptor& d) const noexcept {
        for (const auto& f : d.fields) {
            if (evolving && std::any_of(evolving->fields.begin(), evolving->fields.end(), [&](const auto& o) { return o.id == f.id; })) continue;
            bool known = false;
            for (const auto& sv : versions_)
                if (sv && std::any_of(sv->descriptor.fields.begin(), sv->descriptor.fields.end(), [&](const auto& o) { return o.id == f.id; })) { known = true; break; }
            if (!known) return true;
        }
        return false;
    }

    /// Cold.  The descriptor a lease index's row layout is built from: the MERGE, by field id, of `evolving` (when present)
    /// and every installed version's materialized descriptor.  Fields absent from `evolving` are appended in id order, each
    /// with the type / width of a version that declares it.  The layout therefore depends on SchemaUpdate alone (the writer
    /// versions) as well as on FilterSchema, and a lease write can never be held for a FilterSchema that never comes.
    /// nullopt while neither is present.
    [[nodiscard]] std::optional<rune::codec::Descriptor> leaseLayoutSource() const {
        std::optional<rune::codec::Descriptor> out;
        if (evolving) out = *evolving;
        std::vector<const rune::codec::FieldDescriptor*> extra;
        for (const auto& sv : versions_) {
            if (!sv) continue;
            if (!out) { out = rune::codec::Descriptor{}; out->name = sv->descriptor.name; out->version = sv->descriptor.version; out->blockLength = sv->descriptor.blockLength; }
            for (const auto& f : sv->descriptor.fields) {
                const bool inOut = std::any_of(out->fields.begin(), out->fields.end(), [&](const auto& o) { return o.id == f.id; });
                if (!inOut && std::none_of(extra.begin(), extra.end(), [&](const auto* o) { return o->id == f.id; })) extra.push_back(&f);
            }
        }
        if (!out) return out;
        std::sort(extra.begin(), extra.end(), [](const auto* a, const auto* b) { return a->id < b->id; });
        for (const auto* f : extra) out->fields.push_back(*f);
        return out;
    }

    /// Cold.  Takes the key facts and recomputes every installed version's key offset.  With a lease, every installed
    /// version whose layout is not ok is erased; returns how many were erased.
    uint32_t setFacts(const TopicWriteSpecWire& w) {
        topicId        = w.topicId;
        keyFieldId     = w.keyFieldId;
        keyType        = w.keyType;
        keyWidth       = w.keyWidth;
        legacyValidate = w.legacyValidate;
        keyIndex       = w.keyIndex;
        autoIncrement  = w.autoIncrement;
        partitioner    = w.partitioner;
        autoIncrementMax     = w.autoIncrementMax;
        autoIncrementReserve = w.autoIncrementReserve;
        partitionCount = w.partitionCount;
        lease          = w.lease;
        hasFacts_      = true;
        uint32_t erased = 0;
        for (auto& sv : versions_) {
            if (!sv) continue;
            if (lease && !leaseLayoutOk(sv->descriptor)) { sv.reset(); ++erased; markRefused_(static_cast<uint16_t>(&sv - versions_.data()), true); continue; }
            setKeyOffset_(*sv);
        }
        return erased;
    }

private:
    void markRefused_(uint16_t version, bool refused) {
        if (!refused && version >= refused_.size()) return;
        if (refused_.size() <= version) refused_.resize(static_cast<std::size_t>(version) + 1, 0);
        refused_[version] = refused ? 1 : 0;
    }
    void setKeyOffset_(SpecVersion& sv) const noexcept {
        sv.keyValueOffset = kNoKeyOffset;
        if (!hasFacts_ || keyFieldId == kNoKeyField) return;
        for (const auto& f : sv.descriptor.fields)
            if (f.id == keyFieldId) { sv.keyValueOffset = 4 + f.offset; return; }
    }

    std::vector<std::unique_ptr<SpecVersion>> versions_;   // index = schema version; grown only by installVersion
    std::vector<uint8_t> refused_;   // index = schema version; 1 = installVersion / setFacts refused it (cold-grown)
    uint16_t maxVersion_ = 0;
    uint32_t installSeq_ = 0;   // bumped by every successful installVersion (a replaced version gets a new number)
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
    uint64_t                           seenLogGeneration = UINT64_MAX;   ///< the replica's log generation the timeline and ledger are in step with
    int64_t                            scanHw = 0;           ///< the replica HW at the start of this pass: the scan feed commits ledger slots below it
    bool                               growOwed = false;     ///< the judge saw the rejected index cross 75 %: the worker's cold maintenance grows it
    AutoIncState                       autoInc;              ///< the partition's id counter (an autoincrement topic only)
    AssignedBaseWindow                 assignedBases;        ///< the last assigned bases per producer, for retried key-0 batches
    AutoIncStats                       autoIncStats;
    /// A lease topic: the highest offset whose update or revoke the lease index's per-row `lastAppended` may not have recorded
    /// (-1: none).  `lastAppended` lives in the current lease table's aux area and only onAppended writes it, so a fresh table
    /// (a leadership or layout rebuild) starts every row at -1 while the updates in [judgedThrough, LEO) are still unjudged, and an
    /// update appended while the index was not ready, or before its row was replayed, recorded nothing.  A revoke re-encoded from
    /// the row in that window would drop the update (spec 6.2, L19).  Set to LEO - 1 on every judge activation of a lease source
    /// and raised by onAppended for an untracked update; leaseIndexReady requires judgedThrough > it, so it converges as the judge
    /// advances past that offset (no timer).
    int64_t                            leaseUntrackedThrough = -1;
    LeaseFoldEvents                    leaseEvents;          ///< a lease topic: one event per accepted record, until the wheels consume them
    /// A lease topic's wheels and entries (Task 10): exist only on the partition leader while the lease index is ready, under one
    /// identity (leadership generation, lease rebuild epoch); Worker::leaseDuty_ creates and destroys it.  Cold-allocated.
    std::unique_ptr<lease::LeaseRuntime> leaseRuntime;
    bool                               staleBrokerLogged = false;   ///< the stale-broker-record warning was logged for this partition

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
