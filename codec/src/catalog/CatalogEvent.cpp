#include <rune/catalog/CatalogEvent.h>
#include "Wire.h"

#include <new>
#include <type_traits>
#include <utility>

namespace rune::catalog {
namespace {

using wire::Reader;
using wire::Writer;

static_assert(kMaxStr16Bytes == 0xFFFF && kMaxStr32Bytes == 0xFFFF'FFFF && kMaxListCount == 0xFFFF,
              "formatVer-1 wire widths are frozen: u16 / u32 length prefixes, u16 list counts");

// ── body writers (frozen, formatVer 1) ─────────────────────────────────────
void putConfig(Writer& w, const TopicConfig& c) {
    w.u8(c.cleanupPolicy); w.i64(c.retentionMs); w.i64(c.retentionBytes); w.i64(c.deleteRetentionMs);
    w.i32(c.minCleanableDirtyRatioPpm); w.i64(c.segmentRollBytes); w.i64(c.minCompactionLagMs);
    w.i64(c.maxCompactionLagMs); w.i64(c.retentionCheckMs); w.i64(c.fileDeleteDelayMs);
    w.u8(c.durability); w.u8(c.durabilityTier); w.str16(c.coPartitionGroup); w.str16(c.partitionKey);
}
void putSchema(Writer& w, const SchemaDef& s) {
    w.str16(s.keyField); w.u8(s.validate ? 1 : 0); w.bytes32(s.descriptorBlob); w.u16(s.nextTag);
}
void putView(Writer& w, const ViewDef& v) {
    w.i32(v.viewTopicId); w.i32(v.sinkPartitions); w.u8(static_cast<uint8_t>(v.persistence));
    w.str16(v.keyField);
    w.count16(v.sourceNames.size());
    for (const auto& n : v.sourceNames) w.str16(n);
    w.bytes32(v.normalizedPlanBlob); w.bytes32(v.compiledPlanBlob); w.bytes32(v.outputDescriptorBlob);
}
void putOps(Writer& w, const Ops& ops) {
    w.u8(static_cast<uint8_t>(kindOf(ops)));   // ops tag == kind (catches a kind/ops mix-up)
    std::visit([&](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateTopicOps>) {
            w.i32(o.topicId); w.i32(o.partitions); w.i16(o.replicationFactor);
            w.u8(static_cast<uint8_t>(o.persistence)); putConfig(w, o.config);
            w.u8(o.schema ? 1 : 0);
            if (o.schema) putSchema(w, *o.schema);
        } else if constexpr (std::is_same_v<T, DropTopicOps>) {
            w.i32(o.topicId);
        } else if constexpr (std::is_same_v<T, AlterTopicConfigOps>) {
            w.count16(o.changes.size());
            for (const auto& c : o.changes) { w.u8(static_cast<uint8_t>(c.key)); w.i64(c.value); }
        } else if constexpr (std::is_same_v<T, CreateSchemaOps>) {
            putSchema(w, o.schema);
        } else if constexpr (std::is_same_v<T, AlterSchemaOps>) {
            w.count16(o.ops.size());
            for (const auto& a : o.ops) {   // u8 type, u16 tag, str16 name, u8 primType, u16 fixedLen, u8 mode
                w.u8(static_cast<uint8_t>(a.type)); w.u16(a.tag); w.str16(a.name);
                w.u8(static_cast<uint8_t>(a.primType)); w.u16(a.fixedLen); w.u8(static_cast<uint8_t>(a.mode));
            }
        } else if constexpr (std::is_same_v<T, SetCompatibilityOps>) {
            w.u8(static_cast<uint8_t>(o.mode));
        } else if constexpr (std::is_same_v<T, CreateViewOps> || std::is_same_v<T, AlterViewOps>) {
            putView(w, o.view);
        } else {   // DropViewOps
            w.i32(o.viewTopicId);
        }
    }, ops);
}

// ── body readers ────────────────────────────────────────────────────────────
// The smallest encoding of one list element: a list's count is checked against the
// remaining bytes with these BEFORE the list is sized (a corrupt count never allocates).
constexpr std::size_t kMinSourceNameBytes      = 2;                  // str16 ""
constexpr std::size_t kConfigChangeBytes       = 1 + 8;              // u8 key, i64 value
constexpr std::size_t kMinResolvedAlterOpBytes = 1 + 2 + 2 + 1 + 2 + 1;   // type, tag, str16 "", prim, fixedLen, mode
// Each returns Truncated / BadEnum explicitly; an enum byte is never cast unchecked.
std::optional<DecodeError> getPersistence(Reader& r, Persistence& p) {
    uint8_t b; if (!r.u8(b)) return DecodeError::Truncated;
    if (b > 1) return DecodeError::BadEnum;
    p = static_cast<Persistence>(b); return std::nullopt;
}
std::optional<DecodeError> getConfig(Reader& r, TopicConfig& c) {
    if (!r.u8(c.cleanupPolicy)) return DecodeError::Truncated;
    if (c.cleanupPolicy > 2) return DecodeError::BadEnum;
    if (!r.i64(c.retentionMs) || !r.i64(c.retentionBytes) || !r.i64(c.deleteRetentionMs) ||
        !r.i32(c.minCleanableDirtyRatioPpm) || !r.i64(c.segmentRollBytes) || !r.i64(c.minCompactionLagMs) ||
        !r.i64(c.maxCompactionLagMs) || !r.i64(c.retentionCheckMs) || !r.i64(c.fileDeleteDelayMs) ||
        !r.u8(c.durability) || !r.u8(c.durabilityTier) || !r.str16(c.coPartitionGroup) || !r.str16(c.partitionKey))
        return DecodeError::Truncated;
    if (c.durability > 1 || c.durabilityTier > 2) return DecodeError::BadEnum;
    return std::nullopt;
}
std::optional<DecodeError> getSchema(Reader& r, SchemaDef& s) {
    uint8_t v;
    if (!r.str16(s.keyField) || !r.u8(v) || !r.bytes32(s.descriptorBlob) || !r.u16(s.nextTag))
        return DecodeError::Truncated;
    if (v > 1) return DecodeError::BadEnum;
    s.validate = v == 1; return std::nullopt;
}
std::optional<DecodeError> getView(Reader& r, ViewDef& v) {
    if (!r.i32(v.viewTopicId) || !r.i32(v.sinkPartitions)) return DecodeError::Truncated;
    if (auto e = getPersistence(r, v.persistence)) return e;
    uint16_t n;
    if (!r.str16(v.keyField) || !r.u16(n)) return DecodeError::Truncated;
    if (!r.canHold(n, kMinSourceNameBytes)) return DecodeError::Truncated;
    v.sourceNames.resize(n);
    for (auto& s : v.sourceNames) if (!r.str16(s)) return DecodeError::Truncated;
    if (!r.bytes32(v.normalizedPlanBlob) || !r.bytes32(v.compiledPlanBlob) || !r.bytes32(v.outputDescriptorBlob))
        return DecodeError::Truncated;
    return std::nullopt;
}
std::expected<Ops, DecodeError> getOps(Reader& r, EventKind kind) {
    uint8_t tag; if (!r.u8(tag)) return std::unexpected(DecodeError::Truncated);
    if (tag != static_cast<uint8_t>(kind)) return std::unexpected(DecodeError::KindOpsMismatch);
    switch (kind) {
    case EventKind::CreateTopic: {
        CreateTopicOps o;
        if (!r.i32(o.topicId) || !r.i32(o.partitions) || !r.i16(o.replicationFactor))
            return std::unexpected(DecodeError::Truncated);
        if (auto e = getPersistence(r, o.persistence)) return std::unexpected(*e);
        if (auto e = getConfig(r, o.config)) return std::unexpected(*e);
        uint8_t has; if (!r.u8(has)) return std::unexpected(DecodeError::Truncated);
        if (has > 1) return std::unexpected(DecodeError::BadEnum);
        if (has) { SchemaDef s; if (auto e = getSchema(r, s)) return std::unexpected(*e); o.schema = std::move(s); }
        return o;
    }
    case EventKind::DropTopic: { DropTopicOps o; if (!r.i32(o.topicId)) return std::unexpected(DecodeError::Truncated); return o; }
    case EventKind::AlterTopicConfig: {
        AlterTopicConfigOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        if (!r.canHold(n, kConfigChangeBytes)) return std::unexpected(DecodeError::Truncated);
        o.changes.resize(n);
        for (auto& c : o.changes) {
            uint8_t k; if (!r.u8(k) || !r.i64(c.value)) return std::unexpected(DecodeError::Truncated);
            if (k < 1 || k > 6) return std::unexpected(DecodeError::BadEnum);
            c.key = static_cast<ConfigKey>(k);
        }
        return o;
    }
    case EventKind::CreateSchema: { CreateSchemaOps o; if (auto e = getSchema(r, o.schema)) return std::unexpected(*e); return o; }
    case EventKind::AlterSchema: {
        AlterSchemaOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        if (!r.canHold(n, kMinResolvedAlterOpBytes)) return std::unexpected(DecodeError::Truncated);
        o.ops.resize(n);
        for (auto& a : o.ops) {
            uint8_t t, pt, m;
            if (!r.u8(t) || !r.u16(a.tag) || !r.str16(a.name) || !r.u8(pt) || !r.u16(a.fixedLen) || !r.u8(m))
                return std::unexpected(DecodeError::Truncated);
            if (t < 1 || t > 4 || pt > static_cast<uint8_t>(rune::codec::PrimType::FixedStr) || m > 1)
                return std::unexpected(DecodeError::BadEnum);
            a.type = static_cast<rune::codec::AlterOpType>(t); a.primType = static_cast<rune::codec::PrimType>(pt);
            a.mode = static_cast<rune::codec::CompatMode>(m);
        }
        return o;
    }
    case EventKind::SetCompatibility: {
        uint8_t m; if (!r.u8(m)) return std::unexpected(DecodeError::Truncated);
        if (m > 1) return std::unexpected(DecodeError::BadEnum);
        return SetCompatibilityOps{static_cast<rune::codec::CompatMode>(m)};
    }
    case EventKind::CreateView: { CreateViewOps o; if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::AlterView:  { AlterViewOps o;  if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::DropView:   { DropViewOps o; if (!r.i32(o.viewTopicId)) return std::unexpected(DecodeError::Truncated); return o; }
    }
    return std::unexpected(DecodeError::UnknownKind);
}

} // namespace

EventKind kindOf(const Ops& ops) noexcept {
    return static_cast<EventKind>(ops.index() + 1);   // variant order == kind order (1..9)
}

std::expected<void, EncodeError> encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out) {
    out.clear();
    Writer w(out);
    w.u16(e.formatVer); w.u16(static_cast<uint16_t>(e.kind)); w.u64(e.objectId);
    w.u32(e.prevVersion); w.u32(e.newVersion); w.i64(e.timestampMs); w.u64(e.fingerprint);
    w.str16(e.objectName); w.str32(e.statement);
    w.i64(e.principal.sessionId); w.str16(e.principal.clientName); w.str16(e.principal.sourceAddress);
    w.str16(e.principal.authenticated); w.u64(e.principal.requestId.hi); w.u64(e.principal.requestId.lo);
    w.u64(e.principal.requestFingerprint);
    w.u8(e.replayedFrom ? 1 : 0);
    if (e.replayedFrom) { w.str16(e.replayedFrom->clusterId); w.i64(e.replayedFrom->offset); w.str16(e.replayedFrom->principal); }
    const std::size_t lenAt = w.size();
    w.u32(0);
    putOps(w, e.ops);
    w.patchLen32(lenAt);
    if (const auto err = w.error()) {   // never leave a truncated, undecodable prefix behind
        out.clear();
        return std::unexpected(*err);
    }
    return {};
}

std::expected<std::vector<uint8_t>, EncodeError> encode(const CatalogEvent& e) {
    std::vector<uint8_t> b;
    if (auto r = encodeInto(e, b); !r) return std::unexpected(r.error());
    return b;
}

std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept {
    try {
        Reader r(bytes);
        CatalogEvent e;
        uint16_t kind;
        if (!r.u16(e.formatVer)) return std::unexpected(DecodeError::Truncated);
        if (e.formatVer != kFormatV1) return std::unexpected(DecodeError::UnknownFormatVer);
        if (!r.u16(kind)) return std::unexpected(DecodeError::Truncated);
        if (kind >= kReservedKindFirst && kind <= kReservedKindLast) return std::unexpected(DecodeError::ReservedKind);
        if (kind < 1 || kind > 9) return std::unexpected(DecodeError::UnknownKind);
        e.kind = static_cast<EventKind>(kind);
        if (!r.u64(e.objectId) || !r.u32(e.prevVersion) || !r.u32(e.newVersion) || !r.i64(e.timestampMs) ||
            !r.u64(e.fingerprint) || !r.str16(e.objectName) || !r.str32(e.statement) ||
            !r.i64(e.principal.sessionId) || !r.str16(e.principal.clientName) ||
            !r.str16(e.principal.sourceAddress) || !r.str16(e.principal.authenticated) ||
            !r.u64(e.principal.requestId.hi) || !r.u64(e.principal.requestId.lo) || !r.u64(e.principal.requestFingerprint))
            return std::unexpected(DecodeError::Truncated);
        uint8_t hasReplay; if (!r.u8(hasReplay)) return std::unexpected(DecodeError::Truncated);
        if (hasReplay > 1) return std::unexpected(DecodeError::BadEnum);
        if (hasReplay) {
            ReplayedFrom rf;
            if (!r.str16(rf.clusterId) || !r.i64(rf.offset) || !r.str16(rf.principal)) return std::unexpected(DecodeError::Truncated);
            e.replayedFrom = std::move(rf);
        }
        uint32_t opsLen; Reader ops(std::span<const uint8_t>{});
        if (!r.u32(opsLen) || !r.sub(opsLen, ops)) return std::unexpected(DecodeError::Truncated);
        auto o = getOps(ops, e.kind);
        if (!o) return std::unexpected(o.error());
        if (ops.rem() != 0 || r.rem() != 0) return std::unexpected(DecodeError::TrailingBytes);
        e.ops = std::move(*o);
        return e;
    } catch (const std::bad_alloc&) {
        return std::unexpected(DecodeError::Truncated);   // a length the process cannot hold is never trusted
    }
}

std::string_view kindName(EventKind k) noexcept {
    switch (k) {
    case EventKind::CreateTopic: return "CreateTopic";       case EventKind::DropTopic: return "DropTopic";
    case EventKind::AlterTopicConfig: return "AlterTopicConfig"; case EventKind::CreateSchema: return "CreateSchema";
    case EventKind::AlterSchema: return "AlterSchema";       case EventKind::SetCompatibility: return "SetCompatibility";
    case EventKind::CreateView: return "CreateView";         case EventKind::AlterView: return "AlterView";
    case EventKind::DropView: return "DropView";
    }
    return "?";
}
std::string_view decodeErrorName(DecodeError e) noexcept {
    switch (e) {
    case DecodeError::Truncated: return "Truncated"; case DecodeError::UnknownFormatVer: return "UnknownFormatVer";
    case DecodeError::ReservedKind: return "ReservedKind"; case DecodeError::UnknownKind: return "UnknownKind";
    case DecodeError::KindOpsMismatch: return "KindOpsMismatch"; case DecodeError::BadEnum: return "BadEnum";
    case DecodeError::TrailingBytes: return "TrailingBytes";
    }
    return "?";
}

std::string_view encodeErrorName(EncodeError e) noexcept {
    switch (e) {
    case EncodeError::FieldTooLong: return "FieldTooLong"; case EncodeError::TooManyElements: return "TooManyElements";
    }
    return "?";
}

namespace detail_testing {
std::size_t createTopicOpsSize(const CreateTopicOps& o) {
    std::vector<uint8_t> b; Writer w(b); putOps(w, Ops{o}); return b.size() - 1;
}
}

} // namespace rune::catalog
