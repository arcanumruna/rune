#include <rune/catalog/CatalogEvent.h>
#include "Wire.h"

#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace rune::catalog {
namespace {

using wire::Reader;
using wire::Writer;

static_assert(kMaxStr16Bytes == 0xFFFF && kMaxStr32Bytes == 0xFFFF'FFFF && kMaxListCount == 0xFFFF,
              "formatVer-1 wire widths are frozen: u16 / u32 length prefixes, u16 list counts");

// ── THE validation (formatVer 1) ───────────────────────────────────────────
// One set of value rules, run by BOTH directions: encodeInto refuses exactly what decode
// refuses, so no event the decoder would reject can ever be appended to __catalog.
// (Byte-level refusals — Truncated, TrailingBytes, a bool byte other than 0/1 — have no
// in-memory counterpart: the encoder cannot produce them.)
enum class Violation : uint8_t { UnknownFormatVer, ReservedKind, UnknownKind, KindOpsMismatch, BadEnum };

constexpr bool validPersistence(Persistence p) noexcept { return static_cast<uint8_t>(p) <= 1; }
constexpr bool validCleanupPolicy(uint8_t v) noexcept  { return v <= 2; }   // Delete, Compact, CompactDelete
constexpr bool validDurability(uint8_t v) noexcept     { return v <= 1; }   // Majority, AllInSync
constexpr bool validDurabilityTier(uint8_t v) noexcept { return v <= 2; }   // Leader, Majority, Strict
constexpr bool validConfigKey(ConfigKey k) noexcept {
    const auto v = static_cast<uint8_t>(k);
    return v >= static_cast<uint8_t>(ConfigKey::RetentionMs) && v <= static_cast<uint8_t>(ConfigKey::MaxCompactionLagMs);
}
constexpr bool validAlterOpType(rune::codec::AlterOpType t) noexcept {
    const auto v = static_cast<uint8_t>(t);
    return v >= static_cast<uint8_t>(rune::codec::AlterOpType::Add) &&
           v <= static_cast<uint8_t>(rune::codec::AlterOpType::SetCompatibility);
}
constexpr bool validPrimType(rune::codec::PrimType t) noexcept {
    return static_cast<uint8_t>(t) <= static_cast<uint8_t>(rune::codec::PrimType::FixedStr);
}
constexpr bool validCompatMode(rune::codec::CompatMode m) noexcept {
    return static_cast<uint8_t>(m) <= static_cast<uint8_t>(rune::codec::CompatMode::Forward);
}

std::optional<Violation> checkFormatVer(uint16_t v) noexcept {
    if (v != kFormatV1) return Violation::UnknownFormatVer;
    return std::nullopt;
}
std::optional<Violation> checkKind(uint16_t k) noexcept {
    if (k >= kReservedKindFirst && k <= kReservedKindLast) return Violation::ReservedKind;
    if (k < static_cast<uint16_t>(EventKind::CreateTopic) || k > static_cast<uint16_t>(EventKind::DropView))
        return Violation::UnknownKind;
    return std::nullopt;
}
bool validConfig(const TopicConfig& c) noexcept {
    return validCleanupPolicy(c.cleanupPolicy) && validDurability(c.durability) && validDurabilityTier(c.durabilityTier);
}
bool validOps(const Ops& ops) noexcept {
    return std::visit([](const auto& o) -> bool {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateTopicOps>) {
            return validPersistence(o.persistence) && validConfig(o.config);
        } else if constexpr (std::is_same_v<T, AlterTopicConfigOps>) {
            for (const auto& c : o.changes) if (!validConfigKey(c.key)) return false;
            return true;
        } else if constexpr (std::is_same_v<T, AlterSchemaOps>) {
            for (const auto& a : o.ops)
                if (!validAlterOpType(a.type) || !validPrimType(a.primType) || !validCompatMode(a.mode)) return false;
            return true;
        } else if constexpr (std::is_same_v<T, SetCompatibilityOps>) {
            return validCompatMode(o.mode);
        } else if constexpr (std::is_same_v<T, CreateViewOps> || std::is_same_v<T, AlterViewOps>) {
            return validPersistence(o.view.persistence);
        } else {   // DropTopicOps, CreateSchemaOps, DropViewOps: no enum fields
            return true;
        }
    }, ops);
}
std::optional<Violation> validate(const CatalogEvent& e) noexcept {
    if (auto v = checkFormatVer(e.formatVer)) return v;
    if (auto v = checkKind(static_cast<uint16_t>(e.kind))) return v;
    if (e.kind != kindOf(e.ops)) return Violation::KindOpsMismatch;
    if (!validOps(e.ops)) return Violation::BadEnum;
    return std::nullopt;
}
DecodeError asDecodeError(Violation v) noexcept {
    switch (v) {
    case Violation::UnknownFormatVer: return DecodeError::UnknownFormatVer;
    case Violation::ReservedKind:     return DecodeError::ReservedKind;
    case Violation::UnknownKind:      return DecodeError::UnknownKind;
    case Violation::KindOpsMismatch:  return DecodeError::KindOpsMismatch;
    case Violation::BadEnum:          return DecodeError::BadEnum;
    }
    return DecodeError::BadEnum;
}
EncodeError asEncodeError(Violation v) noexcept {
    switch (v) {
    case Violation::UnknownFormatVer: return EncodeError::UnknownFormatVer;
    case Violation::ReservedKind:     return EncodeError::ReservedKind;
    case Violation::UnknownKind:      return EncodeError::UnknownKind;
    case Violation::KindOpsMismatch:  return EncodeError::KindOpsMismatch;
    case Violation::BadEnum:          return EncodeError::BadEnum;
    }
    return EncodeError::BadEnum;
}

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
    w.i32(v.viewTopicId); w.i32(v.sinkPartitions); w.e8(v.persistence);
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
            w.e8(o.persistence); putConfig(w, o.config);
            w.u8(o.schema ? 1 : 0);
            if (o.schema) putSchema(w, *o.schema);
        } else if constexpr (std::is_same_v<T, DropTopicOps>) {
            w.i32(o.topicId);
        } else if constexpr (std::is_same_v<T, AlterTopicConfigOps>) {
            w.count16(o.changes.size());
            for (const auto& c : o.changes) { w.e8(c.key); w.i64(c.value); }
        } else if constexpr (std::is_same_v<T, CreateSchemaOps>) {
            putSchema(w, o.schema);
        } else if constexpr (std::is_same_v<T, AlterSchemaOps>) {
            w.count16(o.ops.size());
            for (const auto& a : o.ops) {   // u8 type, u16 tag, str16 name, u8 primType, u16 fixedLen, u8 mode
                w.e8(a.type); w.u16(a.tag); w.str16(a.name); w.e8(a.primType); w.u16(a.fixedLen); w.e8(a.mode);
            }
        } else if constexpr (std::is_same_v<T, SetCompatibilityOps>) {
            w.e8(o.mode);
        } else if constexpr (std::is_same_v<T, CreateViewOps> || std::is_same_v<T, AlterViewOps>) {
            putView(w, o.view);
        } else {   // DropViewOps
            w.i32(o.viewTopicId);
        }
    }, ops);
}

// ── body readers ────────────────────────────────────────────────────────────
// They only PARSE: enum bytes are loaded raw (Reader::e8) and judged afterwards by the
// shared validate(); a bool byte must be 0 or 1 (getBool) — the one byte-level value rule.
// The smallest encoding of one list element: a list's count is checked against the
// remaining bytes with these BEFORE the list is sized (a corrupt count never allocates).
constexpr std::size_t kMinSourceNameBytes      = 2;                  // str16 ""
constexpr std::size_t kConfigChangeBytes       = 1 + 8;              // u8 key, i64 value
constexpr std::size_t kMinResolvedAlterOpBytes = 1 + 2 + 2 + 1 + 2 + 1;   // type, tag, str16 "", prim, fixedLen, mode

std::optional<DecodeError> getBool(Reader& r, bool& b) {
    uint8_t v; if (!r.u8(v)) return DecodeError::Truncated;
    if (v > 1) return DecodeError::BadEnum;
    b = v == 1; return std::nullopt;
}
bool getConfig(Reader& r, TopicConfig& c) {
    return r.u8(c.cleanupPolicy) && r.i64(c.retentionMs) && r.i64(c.retentionBytes) && r.i64(c.deleteRetentionMs) &&
           r.i32(c.minCleanableDirtyRatioPpm) && r.i64(c.segmentRollBytes) && r.i64(c.minCompactionLagMs) &&
           r.i64(c.maxCompactionLagMs) && r.i64(c.retentionCheckMs) && r.i64(c.fileDeleteDelayMs) &&
           r.u8(c.durability) && r.u8(c.durabilityTier) && r.str16(c.coPartitionGroup) && r.str16(c.partitionKey);
}
std::optional<DecodeError> getSchema(Reader& r, SchemaDef& s) {
    if (!r.str16(s.keyField)) return DecodeError::Truncated;
    if (auto e = getBool(r, s.validate)) return e;
    if (!r.bytes32(s.descriptorBlob) || !r.u16(s.nextTag)) return DecodeError::Truncated;
    return std::nullopt;
}
std::optional<DecodeError> getView(Reader& r, ViewDef& v) {
    uint16_t n;
    if (!r.i32(v.viewTopicId) || !r.i32(v.sinkPartitions) || !r.e8(v.persistence) || !r.str16(v.keyField) || !r.u16(n))
        return DecodeError::Truncated;
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
        if (!r.i32(o.topicId) || !r.i32(o.partitions) || !r.i16(o.replicationFactor) || !r.e8(o.persistence) ||
            !getConfig(r, o.config))
            return std::unexpected(DecodeError::Truncated);
        bool has = false;
        if (auto e = getBool(r, has)) return std::unexpected(*e);
        if (has) { SchemaDef s; if (auto e = getSchema(r, s)) return std::unexpected(*e); o.schema = std::move(s); }
        return o;
    }
    case EventKind::DropTopic: { DropTopicOps o; if (!r.i32(o.topicId)) return std::unexpected(DecodeError::Truncated); return o; }
    case EventKind::AlterTopicConfig: {
        AlterTopicConfigOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        if (!r.canHold(n, kConfigChangeBytes)) return std::unexpected(DecodeError::Truncated);
        o.changes.resize(n);
        for (auto& c : o.changes) if (!r.e8(c.key) || !r.i64(c.value)) return std::unexpected(DecodeError::Truncated);
        return o;
    }
    case EventKind::CreateSchema: { CreateSchemaOps o; if (auto e = getSchema(r, o.schema)) return std::unexpected(*e); return o; }
    case EventKind::AlterSchema: {
        AlterSchemaOps o; uint16_t n; if (!r.u16(n)) return std::unexpected(DecodeError::Truncated);
        if (!r.canHold(n, kMinResolvedAlterOpBytes)) return std::unexpected(DecodeError::Truncated);
        o.ops.resize(n);
        for (auto& a : o.ops)
            if (!r.e8(a.type) || !r.u16(a.tag) || !r.str16(a.name) || !r.e8(a.primType) || !r.u16(a.fixedLen) || !r.e8(a.mode))
                return std::unexpected(DecodeError::Truncated);
        return o;
    }
    case EventKind::SetCompatibility: {
        SetCompatibilityOps o; if (!r.e8(o.mode)) return std::unexpected(DecodeError::Truncated); return o;
    }
    case EventKind::CreateView: { CreateViewOps o; if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::AlterView:  { AlterViewOps o;  if (auto e = getView(r, o.view)) return std::unexpected(*e); return o; }
    case EventKind::DropView:   { DropViewOps o; if (!r.i32(o.viewTopicId)) return std::unexpected(DecodeError::Truncated); return o; }
    }
    return std::unexpected(DecodeError::UnknownKind);   // unreachable: the kind was validated first
}

} // namespace

EventKind kindOf(const Ops& ops) noexcept {
    return static_cast<EventKind>(ops.index() + 1);   // variant order == kind order (static_asserts in the header)
}

std::expected<void, EncodeError> encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out) {
    out.clear();
    if (auto v = validate(e)) return std::unexpected(asEncodeError(*v));   // the decoder's own rules
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
        // formatVer and kind decide how the rest is parsed, so they are judged (by the
        // shared rules) before anything else is read.
        if (!r.u16(e.formatVer)) return std::unexpected(DecodeError::Truncated);
        if (auto v = checkFormatVer(e.formatVer)) return std::unexpected(asDecodeError(*v));
        if (!r.u16(kind)) return std::unexpected(DecodeError::Truncated);
        if (auto v = checkKind(kind)) return std::unexpected(asDecodeError(*v));
        e.kind = static_cast<EventKind>(kind);
        if (!r.u64(e.objectId) || !r.u32(e.prevVersion) || !r.u32(e.newVersion) || !r.i64(e.timestampMs) ||
            !r.u64(e.fingerprint) || !r.str16(e.objectName) || !r.str32(e.statement) ||
            !r.i64(e.principal.sessionId) || !r.str16(e.principal.clientName) ||
            !r.str16(e.principal.sourceAddress) || !r.str16(e.principal.authenticated) ||
            !r.u64(e.principal.requestId.hi) || !r.u64(e.principal.requestId.lo) || !r.u64(e.principal.requestFingerprint))
            return std::unexpected(DecodeError::Truncated);
        bool hasReplay = false;
        if (auto err = getBool(r, hasReplay)) return std::unexpected(*err);
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
        if (auto v = validate(e)) return std::unexpected(asDecodeError(*v));   // the encoder's own rules
        return e;
    } catch (const std::bad_alloc&) {
        // Every length is bounded by the input before anything is sized, so this is the
        // process running out of memory — reported as such, never as corrupt bytes.
        return std::unexpected(DecodeError::OutOfMemory);
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
    case DecodeError::TrailingBytes: return "TrailingBytes"; case DecodeError::OutOfMemory: return "OutOfMemory";
    }
    return "?";
}

std::string_view encodeErrorName(EncodeError e) noexcept {
    switch (e) {
    case EncodeError::FieldTooLong: return "FieldTooLong"; case EncodeError::TooManyElements: return "TooManyElements";
    case EncodeError::UnknownFormatVer: return "UnknownFormatVer"; case EncodeError::ReservedKind: return "ReservedKind";
    case EncodeError::UnknownKind: return "UnknownKind"; case EncodeError::KindOpsMismatch: return "KindOpsMismatch";
    case EncodeError::BadEnum: return "BadEnum";
    }
    return "?";
}

} // namespace rune::catalog
