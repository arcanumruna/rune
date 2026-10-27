#pragma once
// rune/catalog/CatalogEvent.h — Catalog history (spec §4): one event per catalog
// change, stored as one record in __catalog.  The formatVer-1 layout below is
// FROZEN: a change ships under a new formatVer, and old events keep decoding
// and applying under their own rules (spec §6).
//
// formatVer 2 (namespaces, plan D2) is formatVer 1 with `u32 nsId` inserted right after `u16 kind`; the rest is
// unchanged.  For object kinds (1..9) nsId is the namespace of the object; for namespace kinds (32..35) it is the
// subject namespace.  A formatVer-1 event decodes with nsId = 0 and cannot carry another.  The broker writes only
// formatVer 2 (kCurrentFormatVer).
//
// formatVer-1 layout (all little-endian, written and read byte-wise — host-endian independent):
//   u16 formatVer | u16 kind | u64 objectId | u32 prevVersion | u32 newVersion |
//   i64 timestampMs | u64 fingerprint |
//   str16 objectName | str32 statement |
//   principal: i64 sessionId, str16 clientName, str16 sourceAddress,
//              str16 authenticated, u64 requestIdHi, u64 requestIdLo,
//              u64 requestFingerprint |
//   u8 hasReplayedFrom [str16 clusterId, i64 offset, str16 principal] |
//   bytes32 ops  = u8 opsTag (== kind) + the kind's body (see CatalogEvent.cpp)
// str16 = u16 length + bytes; str32/bytes32 = u32 length + bytes.
// Golden byte vectors for every kind pin this layout (tests/catalog/CatalogEventTest.cpp).
#include <rune/codec/AlterOps.h>
#include <rune/codec/PrimType.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/NamespaceLimits.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace rune::catalog {

enum class EventKind : uint16_t {
    CreateTopic = 1, DropTopic = 2, AlterTopicConfig = 3,
    CreateSchema = 4, AlterSchema = 5, SetCompatibility = 6,
    CreateView = 7, AlterView = 8, DropView = 9,
    // formatVer 2 (namespaces): the subject namespace is the event's nsId.
    NamespaceCreated = 32, NamespaceLimitsSet = 33, NamespaceStateChanged = 34, NamespaceDeleted = 35,
};
inline constexpr uint16_t kFormatV1          = 1;
inline constexpr uint16_t kFormatV2          = 2;
inline constexpr uint16_t kCurrentFormatVer  = kFormatV2;
/// Kinds 32..63 are reserved: formatVer 1 refuses all of them, formatVer 2 accepts 32..35 and refuses 36..63.
inline constexpr uint16_t kReservedKindFirst = 32;
inline constexpr uint16_t kReservedKindLast  = 63;
inline constexpr uint16_t kLastNamespaceKind = 35;

enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

/// Every TopicSpec property a CreateTopic fixes — the ONE definition lives in rune-protocol
/// (F32): the wire, the client and rune-ql use the same struct and the same defaults.
using TopicConfig   = rune::protocol::TopicConfig;
/// The client's id of the logical DDL (F14, ruling 9): 128-bit, stable across retries.
using RequestId     = rune::protocol::RequestId;
using RequestIdHash = rune::protocol::RequestIdHash;

/// AlterTopicConfig keys (ruling 5): the ONE definition lives in rune-protocol
/// CatalogTypes.h (shared with the wire).  Append-only.
using ConfigKey     = rune::protocol::ConfigKey;
struct ConfigChange { ConfigKey key; int64_t value; bool operator==(const ConfigChange&) const = default; };

struct SchemaDef {
    std::string          keyField;
    bool                 validate = false;
    std::vector<uint8_t> descriptorBlob;     // rune::codec::serialize(Descriptor) — the initial EVOLVING blob
    uint16_t             nextTag  = 0;       // = descriptor field count
    bool operator==(const SchemaDef&) const = default;
};

struct ViewDef {
    int32_t                  viewTopicId    = -1;   // the view's sink topic id (dense, from the catalog)
    int32_t                  sinkPartitions = 1;
    Persistence              persistence    = Persistence::Disk;
    std::string              keyField;
    std::vector<std::string> sourceNames;           // resolved source topic NAMES, in plan order
    std::vector<uint8_t>     normalizedPlanBlob;    // authored plan, source topicId := ordinal (ruling 11)
    std::vector<uint8_t>     compiledPlanBlob;      // rune::view::serialize(CompiledViewPlan) — embeds ids, NOT fingerprinted
    std::vector<uint8_t>     outputDescriptorBlob;  // rune::codec::serialize(output Descriptor)
    bool operator==(const ViewDef&) const = default;
};

struct CreateTopicOps {
    int32_t                  topicId           = -1;    // dense id, from CatalogState::nextTopicId()
    int32_t                  partitions        = 1;
    int16_t                  replicationFactor = 0;     // 0 = every broker
    Persistence              persistence       = Persistence::Disk;
    TopicConfig              config;
    std::optional<SchemaDef> schema;                    // nullopt = untyped topic
    bool operator==(const CreateTopicOps&) const = default;
};
struct DropTopicOps        { int32_t topicId = -1;                bool operator==(const DropTopicOps&) const = default; };
struct AlterTopicConfigOps { std::vector<ConfigChange> changes;   bool operator==(const AlterTopicConfigOps&) const = default; };
struct CreateSchemaOps     { SchemaDef schema;                    bool operator==(const CreateSchemaOps&) const = default; };
/// One AlterSchema operation RESOLVED against the evolving descriptor (spec §4, F20):
/// every field is named by its durable tag (FieldDescriptor::id), never by name.
struct ResolvedAlterOp {
    rune::codec::AlterOpType type     = rune::codec::AlterOpType::Add;
    uint16_t                 tag      = 0;     // Add: the tag it is assigned; Drop / Rename: the field's tag; SetCompat: 0
    std::string              name;            // Add: the new field's name; Rename: the NEW name; otherwise empty
    rune::codec::PrimType    primType = rune::codec::PrimType::Int32;          // Add only
    uint16_t                 fixedLen = 0;                                     // Add only (FixedStr width)
    rune::codec::CompatMode  mode     = rune::codec::CompatMode::Backward;     // SetCompatibility only
    bool operator==(const ResolvedAlterOp&) const = default;
};
struct AlterSchemaOps {   // resolved (F20) — never encodeAlterOps names
    std::vector<ResolvedAlterOp> ops;
    bool operator==(const AlterSchemaOps&) const = default;
};
struct SetCompatibilityOps {
    rune::codec::CompatMode mode = rune::codec::CompatMode::Backward;
    bool operator==(const SetCompatibilityOps&) const = default;
};
struct CreateViewOps { ViewDef view;            bool operator==(const CreateViewOps&) const = default; };
struct AlterViewOps  { ViewDef view;            bool operator==(const AlterViewOps&) const = default; };   // a FRESH sink (ruling 4)
struct DropViewOps   { int32_t viewTopicId = -1; bool operator==(const DropViewOps&) const = default; };

/// A namespace's lifecycle tag as carried on the wire (NamespaceStateChanged's target; the registry's NsState
/// variant keeps the rich state).  Active is the only state a namespace is created in.
enum class NsStateTag : uint8_t { Active = 0, SuspendedReadOnly = 1, SuspendedRejectAll = 2, Deleting = 3 };

/// formatVer 2.  The namespace is the event's nsId; `name` is its (valid, non-"default") name.
struct NamespaceCreatedOps      { std::string name; rune::protocol::NamespaceLimits limits;
                                  bool operator==(const NamespaceCreatedOps&) const = default; };
/// The FULL replacement limits record (the leader resolves omitted fields, plan §5.1).
struct NamespaceLimitsSetOps    { rune::protocol::NamespaceLimits limits;
                                  bool operator==(const NamespaceLimitsSetOps&) const = default; };
struct NamespaceStateChangedOps { NsStateTag to = NsStateTag::Active;
                                  bool operator==(const NamespaceStateChangedOps&) const = default; };
struct NamespaceDeletedOps      { bool operator==(const NamespaceDeletedOps&) const = default; };

/// The body of each event kind.  Alternative order is NOT the kind number any more (kinds 10..31 are unused):
/// kKindOfAlt / altOf below are the one mapping, and the ops tag byte on the wire is the KIND.
using Ops = std::variant<CreateTopicOps, DropTopicOps, AlterTopicConfigOps, CreateSchemaOps,
                         AlterSchemaOps, SetCompatibilityOps, CreateViewOps, AlterViewOps, DropViewOps,
                         NamespaceCreatedOps, NamespaceLimitsSetOps, NamespaceStateChangedOps, NamespaceDeletedOps>;

inline constexpr std::array<EventKind, 13> kKindOfAlt = {
    EventKind::CreateTopic, EventKind::DropTopic, EventKind::AlterTopicConfig, EventKind::CreateSchema,
    EventKind::AlterSchema, EventKind::SetCompatibility, EventKind::CreateView, EventKind::AlterView,
    EventKind::DropView, EventKind::NamespaceCreated, EventKind::NamespaceLimitsSet,
    EventKind::NamespaceStateChanged, EventKind::NamespaceDeleted};

/// The Ops alternative index of `k`; -1 for a kind that has none (10..31, 36 and up).
[[nodiscard]] constexpr int altOf(EventKind k) noexcept {
    for (std::size_t i = 0; i < kKindOfAlt.size(); ++i)
        if (kKindOfAlt[i] == k) return static_cast<int>(i);
    return -1;
}

namespace detail {
template <EventKind K, class T>
inline constexpr bool kOpsAt = std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(altOf(K)), Ops>, T>;
constexpr bool kindTableBijective() noexcept {
    for (std::size_t i = 0; i < kKindOfAlt.size(); ++i) {
        if (altOf(kKindOfAlt[i]) != static_cast<int>(i)) return false;       // a kind appears twice
        for (std::size_t j = 0; j < i; ++j) if (kKindOfAlt[i] == kKindOfAlt[j]) return false;
    }
    return true;
}
}
static_assert(std::variant_size_v<Ops> == 13 && kKindOfAlt.size() == 13, "one Ops alternative per EventKind 1..9, 32..35");
static_assert(detail::kindTableBijective(), "kKindOfAlt must be a bijection onto the Ops alternatives");
static_assert(detail::kOpsAt<EventKind::CreateTopic, CreateTopicOps> &&
              detail::kOpsAt<EventKind::DropTopic, DropTopicOps> &&
              detail::kOpsAt<EventKind::AlterTopicConfig, AlterTopicConfigOps> &&
              detail::kOpsAt<EventKind::CreateSchema, CreateSchemaOps> &&
              detail::kOpsAt<EventKind::AlterSchema, AlterSchemaOps> &&
              detail::kOpsAt<EventKind::SetCompatibility, SetCompatibilityOps> &&
              detail::kOpsAt<EventKind::CreateView, CreateViewOps> &&
              detail::kOpsAt<EventKind::AlterView, AlterViewOps> &&
              detail::kOpsAt<EventKind::DropView, DropViewOps> &&
              detail::kOpsAt<EventKind::NamespaceCreated, NamespaceCreatedOps> &&
              detail::kOpsAt<EventKind::NamespaceLimitsSet, NamespaceLimitsSetOps> &&
              detail::kOpsAt<EventKind::NamespaceStateChanged, NamespaceStateChangedOps> &&
              detail::kOpsAt<EventKind::NamespaceDeleted, NamespaceDeletedOps>,
              "kKindOfAlt must list each kind at the index of its Ops alternative");

/// Until authentication exists: session id, client name and source address.  Afterwards
/// `authenticated` carries the principal (same field, no layout change).  `requestId` is
/// the client's id of the logical DDL, stable across retries (ruling 9, F14); the leader's
/// `requestFingerprint` (FNV-1a-64 over the request's statement and operation bytes, Task 9)
/// lets a retry be told apart from a different statement that reuses the id.
struct Principal {
    int64_t     sessionId = 0;
    std::string clientName;
    std::string sourceAddress;
    std::string authenticated;
    RequestId   requestId{};               // {0, 0} = none (a pre-v8 client)
    uint64_t    requestFingerprint = 0;
    bool operator==(const Principal&) const = default;
};
/// Set only on events written by `rune catalog replay` (spec §7).
struct ReplayedFrom {
    std::string clusterId;
    int64_t     offset = -1;
    std::string principal;
    bool operator==(const ReplayedFrom&) const = default;
};

struct CatalogEvent {
    uint16_t                    formatVer   = kCurrentFormatVer;
    EventKind                   kind        = EventKind::CreateTopic;
    uint32_t                    nsId        = 0;   // formatVer 2: the object's / subject namespace; a v1 event is 0
    uint64_t                    objectId    = 0;
    std::string                 objectName;
    uint32_t                    prevVersion = 0;
    uint32_t                    newVersion  = 0;
    std::string                 statement;
    Ops                         ops;
    Principal                   principal;
    std::optional<ReplayedFrom> replayedFrom;
    int64_t                     timestampMs = 0;   // informational; the OFFSET is the order
    uint64_t                    fingerprint = 0;   // of the object's resulting state
    bool operator==(const CatalogEvent&) const = default;
};

enum class DecodeError : uint8_t {
    Truncated, UnknownFormatVer, ReservedKind, UnknownKind, KindOpsMismatch, BadEnum, TrailingBytes,
    /// The process could not allocate the decoded event (std::bad_alloc).  Not a statement
    /// about the bytes: every length is bounded by the input before anything is sized.
    OutOfMemory,
};

/// Wire widths (formatVer 1): a str16 holds at most kMaxStr16Bytes, a str32/bytes32 (and the
/// whole ops blob) at most kMaxStr32Bytes, a list at most kMaxListCount elements.
inline constexpr std::size_t kMaxStr16Bytes = 0xFFFF;
inline constexpr std::size_t kMaxStr32Bytes = 0xFFFF'FFFF;
inline constexpr std::size_t kMaxListCount  = 0xFFFF;

/// Why an event cannot be encoded.  Encoding refuses EXACTLY what decoding refuses: the
/// value rules (formatVer, kind, kind/ops agreement, every enum range) are one shared
/// validation run by both directions, and no length or count is ever truncated — so no
/// undecodable event is ever produced.
enum class EncodeError : uint8_t {
    FieldTooLong,      // a string / blob (or the ops blob) exceeds its length prefix's width
    TooManyElements,   // a list exceeds its u16 count
    UnknownFormatVer,  // formatVer not 1 or 2               (decode: UnknownFormatVer)
    ReservedKind,      // v1: kind in [32, 63]; v2: [36, 63] (decode: ReservedKind)
    UnknownKind,       // kind not 1..9 and not reserved    (decode: UnknownKind)
    KindOpsMismatch,   // kind != kindOf(ops)               (decode: KindOpsMismatch)
    BadEnum,           // an enum field outside its range, or a v1 event with nsId != 0 (decode: BadEnum)
};

[[nodiscard]] EventKind kindOf(const Ops& ops) noexcept;
/// Clears `out` and encodes into it, reusing its capacity.  On error `out` is left empty.
[[nodiscard]] std::expected<void, EncodeError> encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out);
[[nodiscard]] std::expected<std::vector<uint8_t>, EncodeError> encode(const CatalogEvent& e);
/// Bounds-checked decode of one formatVer-1 or -2 event; the same validation as encode, plus the
/// byte-level refusals (Truncated, TrailingBytes, a bool byte other than 0/1 → BadEnum).
[[nodiscard]] std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept;
[[nodiscard]] std::string_view kindName(EventKind k) noexcept;
[[nodiscard]] std::string_view decodeErrorName(DecodeError e) noexcept;
[[nodiscard]] std::string_view encodeErrorName(EncodeError e) noexcept;

} // namespace rune::catalog
