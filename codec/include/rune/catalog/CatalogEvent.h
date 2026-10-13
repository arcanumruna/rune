#pragma once
// rune/catalog/CatalogEvent.h — Catalog history (spec §4): one event per catalog
// change, stored as one record in __catalog.  The formatVer-1 layout below is
// FROZEN: a change ships under a new formatVer, and old events keep decoding
// and applying under their own rules (spec §6).
//
// formatVer-1 layout (all little-endian):
//   u16 formatVer | u16 kind | u64 objectId | u32 prevVersion | u32 newVersion |
//   i64 timestampMs | u64 fingerprint |
//   str16 objectName | str32 statement |
//   principal: i64 sessionId, str16 clientName, str16 sourceAddress,
//              str16 authenticated, u64 requestIdHi, u64 requestIdLo,
//              u64 requestFingerprint |
//   u8 hasReplayedFrom [str16 clusterId, i64 offset, str16 principal] |
//   bytes32 ops  = u8 opsTag (== kind) + the kind's body (see CatalogEvent.cpp)
// str16 = u16 length + bytes; str32/bytes32 = u32 length + bytes.
#include <rune/codec/AlterOps.h>
#include <rune/codec/PrimType.h>
#include <rune/protocol/CatalogTypes.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::catalog {

enum class EventKind : uint16_t {
    CreateTopic = 1, DropTopic = 2, AlterTopicConfig = 3,
    CreateSchema = 4, AlterSchema = 5, SetCompatibility = 6,
    CreateView = 7, AlterView = 8, DropView = 9,
};
inline constexpr uint16_t kFormatV1          = 1;
inline constexpr uint16_t kCurrentFormatVer  = kFormatV1;
/// Reserved for the multi-tenancy design (tenants + ACLs).  formatVer 1 refuses them.
inline constexpr uint16_t kReservedKindFirst = 32;
inline constexpr uint16_t kReservedKindLast  = 63;

enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

/// Every TopicSpec property a CreateTopic fixes — the ONE definition lives in rune-protocol
/// (F32): the wire, the client and rune-ql use the same struct and the same defaults.
using TopicConfig   = rune::protocol::TopicConfig;
/// The client's id of the logical DDL (F14, ruling 9): 128-bit, stable across retries.
using RequestId     = rune::protocol::RequestId;
using RequestIdHash = rune::protocol::RequestIdHash;

/// AlterTopicConfig keys (ruling 5).  Append-only.
enum class ConfigKey : uint8_t {
    RetentionMs = 1, RetentionBytes = 2, DeleteRetentionMs = 3,
    MinCleanableDirtyRatioPpm = 4, MinCompactionLagMs = 5, MaxCompactionLagMs = 6,
};
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
};
struct DropTopicOps        { int32_t topicId = -1; };
struct AlterTopicConfigOps { std::vector<ConfigChange> changes; };
struct CreateSchemaOps     { SchemaDef schema; };
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
struct AlterSchemaOps      { std::vector<ResolvedAlterOp> ops; };   // resolved (F20) — never encodeAlterOps names
struct SetCompatibilityOps { rune::codec::CompatMode mode = rune::codec::CompatMode::Backward; };
struct CreateViewOps       { ViewDef view; };
struct AlterViewOps        { ViewDef view; };                          // a FRESH sink (ruling 4)
struct DropViewOps         { int32_t viewTopicId = -1; };

using Ops = std::variant<CreateTopicOps, DropTopicOps, AlterTopicConfigOps, CreateSchemaOps,
                         AlterSchemaOps, SetCompatibilityOps, CreateViewOps, AlterViewOps, DropViewOps>;

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
};
/// Set only on events written by `rune catalog replay` (spec §7).
struct ReplayedFrom {
    std::string clusterId;
    int64_t     offset = -1;
    std::string principal;
};

struct CatalogEvent {
    uint16_t                    formatVer   = kCurrentFormatVer;
    EventKind                   kind        = EventKind::CreateTopic;
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
};

enum class DecodeError : uint8_t {
    Truncated, UnknownFormatVer, ReservedKind, UnknownKind, KindOpsMismatch, BadEnum, TrailingBytes,
};

[[nodiscard]] EventKind kindOf(const Ops& ops) noexcept;
void encodeInto(const CatalogEvent& e, std::vector<uint8_t>& out);
[[nodiscard]] std::vector<uint8_t> encode(const CatalogEvent& e);
[[nodiscard]] std::expected<CatalogEvent, DecodeError> decode(std::span<const uint8_t> bytes) noexcept;
[[nodiscard]] std::string_view kindName(EventKind k) noexcept;
[[nodiscard]] std::string_view decodeErrorName(DecodeError e) noexcept;

namespace detail_testing {
/// Encoded size of a CreateTopicOps body (no tag byte).  Test-only.
[[nodiscard]] std::size_t createTopicOpsSize(const CreateTopicOps& o);
}

} // namespace rune::catalog
