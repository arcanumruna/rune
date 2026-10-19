#pragma once
// rune/security/SecurityEvent.h — the __security event (authentication spec §3, plan D1/D2/D3):
// one fixed-layout, versioned record per event.  The 36-byte header is byte-identical to
// __catalog's (formatVer u16, kind u16, objectId u64, prevVersion u32, newVersion u32,
// timestampMs i64, fingerprint u64), then
//   objectName (u16 len + bytes), statement (u32 len + bytes; REDACTED — never a password),
//   the principal and replayedFrom exactly as __catalog v1 (shared wire code),
//   opsLen u32, then tag u8 (= the KIND, exactly as __catalog frames its ops) and the body (the event's tail).
// Verifier body: saltLen u8 (16..64), salt, iterations u32 (4096..1e6), storedKey[64], serverKey[64].
// Grant / Revoke body: grantee u8, op u8, type u8, patternKind u8, name u16+bytes.
// AlterUser body: changeTag u8 (= UserChangeKind, 0-based: Add 0, Drop 1, Set 2, Disable 3, Enable 4) + (DropPassword: index u8;
// AddPassword: verifier; SetPassword: verifier + terminate u8 0|1; Disable / Enable: empty).
// Bootstrap body: verifier, fakeSaltSeed[32] (D8: the cluster-wide fake-salt key, set once).
// GrantRole / RevokeRole body: roleId u64, roleName u16+bytes.  Empty bodies are zero bytes.
// FROZEN with formatVer 1: a change ships as formatVer 2.
#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/SecurityTypes.h>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::security {

enum class EventKind : uint16_t { CreateUser = 1, AlterUser = 2, DropUser = 3, CreateRole = 4, DropRole = 5,
                                  Grant = 6, Revoke = 7, GrantRole = 8, RevokeRole = 9, Bootstrap = 10 };
inline constexpr uint16_t kFormatV1 = 1, kCurrentFormatVer = kFormatV1;
// The frozen enums are defined ONCE, in rune-protocol (SecurityTypes.h); the wire, this event and rune-ql share them.
using Operation    = rune::protocol::SecurityOperation;      // Read 0 … Admin 6
using ResourceType = rune::protocol::SecurityResourceType;   // Cluster 0 … TransactionalId 4
using PatternKind  = rune::protocol::SecurityPatternKind;    // Exact 0, Prefix 1
using GranteeKind  = rune::protocol::SecurityGranteeKind;    // User 0, Role 1
[[nodiscard]] constexpr uint8_t opBit(Operation o) noexcept { return static_cast<uint8_t>(1u << static_cast<uint8_t>(o)); }

struct ResourcePattern { ResourceType type = ResourceType::Topic; PatternKind kind = PatternKind::Exact;
                         std::string name; auto operator<=>(const ResourcePattern&) const = default; };
struct GrantSpec { Operation op = Operation::Read; ResourcePattern resource;
                   auto operator<=>(const GrantSpec&) const = default; };

inline constexpr std::size_t kSaltMin = 16, kSaltMax = 64, kKeyBytes = 64;
inline constexpr uint32_t    kIterMin = 4096, kIterMax = 1'000'000;
struct ScramVerifier { std::vector<uint8_t> salt; uint32_t iterations = 0;
                       std::array<uint8_t, kKeyBytes> storedKey{}, serverKey{};
                       bool operator==(const ScramVerifier&) const = default; };   // SHA-512 only in v1

struct AddPassword { ScramVerifier verifier; bool operator==(const AddPassword&) const = default; };
struct DropPassword { uint8_t index = 1; bool operator==(const DropPassword&) const = default; };   // 1 = oldest (D6)
struct SetPassword { ScramVerifier verifier; bool terminateSessions = false;
                     bool operator==(const SetPassword&) const = default; };                         // D37
struct DisableUser { bool operator==(const DisableUser&) const = default; };
struct EnableUser  { bool operator==(const EnableUser&) const = default; };
using UserChange = std::variant<AddPassword, DropPassword, SetPassword, DisableUser, EnableUser>;

struct CreateUserOps { ScramVerifier verifier; bool operator==(const CreateUserOps&) const = default; };
struct AlterUserOps  { UserChange change; bool operator==(const AlterUserOps&) const = default; };
struct DropUserOps   { bool operator==(const DropUserOps&) const = default; };
struct CreateRoleOps { bool operator==(const CreateRoleOps&) const = default; };
struct DropRoleOps   { bool operator==(const DropRoleOps&) const = default; };
struct GrantOps  { GranteeKind grantee = GranteeKind::User; GrantSpec grant;
                   bool operator==(const GrantOps&) const = default; };    // object = the grantee
struct RevokeOps { GranteeKind grantee = GranteeKind::User; GrantSpec grant;
                   bool operator==(const RevokeOps&) const = default; };
struct GrantRoleOps  { uint64_t roleId = 0; std::string roleName;
                       bool operator==(const GrantRoleOps&) const = default; };     // object = the user
struct RevokeRoleOps { uint64_t roleId = 0; std::string roleName;
                       bool operator==(const RevokeRoleOps&) const = default; };
inline constexpr std::size_t kFakeSaltSeedBytes = 32;
struct BootstrapOps  { ScramVerifier verifier; std::array<uint8_t, kFakeSaltSeedBytes> fakeSaltSeed{};   // D3, D8
                       bool operator==(const BootstrapOps&) const = default; };

using Ops = std::variant<CreateUserOps, AlterUserOps, DropUserOps, CreateRoleOps, DropRoleOps,
                         GrantOps, RevokeOps, GrantRoleOps, RevokeRoleOps, BootstrapOps>;
static_assert(std::variant_size_v<Ops> == 10, "Ops variant order must equal EventKind order (kindOf = index + 1)");

using rune::catalog::Principal; using rune::catalog::ReplayedFrom; using rune::catalog::RequestId;

struct SecurityEvent {
    uint16_t formatVer = kCurrentFormatVer; EventKind kind = EventKind::CreateUser;
    uint64_t objectId = 0; std::string objectName; uint32_t prevVersion = 0, newVersion = 0;
    std::string statement;          // REDACTED (D10)
    Ops ops; Principal principal; std::optional<ReplayedFrom> replayedFrom;
    int64_t timestampMs = 0; uint64_t fingerprint = 0;
    bool operator==(const SecurityEvent&) const = default;
};

// Every refusal is explicit; nothing is skipped.  The encoder refuses exactly what the decoder
// refuses (one rule set), so no event the replayer would reject can be appended to __security.
enum class DecodeError : uint8_t { Truncated, UnknownFormatVer, UnknownKind, KindOpsMismatch, BadEnum, BadVerifier,
                                   TrailingBytes,
                                   /// The process could not allocate the decoded event (never corrupt bytes).
                                   OutOfMemory };
enum class EncodeError : uint8_t { KindOpsMismatch, FieldTooLong, BadVerifier,
                                   UnknownFormatVer, UnknownKind, BadEnum };

[[nodiscard]] EventKind kindOf(const Ops&) noexcept;
/// THE formatVer-1 value validation, the one rule set encode and decode run — exported so apply()
/// refuses an in-memory event exactly where encode would.  nullopt = valid.
[[nodiscard]] std::optional<EncodeError> validateEvent(const SecurityEvent&) noexcept;
[[nodiscard]] std::expected<void, EncodeError> encodeInto(const SecurityEvent&, std::vector<uint8_t>& out);   // clears, reuses capacity
[[nodiscard]] std::expected<std::vector<uint8_t>, EncodeError> encode(const SecurityEvent&);
[[nodiscard]] std::expected<SecurityEvent, DecodeError> decode(std::span<const uint8_t>) noexcept;
[[nodiscard]] std::string_view kindName(EventKind) noexcept;
[[nodiscard]] std::string_view decodeErrorName(DecodeError) noexcept;
[[nodiscard]] std::string_view encodeErrorName(EncodeError) noexcept;
[[nodiscard]] std::string_view operationName(Operation) noexcept;       // "READ" … "ADMIN"
[[nodiscard]] std::string_view resourceTypeName(ResourceType) noexcept;  // "CLUSTER" … "TRANSACTIONAL_ID"
namespace detail_testing { [[nodiscard]] std::size_t opsBodySize(const Ops&); }

} // namespace rune::security
