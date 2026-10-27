#pragma once
// rune/security/SecurityState.h — the security state (users, roles, grants, tombstones), mutated
// ONLY by apply() under the per-formatVer rules (src/security/ApplyV1.cpp, ApplyV2.cpp).  Users and
// roles are per NAMESPACE: the key of a name is (nsId, local name), ids stay global.  The broker
// (admin applier + control mirror), the client and the CLI all run these same rules.
// Single-threaded, owned by whoever replays.  Cold path (control plane).
#include <rune/catalog/NsName.h>
#include <rune/security/SecurityEvent.h>

#include <array>
#include <cstdint>
#include <expected>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::security {

/// The key of a user or role name: (nsId, local name).
struct SecKey { uint32_t nsId = 0; std::string local; auto operator<=>(const SecKey&) const = default; };
struct SecKeyView { uint32_t nsId; std::string_view local; };
/// Transparent ordering over SecKey / SecKeyView (nsId, then name bytes).
struct SecKeyLess {
    using is_transparent = void;
    template <class A, class B> bool operator()(const A& a, const B& b) const noexcept {
        if (a.nsId != b.nsId) return a.nsId < b.nsId;
        return std::string_view(a.local) < std::string_view(b.local);
    }
};

struct UserObject {
    uint64_t objectId = 0; std::string name; uint32_t version = 0;
    bool enabled = true; bool mustChangePassword = false;
    std::vector<ScramVerifier> verifiers;      // 1..2, oldest first (D6)
    std::set<uint64_t> roles;                  // role objectIds (always roles of this user's namespace)
    std::set<GrantSpec> grants;                // direct grants
    uint32_t nsId = 0;                         // the namespace; `name` is the local name
};
struct RoleObject { uint64_t objectId = 0; std::string name; uint32_t version = 0; std::set<GrantSpec> grants;
                    uint32_t nsId = 0; };
struct Tombstone  { uint64_t objectId = 0; bool wasRole = false; std::string name; uint32_t finalVersion = 0;
                    uint32_t nsId = 0; };

enum class SessionEffect : uint8_t { None, Refresh, Revoke };          // D25
struct Applied {
    EventKind kind = EventKind::CreateUser; uint64_t objectId = 0; uint32_t newVersion = 0; uint64_t fingerprint = 0;
    SessionEffect effect = SessionEffect::None;
    std::vector<uint64_t> affectedUsers;       // the users whose sessions the effect applies to (sorted)
    uint32_t nsId = 0;                         // the event's namespace
};
struct RequestRecord { int64_t offset = -1; EventKind kind{}; uint64_t objectId = 0; uint32_t newVersion = 0;
                       uint64_t requestFingerprint = 0; };

enum class Refusal : uint8_t { InvalidName, UserExists, RoleExists, UnknownUser, UnknownRole, VersionMismatch,
    AlreadyGranted, NotGranted, InvalidGrant, AlreadyMember, NotMember, TooManyPasswords, LastPassword,
    NoSuchPassword, AlreadyDisabled, AlreadyEnabled, AlreadyBootstrapped,
    /// formatVer 2: a role membership across namespaces, or a Bootstrap outside the default namespace.
    NamespaceMismatch };
struct ApplyError {
    enum class Code : uint8_t { UnsupportedFormat, Precondition, FingerprintMismatch, BadOps, OutOfOrder };
    int64_t offset = -1; Code code = Code::Precondition; std::optional<Refusal> refusal;
    uint64_t expected = 0, actual = 0; std::string detail;     // detail: user-facing, never a secret
};

class SecurityState {
public:
    [[nodiscard]] const UserObject* userByName(uint32_t nsId, std::string_view local) const noexcept;
    [[nodiscard]] const UserObject* userById(uint64_t id) const noexcept;
    [[nodiscard]] const RoleObject* roleByName(uint32_t nsId, std::string_view local) const noexcept;
    [[nodiscard]] const RoleObject* roleById(uint64_t id) const noexcept;
    [[nodiscard]] const std::map<uint64_t, UserObject>& users() const noexcept { return users_; }
    [[nodiscard]] const std::map<uint64_t, RoleObject>& roles() const noexcept { return roles_; }
    [[nodiscard]] const std::vector<Tombstone>& tombstones() const noexcept { return tombstones_; }
    [[nodiscard]] std::vector<uint64_t> membersOf(uint64_t roleId) const;   // sorted
    [[nodiscard]] bool hasSecurityAdmin() const;                    // an ENABLED DEFAULT-namespace user effectively holds ADMIN CLUSTER
    /// The enabled users of namespace `nsId` that effectively hold ADMIN on NAMESPACE "<nsName>" (sorted by id).  The
    /// codec has no catalog, so the caller supplies the namespace's name; a grant naming another namespace is inert.
    /// A namespace admin never counts toward hasSecurityAdmin().
    [[nodiscard]] std::vector<uint64_t> nsAdminOf(uint32_t nsId, std::string_view nsName) const;
    [[nodiscard]] int64_t appliedOffset() const noexcept { return appliedOffset_; }   // -1 = nothing applied
    [[nodiscard]] uint64_t nextObjectId() const noexcept { return nextObjectId_; }    // users and roles share one counter
    [[nodiscard]] std::optional<RequestRecord> request(const RequestId&) const;
    /// FNV-1a-64 over appliedOffset, nextObjectId, each live user then role fingerprint in objectId order,
    /// each tombstone in apply order; an object or tombstone of a namespace other than 0 hashes
    /// objectFingerprintV2(fingerprint, nsId) (a default-only state keeps the digest it had before namespaces).
    /// Equal digests at equal offsets = the replicas agree.
    [[nodiscard]] uint64_t digest() const;
    /// D8: the cluster-wide key of the fake salts, set by the FIRST Bootstrap event and never changed
    /// (a later Bootstrap keeps it).  nullopt before any bootstrap.  Covered by digest(), not by fingerprints.
    [[nodiscard]] const std::optional<std::array<uint8_t, kFakeSaltSeedBytes>>& fakeSaltSeed() const noexcept { return fakeSaltSeed_; }
private:
    friend struct ApplyV1;
    friend struct ApplyV2;
    std::map<uint64_t, UserObject> users_; std::map<uint64_t, RoleObject> roles_;
    std::map<SecKey, uint64_t, SecKeyLess> userNames_, roleNames_;
    std::vector<Tombstone> tombstones_;
    std::unordered_map<RequestId, RequestRecord, rune::catalog::RequestIdHash> requests_;
    std::optional<std::array<uint8_t, kFakeSaltSeedBytes>> fakeSaltSeed_;
    uint64_t nextObjectId_ = 1; int64_t appliedOffset_ = -1;
};

[[nodiscard]] std::expected<Applied, ApplyError> apply(SecurityState& s, const SecurityEvent& e, int64_t offset);
[[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const SecurityState& s, const SecurityEvent& e);
[[nodiscard]] std::expected<Applied, ApplyError> previewApplied(const SecurityState& s, const SecurityEvent& e);  // the leader's dry run
[[nodiscard]] std::string_view applyErrorName(ApplyError::Code) noexcept;
[[nodiscard]] std::string_view refusalName(Refusal) noexcept;
[[nodiscard]] bool validName(std::string_view) noexcept;     // [A-Za-z0-9._@-]{1,128}, not "__…" (D7)
/// D10, D16, D17.  `roleNsId` / `roleNsName`: the namespace of the role (or user) the grant is for.
///   Cluster: only roleNsId == 0.  Namespace: ops {Admin, Describe}, an Exact pattern whose name is the namespace's own
///   name.  Topic: Read, Write, Create, Alter, Drop, Describe, Truncate.  The apply rules have no catalog: they pass
///   roleNsName "default" for nsId 0 and "" for any other (unknown), which checks only the SHAPE of a Namespace
///   pattern (a valid namespace name); a front end that has the catalog passes the real name.
[[nodiscard]] bool validGrant(uint32_t roleNsId, std::string_view roleNsName, const GrantSpec&) noexcept;

} // namespace rune::security
