#pragma once
// rune/security/SecurityState.h — the security state (users, roles, grants, tombstones), mutated
// ONLY by apply() under the frozen formatVer-1 rules (src/security/ApplyV1.cpp).  The broker
// (admin applier + control mirror), the client and the CLI all run these same rules.
// Single-threaded, owned by whoever replays.  Cold path (control plane).
#include <rune/security/SecurityEvent.h>

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

struct UserObject {
    uint64_t objectId = 0; std::string name; uint32_t version = 0;
    bool enabled = true; bool mustChangePassword = false;
    std::vector<ScramVerifier> verifiers;      // 1..2, oldest first (D6)
    std::set<uint64_t> roles;                  // role objectIds
    std::set<GrantSpec> grants;                // direct grants
};
struct RoleObject { uint64_t objectId = 0; std::string name; uint32_t version = 0; std::set<GrantSpec> grants; };
struct Tombstone  { uint64_t objectId = 0; bool wasRole = false; std::string name; uint32_t finalVersion = 0; };

enum class SessionEffect : uint8_t { None, Refresh, Revoke };          // D25
struct Applied {
    EventKind kind = EventKind::CreateUser; uint64_t objectId = 0; uint32_t newVersion = 0; uint64_t fingerprint = 0;
    SessionEffect effect = SessionEffect::None;
    std::vector<uint64_t> affectedUsers;       // the users whose sessions the effect applies to (sorted)
};
struct RequestRecord { int64_t offset = -1; EventKind kind{}; uint64_t objectId = 0; uint32_t newVersion = 0;
                       uint64_t requestFingerprint = 0; };

enum class Refusal : uint8_t { InvalidName, UserExists, RoleExists, UnknownUser, UnknownRole, VersionMismatch,
    AlreadyGranted, NotGranted, InvalidGrant, AlreadyMember, NotMember, TooManyPasswords, LastPassword,
    NoSuchPassword, AlreadyDisabled, AlreadyEnabled, AlreadyBootstrapped };
struct ApplyError {
    enum class Code : uint8_t { UnsupportedFormat, Precondition, FingerprintMismatch, BadOps, OutOfOrder };
    int64_t offset = -1; Code code = Code::Precondition; std::optional<Refusal> refusal;
    uint64_t expected = 0, actual = 0; std::string detail;     // detail: user-facing, never a secret
};

class SecurityState {
public:
    [[nodiscard]] const UserObject* user(std::string_view name) const noexcept;
    [[nodiscard]] const UserObject* userById(uint64_t id) const noexcept;
    [[nodiscard]] const RoleObject* role(std::string_view name) const noexcept;
    [[nodiscard]] const RoleObject* roleById(uint64_t id) const noexcept;
    [[nodiscard]] const std::map<uint64_t, UserObject>& users() const noexcept { return users_; }
    [[nodiscard]] const std::map<uint64_t, RoleObject>& roles() const noexcept { return roles_; }
    [[nodiscard]] const std::vector<Tombstone>& tombstones() const noexcept { return tombstones_; }
    [[nodiscard]] std::vector<uint64_t> membersOf(uint64_t roleId) const;   // sorted
    [[nodiscard]] bool hasSecurityAdmin() const noexcept;                    // an ENABLED user effectively holds ADMIN CLUSTER
    [[nodiscard]] int64_t appliedOffset() const noexcept { return appliedOffset_; }   // -1 = nothing applied
    [[nodiscard]] uint64_t nextObjectId() const noexcept { return nextObjectId_; }    // users and roles share one counter
    [[nodiscard]] std::optional<RequestRecord> request(const RequestId&) const;
    /// FNV-1a-64 over appliedOffset, nextObjectId, each live user then role fingerprint in objectId order,
    /// each tombstone in apply order.  Equal digests at equal offsets = the replicas agree.
    [[nodiscard]] uint64_t digest() const noexcept;
private:
    friend struct ApplyV1;
    std::map<uint64_t, UserObject> users_; std::map<uint64_t, RoleObject> roles_;
    std::map<std::string, uint64_t, std::less<>> userNames_, roleNames_;
    std::vector<Tombstone> tombstones_;
    std::unordered_map<RequestId, RequestRecord, rune::catalog::RequestIdHash> requests_;
    uint64_t nextObjectId_ = 1; int64_t appliedOffset_ = -1;
};

[[nodiscard]] std::expected<Applied, ApplyError> apply(SecurityState& s, const SecurityEvent& e, int64_t offset);
[[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const SecurityState& s, const SecurityEvent& e);
[[nodiscard]] std::expected<Applied, ApplyError> previewApplied(const SecurityState& s, const SecurityEvent& e);  // the leader's dry run
[[nodiscard]] std::string_view applyErrorName(ApplyError::Code) noexcept;
[[nodiscard]] std::string_view refusalName(Refusal) noexcept;
[[nodiscard]] bool validName(std::string_view) noexcept;     // [A-Za-z0-9._@-]{1,128}, not "__…" (D7)
[[nodiscard]] bool validGrant(const GrantSpec&) noexcept;    // D16, D17

} // namespace rune::security
