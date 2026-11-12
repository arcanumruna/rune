#pragma once
// SecurityAuthzChangeIndex.h - issues #218 / #67: which applied `__security` events could have changed what ONE user
// may do.  A DDL is stamped with the `__security` offset the gateway authorized it against; the admin thread appends it
// only if no event applied after that stamp could have changed what ITS principal may do (the per-principal fence), so
// an admin pipelining DDL about other users or roles never retries.
//
// "Whose permissions did this event change" is answered by the security state machine itself: the `affectedUsers` (and,
// for a namespace purge, `refreshedUsers`) set of sec::Applied, the same set that drives session refresh and revoke.
// The index keeps one offset per user object id and nothing else about the event.  The narrowing is opt-in: only the
// kinds listed in note() use that set; the bootstrap and any kind added later are "any change" (every principal), so a
// new kind is safe by default.  An unknown principal (anonymous, dropped, purged) is always stale on any change.
// Admin-thread only, owned by SecurityApplier: no locks, no atomics, not on a hot path.
#include <rune/security/SecurityEvent.h>
#include <rune/security/SecurityState.h>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace rune::node {

class SecurityAuthzChangeIndex {
public:
    /// Called right after sec::apply succeeded: `a` is its result, `after` the state WITH the event applied.
    void note(const rune::security::SecurityEvent& ev, const rune::security::Applied& a,
              const rune::security::SecurityState& after, int64_t offset) {
        using rune::security::EventKind;
        const auto mark = [&](uint64_t id) {
            if (after.users().contains(id)) user_[id] = offset;
            else user_.erase(id);   // dropped or purged: unknown from here on, hence always stale
        };
        switch (ev.kind) {
        case EventKind::CreateUser:
        case EventKind::AlterUser:
        case EventKind::DropUser:
            mark(a.objectId);   // a re-created name must not inherit "unchanged"; AlterUser stays conservative
            [[fallthrough]];
        case EventKind::GrantRole:
        case EventKind::RevokeRole:
        case EventKind::Grant:
        case EventKind::Revoke:
        case EventKind::CreateRole:
        case EventKind::DropRole:
        case EventKind::NamespacePurged:
            for (const uint64_t id : a.affectedUsers) mark(id);
            for (const uint64_t id : a.refreshedUsers) mark(id);
            return;
        default:   // Bootstrap and any kind added later: any change
            global_ = offset;
            return;
        }
    }

    /// true when an event applied at an offset > `stamp` could have changed what the user (nsId, local) may do.
    [[nodiscard]] bool changedSince(const rune::security::SecurityState& s, uint32_t nsId, std::string_view local,
                                    int64_t stamp) const {
        if (global_ > stamp) return true;
        if (local.empty()) return true;
        const rune::security::UserObject* u = s.userByName(nsId, local);
        if (u == nullptr) return true;
        const auto it = user_.find(u->objectId);
        // No entry: no event since the log start (boot replay notes every event).
        return it != user_.end() && it->second > stamp;
    }

    /// An event appended but NOT yet applied: would it change what (nsId, local) may do?  `s` is the current state.
    [[nodiscard]] static bool affects(const rune::security::SecurityState& s, const rune::security::SecurityEvent& ev,
                                      uint32_t nsId, std::string_view local) {
        using rune::security::EventKind;
        using rune::security::GranteeKind;
        if (local.empty()) return true;
        const rune::security::UserObject* user = s.userByName(nsId, local);
        if (user == nullptr) return true;
        const auto isUser = [&] { return ev.nsId == nsId && ev.objectName == local; };
        const auto isMember = [&] {   // ev.objectName is the role's local name
            const rune::security::RoleObject* r = s.roleByName(ev.nsId, ev.objectName);
            return r == nullptr || user->roles.contains(r->objectId);   // unknown role: fail safe
        };
        switch (ev.kind) {
        case EventKind::CreateUser:
        case EventKind::AlterUser:
        case EventKind::DropUser:
        case EventKind::GrantRole:
        case EventKind::RevokeRole:
            return isUser();
        case EventKind::Grant:
            if (const auto* g = std::get_if<rune::security::GrantOps>(&ev.ops); g != nullptr)
                return g->grantee == GranteeKind::User ? isUser() : isMember();
            return true;
        case EventKind::Revoke:
            if (const auto* g = std::get_if<rune::security::RevokeOps>(&ev.ops); g != nullptr)
                return g->grantee == GranteeKind::User ? isUser() : isMember();
            return true;
        case EventKind::DropRole:
            return isMember();
        case EventKind::CreateRole:
            return false;
        default:   // Bootstrap, NamespacePurged, anything new
            return true;
        }
    }

    /// Number of users tracked (tests).
    [[nodiscard]] std::size_t usersForTest() const noexcept { return user_.size(); }

    /// The fence predicate: is a request stamped at `stamp` (authorized by the user `authenticated`, whose LOCAL name
    /// starts at `localAt`, in namespace `nsId`) stale against the applier's `applied` offset?  Equal: never.
    /// A stamp beyond `applied` cannot happen (the mirror trails the applier): stale, fail safe.
    [[nodiscard]] bool staleAt(const rune::security::SecurityState& s, int64_t applied, int64_t stamp, uint32_t nsId,
                               std::string_view authenticated, uint16_t localAt) const {
        if (stamp == applied) return false;
        if (stamp > applied) return true;
        const std::string_view local =
            localAt <= authenticated.size() ? authenticated.substr(localAt) : std::string_view{};
        return changedSince(s, nsId, local, stamp);
    }

private:
    int64_t                                  global_ = -1;   // last event that changes everyone's permissions
    std::unordered_map<uint64_t, int64_t>    user_;          // user object id -> offset of the last event about it
};

} // namespace rune::node
