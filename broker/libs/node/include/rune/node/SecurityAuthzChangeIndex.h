#pragma once
// SecurityAuthzChangeIndex.h - issues #218 / #67: which applied `__security` events could have changed what ONE user
// may do.  A DDL is stamped with the `__security` offset the gateway authorized it against; the admin thread appends it
// only if no event applied after that stamp could have changed what ITS principal may do (the per-principal fence), so
// an admin pipelining DDL about other users never retries.
//
// The narrowing is opt-in: an event kind that is not listed in note() is "wide" (marks every principal), so a kind
// added later is safe by default.  Role-side changes (a role's grants, a role created or dropped), a namespace purge
// and the bootstrap are wide; an unknown principal (anonymous, dropped, never seen since boot) is always stale on any
// change.  Admin-thread only, owned by SecurityApplier: no locks, no atomics, not on a hot path.
#include <rune/security/SecurityEvent.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <utility>

namespace rune::node {

class SecurityAuthzChangeIndex {
public:
    /// What one event touches: every principal, or one user (nsId, local name).
    struct Scope {
        bool             global = true;
        uint32_t         nsId   = 0;
        std::string_view local;
    };

    /// The one classification rule (note() and the in-flight check share it).
    [[nodiscard]] static Scope scopeOf(const rune::security::SecurityEvent& ev) noexcept {
        using rune::security::EventKind;
        switch (ev.kind) {
        case EventKind::CreateUser:
        case EventKind::AlterUser:
        case EventKind::GrantRole:
        case EventKind::RevokeRole:
        case EventKind::DropUser:
            return Scope{false, ev.nsId, ev.objectName};
        case EventKind::Grant:
            if (const auto* g = std::get_if<rune::security::GrantOps>(&ev.ops);
                g != nullptr && g->grantee == rune::security::GranteeKind::User)
                return Scope{false, ev.nsId, ev.objectName};
            return Scope{};
        case EventKind::Revoke:
            if (const auto* g = std::get_if<rune::security::RevokeOps>(&ev.ops);
                g != nullptr && g->grantee == rune::security::GranteeKind::User)
                return Scope{false, ev.nsId, ev.objectName};
            return Scope{};
        default:   // CreateRole, DropRole, Bootstrap, NamespacePurged, anything new: wide
            return Scope{};
        }
    }

    /// true when `ev` would make an already-authorized request of (nsId, local) stale: it is wide, or it is about that
    /// user.  An empty `local` (anonymous / unknown) is affected by every event.
    [[nodiscard]] static bool affects(const rune::security::SecurityEvent& ev, uint32_t nsId,
                                      std::string_view local) noexcept {
        const Scope s = scopeOf(ev);
        return s.global || local.empty() || (s.nsId == nsId && s.local == local);
    }

    void note(const rune::security::SecurityEvent& ev, int64_t offset) {
        const Scope s = scopeOf(ev);
        if (s.global) global_ = offset;
        // The bootstrap is wide AND names the first admin: that user is known from here on.
        if (s.global && ev.kind != rune::security::EventKind::Bootstrap) return;
        const auto key = std::make_pair(ev.nsId, std::string(ev.objectName));
        if (ev.kind == rune::security::EventKind::DropUser) user_.erase(key);   // unknown = always stale
        else user_[key] = offset;
    }

    /// true when an event applied at an offset > `stamp` could have changed what the user may do.
    [[nodiscard]] bool changedSince(uint32_t nsId, std::string_view local, int64_t stamp) const {
        if (global_ > stamp) return true;
        if (local.empty()) return true;
        const auto it = user_.find(std::make_pair(nsId, std::string(local)));
        return it == user_.end() || it->second > stamp;
    }

    /// The fence predicate: is a request stamped at `stamp` (authorized by the user `authenticated`, whose LOCAL name
    /// starts at `localAt`, in namespace `nsId`) stale against the applier's `applied` offset?  Equal: never.
    /// A stamp beyond `applied` cannot happen (the mirror trails the applier): stale, fail safe.
    [[nodiscard]] bool staleAt(int64_t applied, int64_t stamp, uint32_t nsId, std::string_view authenticated,
                               uint16_t localAt) const {
        if (stamp == applied) return false;
        if (stamp > applied) return true;
        const std::string_view local =
            localAt <= authenticated.size() ? authenticated.substr(localAt) : std::string_view{};
        return changedSince(nsId, local, stamp);
    }

private:
    int64_t                                                global_ = -1;   // last event not attributable to one user
    std::map<std::pair<uint32_t, std::string>, int64_t>    user_;          // (nsId, local name) -> last event about it
};

} // namespace rune::node
