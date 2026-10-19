#pragma once
// PermissionModel.h - the INDEPENDENT oracle of the authentication stress (Task 18, spec §2 / §2.1 / §11).  Its own maps
// (users -> direct grants, roles -> grants, memberships, enabled) and its own name matching written from the spec: an
// exact name, or PREFIX '<p>' (starts_with); internal objects ("__...") never match a pattern; CREATE / ALTER / DROP /
// DESCRIBE ON CLUSTER reach every topic; effective permissions are the union of the direct and the role grants; default
// deny; a disabled user holds nothing.  It must NOT include rune/security/*: a bug shared with the broker would hide.
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <tuple>

namespace rune::node::test::pmodel {

enum class Op : uint8_t { Read, Write, Create };

struct GrantSpec {
    Op          op = Op::Read;
    bool        cluster = false;   ///< CREATE ON CLUSTER (the only cluster grant the stress uses)
    bool        prefix = false;    ///< PREFIX '<text>' (else exact)
    std::string text;              ///< the topic name / prefix
    bool operator<(const GrantSpec& o) const {
        return std::tie(op, cluster, prefix, text) < std::tie(o.op, o.cluster, o.prefix, o.text);
    }
    bool operator==(const GrantSpec&) const = default;
};

enum class Refusal : uint8_t { None, AlreadyGranted, NotGranted, AlreadyMember, NotMember, AlreadyDisabled, AlreadyEnabled };

struct Change {
    enum class Kind : uint8_t { Grant, Revoke, GrantRole, RevokeRole, Disable, Enable } kind = Kind::Grant;
    std::string who;          ///< the user (or the role for Grant / Revoke with whoIsRole)
    bool        whoIsRole = false;
    GrantSpec   grant;
    std::string role;         ///< GrantRole / RevokeRole
    [[nodiscard]] std::string str() const {
        static constexpr const char* kNames[] = {"GRANT", "REVOKE", "GRANT ROLE", "REVOKE ROLE", "DISABLE", "ENABLE"};
        static constexpr const char* kOps[] = {"READ", "WRITE", "CREATE"};
        std::string s = std::string(kNames[static_cast<int>(kind)]) + " ";
        if (kind == Kind::Grant || kind == Kind::Revoke)
            s += std::string(kOps[static_cast<int>(grant.op)]) + (grant.cluster ? " ON CLUSTER" : (grant.prefix ? " PREFIX '" : " '") + grant.text + "'") +
                 (kind == Kind::Grant ? " TO " : " FROM ") + (whoIsRole ? "role " : "user ") + who;
        else if (kind == Kind::GrantRole || kind == Kind::RevokeRole) s += role + (kind == Kind::GrantRole ? " TO " : " FROM ") + who;
        else s += who;
        return s;
    }
};

class Model {
public:
    void addUser(const std::string& u) { enabled_[u] = true; direct_[u]; roles_[u]; }
    void addRole(const std::string& r) { roleGrants_[r]; }

    /// Apply one change in log order; the refusal the broker must answer with (None = applied).
    Refusal apply(const Change& c) {
        using K = Change::Kind;
        switch (c.kind) {
        case K::Grant: {
            auto& set = c.whoIsRole ? roleGrants_[c.who] : direct_[c.who];
            return set.insert(c.grant).second ? Refusal::None : Refusal::AlreadyGranted;
        }
        case K::Revoke: {
            auto& set = c.whoIsRole ? roleGrants_[c.who] : direct_[c.who];
            return set.erase(c.grant) != 0 ? Refusal::None : Refusal::NotGranted;
        }
        case K::GrantRole: return roles_[c.who].insert(c.role).second ? Refusal::None : Refusal::AlreadyMember;
        case K::RevokeRole: return roles_[c.who].erase(c.role) != 0 ? Refusal::None : Refusal::NotMember;
        case K::Disable: if (!enabled_[c.who]) return Refusal::AlreadyDisabled; enabled_[c.who] = false; return Refusal::None;
        case K::Enable: if (enabled_[c.who]) return Refusal::AlreadyEnabled; enabled_[c.who] = true; return Refusal::None;
        }
        return Refusal::None;
    }

    [[nodiscard]] bool isEnabled(const std::string& user) const {
        auto it = enabled_.find(user);
        return it != enabled_.end() && it->second;
    }

    /// May `user` perform `op` on TOPIC `name` right now (spec §2 / §2.1)?
    [[nodiscard]] bool allowed(const std::string& user, Op op, std::string_view name) const {
        if (!isEnabled(user)) return false;
        if (anyMatch_(direct_, user, op, name)) return true;
        if (auto m = roles_.find(user); m != roles_.end())
            for (const std::string& r : m->second)
                if (anyMatch_(roleGrants_, r, op, name)) return true;
        return false;
    }

private:
    static bool grantCovers_(const GrantSpec& g, Op op, std::string_view name) {
        if (g.cluster) return g.op == Op::Create && op == Op::Create;   // CREATE ON CLUSTER reaches every topic
        if (g.op != op) return false;
        if (name.starts_with("__")) return false;                      // internal objects never match a pattern
        return g.prefix ? name.starts_with(g.text) : name == g.text;
    }
    static bool anyMatch_(const std::map<std::string, std::set<GrantSpec>>& m, const std::string& who, Op op, std::string_view name) {
        auto it = m.find(who);
        if (it == m.end()) return false;
        for (const GrantSpec& g : it->second) if (grantCovers_(g, op, name)) return true;
        return false;
    }

    std::map<std::string, std::set<GrantSpec>>   direct_, roleGrants_;
    std::map<std::string, std::set<std::string>> roles_;
    std::map<std::string, bool>                  enabled_;
};

} // namespace rune::node::test::pmodel
