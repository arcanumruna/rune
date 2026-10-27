// src/security/Permissions.cpp — see rune/security/Permissions.h.
#include <rune/security/Permissions.h>

#include <algorithm>

namespace rune::security {

bool matches(const ResourcePattern& p, ResourceType type, std::string_view name) noexcept {
    if (p.type != type) return false;
    if (type != ResourceType::Cluster && name.starts_with("__")) return false;   // I6: internal objects never match a pattern
    return p.kind == PatternKind::Exact ? name == p.name : name.starts_with(p.name);
}

std::vector<GrantSpec> effectiveGrants(const SecurityState& s, uint64_t userId) {
    const UserObject* u = s.userById(userId);
    if (!u) return {};
    std::vector<GrantSpec> out(u->grants.begin(), u->grants.end());
    for (uint64_t rid : u->roles)
        if (const RoleObject* r = s.roleById(rid)) out.insert(out.end(), r->grants.begin(), r->grants.end());
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

EffectivePerms effectivePerms(const SecurityState& s, uint64_t userId, std::string_view nsName) {
    EffectivePerms p;
    const UserObject* u = s.userById(userId);
    if (!u) return p;
    p.nsId = u->nsId;
    p.grants = effectiveGrants(s, userId);
    p.nsAdmin = (opsOn(p.grants, ResourceType::Namespace, nsName) & opBit(Operation::Admin)) != 0;
    return p;
}

namespace {
constexpr uint8_t kAllOps = 0xFF;
// ADMIN ON CLUSTER is root (namespaces spec §2): the raw OR of the CLUSTER grants holds ADMIN.
bool isRoot(std::span<const GrantSpec> grants) noexcept {
    for (const GrantSpec& g : grants)
        if (g.resource.type == ResourceType::Cluster && g.op == Operation::Admin) return true;
    return false;
}
} // namespace

uint8_t opsOn(const EffectivePerms& perms, ResourceType type, uint32_t objectNsId, std::string_view name) noexcept {
    const std::span<const GrantSpec> grants(perms.grants);
    if (objectNsId != perms.nsId && !isRoot(grants)) return 0;   // D9; root reaches every namespace (spec §2)
    return opsOn(grants, type, name);
}

uint8_t clusterOps(std::span<const GrantSpec> grants) noexcept {
    if (isRoot(grants)) return kAllOps;   // root holds every cluster operation (CREATE TOPIC, DESCRIBE CLUSTER, ...)
    uint8_t bits = 0;
    for (const GrantSpec& g : grants)
        if (g.resource.type == ResourceType::Cluster) bits = static_cast<uint8_t>(bits | opBit(g.op));
    return bits;
}

uint8_t opsOn(std::span<const GrantSpec> grants, ResourceType type, std::string_view name) noexcept {
    const bool internal = type != ResourceType::Cluster && name.starts_with("__");   // I6
    // Root: every operation on the cluster and on every user topic / view / group / transactional id.  An internal
    // object stays at what an explicit cluster grant gives (below), and a NAMESPACE grant is never implied (it decides
    // namespace-admin membership).
    if (type != ResourceType::Namespace && !internal && isRoot(grants)) return kAllOps;
    uint8_t bits = 0;
    for (const GrantSpec& g : grants)
        if (matches(g.resource, type, name)) bits = static_cast<uint8_t>(bits | opBit(g.op));
    // D16: CREATE / ALTER / DROP / DESCRIBE ON CLUSTER reach every topic and view; DESCRIBE reaches
    // groups and transactional ids.  READ / WRITE / TRUNCATE never come from a cluster grant (only root above).
    const uint8_t cluster = clusterOps(grants);
    constexpr uint8_t kObjectOps = opBit(Operation::Create) | opBit(Operation::Alter) | opBit(Operation::Drop) |
                                   opBit(Operation::Describe);
    if (type == ResourceType::Topic || type == ResourceType::View) bits = static_cast<uint8_t>(bits | (cluster & kObjectOps));
    else if (type == ResourceType::Group || type == ResourceType::TransactionalId)
        bits = static_cast<uint8_t>(bits | (cluster & opBit(Operation::Describe)));
    if (bits != 0) bits = static_cast<uint8_t>(bits | opBit(Operation::Describe));   // D15
    return bits;
}

} // namespace rune::security
