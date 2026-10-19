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

uint8_t clusterOps(std::span<const GrantSpec> grants) noexcept {
    uint8_t bits = 0;
    for (const GrantSpec& g : grants)
        if (g.resource.type == ResourceType::Cluster) bits = static_cast<uint8_t>(bits | opBit(g.op));
    return bits;
}

uint8_t opsOn(std::span<const GrantSpec> grants, ResourceType type, std::string_view name) noexcept {
    uint8_t bits = 0;
    for (const GrantSpec& g : grants)
        if (matches(g.resource, type, name)) bits = static_cast<uint8_t>(bits | opBit(g.op));
    // D16: CREATE / ALTER / DROP / DESCRIBE ON CLUSTER reach every topic and view; DESCRIBE reaches
    // groups and transactional ids.  ADMIN (and READ / WRITE, which a cluster grant cannot hold) never do.
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
