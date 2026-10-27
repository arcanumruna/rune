// PermissionTable.cpp - see rune/gateway/PermissionTable.h.
#include <rune/gateway/PermissionTable.h>

namespace rune::gateway {

namespace rsec = rune::security;

namespace {
// Plan D11: the topic / view DDL a namespace admin holds in its own namespace (DESCRIBE is implied, D15).
constexpr uint8_t kNsAdminOps = rsec::opBit(rsec::Operation::Create) | rsec::opBit(rsec::Operation::Alter) |
                                rsec::opBit(rsec::Operation::Drop) | rsec::opBit(rsec::Operation::Describe);
constexpr bool nsAdminCovers(rsec::ResourceType type, std::string_view name) noexcept {
    return (type == rsec::ResourceType::Topic || type == rsec::ResourceType::View) && !name.starts_with("__");   // I6
}
} // namespace

PermissionTable PermissionTable::compile(std::span<const rsec::GrantSpec> effective, uint32_t nsId, const ObjectIndex& index,
                                         std::string_view nsName) {
    PermissionTable t;
    t.nsId_ = nsId;
    t.grants_.assign(effective.begin(), effective.end());
    t.clusterOps_ = rsec::clusterOps(t.grants_);
    for (const rsec::GrantSpec& g : t.grants_) {   // #211: hasAnyGrant
        const auto type = static_cast<std::size_t>(g.resource.type);
        if (type < t.anyOps_.size()) t.anyOps_[type] = static_cast<uint8_t>(t.anyOps_[type] | rsec::opBit(g.op));
    }
    t.root_ = (t.clusterOps_ & rsec::opBit(rsec::Operation::Admin)) != 0;
    // D11: ADMIN ON NAMESPACE "<the session's own namespace>" (a grant naming any other namespace implies nothing).
    if (!nsName.empty() &&
        (rsec::opsOn(t.grants_, rsec::ResourceType::Namespace, nsName) & rsec::opBit(rsec::Operation::Admin)) != 0) {
        t.nsAdminOps_ = kNsAdminOps;
        for (rsec::ResourceType ty : {rsec::ResourceType::Topic, rsec::ResourceType::View}) {
            const auto i = static_cast<std::size_t>(ty);
            t.anyOps_[i] = static_cast<uint8_t>(t.anyOps_[i] | kNsAdminOps);
        }
    }
    if (t.root_) {   // root (spec §2): every op on every type
        const auto ns = static_cast<std::size_t>(rsec::ResourceType::Namespace);   // ... but NAMESPACE stays explicit
        for (std::size_t i = 0; i < t.anyOps_.size(); ++i)
            if (i != ns) t.anyOps_[i] = 0xFF;
    }
    const auto limit = static_cast<std::size_t>(index.idLimit());
    t.ops_.assign(limit, 0);
    t.writeBits_.assign((limit + 63) / 64, 0);
    index.forEach([&](int32_t id, const ObjectEntry& e) { t.setOps_(id, t.opsOf_(e)); });
    return t;
}

uint8_t PermissionTable::opsOf_(const ObjectEntry& e) const noexcept {
    if (e.nsId != nsId_ && !root_) return 0;   // plan D9: a foreign object has no bits; root reaches every ns (#626)
    const uint8_t bits = rsec::opsOn(grants_, e.type, e.name);
    if (e.nsId != nsId_ || !nsAdminCovers(e.type, e.name)) return bits;   // D11 reaches the OWN namespace only
    return static_cast<uint8_t>(bits | nsAdminOps_);
}

bool PermissionTable::allowsName(rsec::ResourceType type, std::string_view name, uint8_t opBits) const noexcept {
    if (allowAll_) return true;
    uint8_t bits = rsec::opsOn(grants_, type, name);
    if (nsAdminCovers(type, name)) bits = static_cast<uint8_t>(bits | nsAdminOps_);   // D11: names resolve in the own ns
    return (bits & opBits) == opBits;
}

void PermissionTable::setOps_(int32_t id, uint8_t ops) {
    const auto i = static_cast<std::size_t>(id);
    if (i >= ops_.size()) ops_.resize(i + 1, 0);
    if (writeBits_.size() < (ops_.size() + 63) / 64) writeBits_.resize((ops_.size() + 63) / 64, 0);
    ops_[i] = ops;
    const uint64_t bit = uint64_t{1} << (i % 64);
    if ((ops & rsec::opBit(rsec::Operation::Write)) != 0) writeBits_[i / 64] |= bit;
    else                                                   writeBits_[i / 64] &= ~bit;
}

void PermissionTable::onObjectCreated(int32_t id, const ObjectEntry& entry) {
    if (allowAll_ || id < 0) return;
    setOps_(id, opsOf_(entry));
}

void PermissionTable::onObjectDropped(int32_t id) noexcept {
    if (allowAll_ || static_cast<uint32_t>(id) >= ops_.size()) return;
    const auto i = static_cast<std::size_t>(id);
    ops_[i] = 0;
    writeBits_[i / 64] &= ~(uint64_t{1} << (i % 64));
}

} // namespace rune::gateway
