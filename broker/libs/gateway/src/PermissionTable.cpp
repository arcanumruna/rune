// PermissionTable.cpp - see rune/gateway/PermissionTable.h.
#include <rune/gateway/PermissionTable.h>

namespace rune::gateway {

namespace rsec = rune::security;

PermissionTable PermissionTable::compile(std::span<const rsec::GrantSpec> effective, const ObjectIndex& index) {
    PermissionTable t;
    t.grants_.assign(effective.begin(), effective.end());
    t.clusterOps_ = rsec::clusterOps(t.grants_);
    const auto limit = static_cast<std::size_t>(index.idLimit());
    t.ops_.assign(limit, 0);
    t.writeBits_.assign((limit + 63) / 64, 0);
    index.forEach([&](int32_t id, const ObjectEntry& e) { t.setOps_(id, rsec::opsOn(t.grants_, e.type, e.name)); });
    return t;
}

bool PermissionTable::allowsName(rsec::ResourceType type, std::string_view name, uint8_t opBits) const noexcept {
    if (allowAll_) return true;
    return (rsec::opsOn(grants_, type, name) & opBits) == opBits;
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
    setOps_(id, rsec::opsOn(grants_, entry.type, entry.name));
}

void PermissionTable::onObjectDropped(int32_t id) noexcept {
    if (allowAll_ || static_cast<uint32_t>(id) >= ops_.size()) return;
    const auto i = static_cast<std::size_t>(id);
    ops_[i] = 0;
    writeBits_[i / 64] &= ~(uint64_t{1} << (i % 64));
}

} // namespace rune::gateway
