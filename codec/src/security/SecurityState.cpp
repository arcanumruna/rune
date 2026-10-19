// src/security/SecurityState.cpp — reads, digest, names.  Mutation lives in ApplyV1.cpp.
#include <rune/security/SecurityState.h>
#include <rune/security/Permissions.h>
#include <rune/security/SecurityFingerprint.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>

namespace rune::security {

namespace {
template <class Map, class Index, class Key>
const typename Map::mapped_type* viaIndex(const Map& objects, const Index& index, const Key& key) {
    auto it = index.find(key);
    if (it == index.end()) return nullptr;
    auto ot = objects.find(it->second);
    return ot == objects.end() ? nullptr : &ot->second;
}
template <class Map>
const typename Map::mapped_type* byId(const Map& m, uint64_t id) {
    auto it = m.find(id);
    return it == m.end() ? nullptr : &it->second;
}
} // namespace

const UserObject* SecurityState::user(std::string_view name) const noexcept { return viaIndex(users_, userNames_, name); }
const UserObject* SecurityState::userById(uint64_t id) const noexcept { return byId(users_, id); }
const RoleObject* SecurityState::role(std::string_view name) const noexcept { return viaIndex(roles_, roleNames_, name); }
const RoleObject* SecurityState::roleById(uint64_t id) const noexcept { return byId(roles_, id); }

std::vector<uint64_t> SecurityState::membersOf(uint64_t roleId) const {
    std::vector<uint64_t> out;
    for (const auto& [id, u] : users_)   // std::map: ascending id = sorted
        if (u.roles.contains(roleId)) out.push_back(id);
    return out;
}

bool SecurityState::hasSecurityAdmin() const {
    for (const auto& [id, u] : users_) {
        if (!u.enabled) continue;
        const auto grants = effectiveGrants(*this, id);
        if (clusterOps(grants) & opBit(Operation::Admin)) return true;
    }
    return false;
}

std::optional<RequestRecord> SecurityState::request(const RequestId& id) const {
    auto it = requests_.find(id);
    if (it == requests_.end()) return std::nullopt;
    return it->second;
}

uint64_t SecurityState::digest() const {
    namespace det = rune::catalog::detail;
    uint64_t h = rune::codec::detail::kFnvSeed;
    h = det::fnvU64(h, static_cast<uint64_t>(appliedOffset_));
    h = det::fnvU64(h, nextObjectId_);
    // Live objects in objectId order: merge the two objectId-ordered maps.
    auto u = users_.begin();
    auto r = roles_.begin();
    while (u != users_.end() || r != roles_.end()) {
        if (r == roles_.end() || (u != users_.end() && u->first < r->first)) {
            h = det::fnvU64(h, fingerprintOf(u->second, *this));
            ++u;
        } else {
            h = det::fnvU64(h, fingerprintOf(r->second));
            ++r;
        }
    }
    for (const Tombstone& t : tombstones_) h = det::fnvU64(h, fingerprintOf(t));
    h = rune::codec::detail::fnv1a_byte(h, fakeSaltSeed_ ? 1 : 0);   // replicas must agree on the D8 seed
    if (fakeSaltSeed_) h = det::fnvBytes(h, fakeSaltSeed_->data(), fakeSaltSeed_->size());
    return h;
}

std::string_view applyErrorName(ApplyError::Code c) noexcept {
    switch (c) {
    case ApplyError::Code::UnsupportedFormat:   return "UnsupportedFormat";
    case ApplyError::Code::Precondition:        return "Precondition";
    case ApplyError::Code::FingerprintMismatch: return "FingerprintMismatch";
    case ApplyError::Code::BadOps:              return "BadOps";
    case ApplyError::Code::OutOfOrder:          return "OutOfOrder";
    }
    return "?";
}

std::string_view refusalName(Refusal r) noexcept {
    switch (r) {
    case Refusal::InvalidName: return "InvalidName";         case Refusal::UserExists: return "UserExists";
    case Refusal::RoleExists: return "RoleExists";           case Refusal::UnknownUser: return "UnknownUser";
    case Refusal::UnknownRole: return "UnknownRole";         case Refusal::VersionMismatch: return "VersionMismatch";
    case Refusal::AlreadyGranted: return "AlreadyGranted";   case Refusal::NotGranted: return "NotGranted";
    case Refusal::InvalidGrant: return "InvalidGrant";       case Refusal::AlreadyMember: return "AlreadyMember";
    case Refusal::NotMember: return "NotMember";             case Refusal::TooManyPasswords: return "TooManyPasswords";
    case Refusal::LastPassword: return "LastPassword";       case Refusal::NoSuchPassword: return "NoSuchPassword";
    case Refusal::AlreadyDisabled: return "AlreadyDisabled"; case Refusal::AlreadyEnabled: return "AlreadyEnabled";
    case Refusal::AlreadyBootstrapped: return "AlreadyBootstrapped";
    }
    return "?";
}

bool validName(std::string_view n) noexcept {
    if (n.empty() || n.size() > 128 || n.starts_with("__")) return false;
    for (char c : n) {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                        c == '.' || c == '_' || c == '@' || c == '-';
        if (!ok) return false;
    }
    return true;
}

bool validGrant(const GrantSpec& g) noexcept {
    const ResourcePattern& p = g.resource;
    if (static_cast<uint8_t>(g.op) > static_cast<uint8_t>(Operation::Admin) ||
        static_cast<uint8_t>(p.type) > static_cast<uint8_t>(ResourceType::TransactionalId) ||
        static_cast<uint8_t>(p.kind) > static_cast<uint8_t>(PatternKind::Prefix))
        return false;
    uint8_t allowed = 0;   // D16, D17
    switch (p.type) {
    case ResourceType::Cluster:
        allowed = opBit(Operation::Create) | opBit(Operation::Alter) | opBit(Operation::Drop) |
                  opBit(Operation::Describe) | opBit(Operation::Admin);
        if (p.kind != PatternKind::Exact || !p.name.empty()) return false;   // CLUSTER is Exact ""
        break;
    case ResourceType::Topic:
        allowed = opBit(Operation::Read) | opBit(Operation::Write) | opBit(Operation::Create) |
                  opBit(Operation::Alter) | opBit(Operation::Drop) | opBit(Operation::Describe);
        break;
    case ResourceType::View:
        allowed = opBit(Operation::Read) | opBit(Operation::Create) | opBit(Operation::Alter) |
                  opBit(Operation::Drop) | opBit(Operation::Describe);
        break;
    case ResourceType::Group:
        allowed = opBit(Operation::Read) | opBit(Operation::Describe);
        break;
    case ResourceType::TransactionalId:
        allowed = opBit(Operation::Write) | opBit(Operation::Describe);
        break;
    }
    if (!(allowed & opBit(g.op))) return false;
    // I6: internal objects ("__…": __security, __catalog, __consumer_offsets, …) are never grantable by pattern;
    // and a pattern is bounded like the names it matches (the protocol's kSecurityPatternMax).
    if (p.type != ResourceType::Cluster && (p.name.starts_with("__") || p.name.size() > 249)) return false;
    if (p.type != ResourceType::Cluster && p.kind == PatternKind::Exact && p.name.empty()) return false;
    return true;
}

} // namespace rune::security
