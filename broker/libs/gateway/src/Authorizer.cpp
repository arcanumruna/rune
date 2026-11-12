// Authorizer.cpp - see rune/gateway/Authorizer.h.
#include <rune/gateway/Authorizer.h>
#include <rune/gateway/NsResolve.h>

#include <charconv>

namespace rune::gateway {

namespace rsec = rune::security;

namespace {
const char* opName(rsec::Operation o) noexcept {
    switch (o) {
        case rsec::Operation::Read:     return "READ";
        case rsec::Operation::Write:    return "WRITE";
        case rsec::Operation::Create:   return "CREATE";
        case rsec::Operation::Alter:    return "ALTER";
        case rsec::Operation::Drop:     return "DROP";
        case rsec::Operation::Describe: return "DESCRIBE";
        case rsec::Operation::Admin:    return "ADMIN";
        case rsec::Operation::Truncate: return "TRUNCATE";
        case rsec::Operation::Import:   return "IMPORT";
    }
    return "?";
}
const char* typeName(rsec::ResourceType t) noexcept {
    switch (t) {
        case rsec::ResourceType::Cluster:         return "CLUSTER";
        case rsec::ResourceType::Topic:           return "TOPIC";
        case rsec::ResourceType::View:            return "VIEW";
        case rsec::ResourceType::Group:           return "GROUP";
        case rsec::ResourceType::TransactionalId: return "TRANSACTIONAL_ID";
        case rsec::ResourceType::Namespace:       return "NAMESPACE";
    }
    return "?";
}
Denial deny(rsec::Operation op, rsec::ResourceType type, std::string_view what) {
    Denial d;
    d.append("not authorized: ");
    d.append(opName(op));
    d.append(" on ");
    d.append(typeName(type));
    if (type != rsec::ResourceType::Cluster) {
        d.append(" ");
        d.append(what.substr(0, kMaxNameInDetail));
        if (what.size() > kMaxNameInDetail) d.append("...");
    }
    return d;
}
/// The id as decimal text in a stack buffer (no std::to_string: a denied request allocates nothing).
struct IdText {
    std::array<char, 12> buf{};
    std::size_t n = 0;
    explicit IdText(int32_t id) noexcept {
        n = static_cast<std::size_t>(std::to_chars(buf.data(), buf.data() + buf.size(), id).ptr - buf.data());
    }
    [[nodiscard]] std::string_view view() const noexcept { return {buf.data(), n}; }
};
/// #640: the target's namespace is neither the table's own, nor the "already hidden" marker, and the table is not root.
bool foreignNs(const PermissionTable& t, uint32_t nsId) noexcept {
    return !t.isRoot() && nsId != kHiddenNsId && nsId != t.nsId();
}
} // namespace

std::optional<Denial> authorizeTopicId(const PermissionTable& t, int32_t topicId, rsec::Operation op,
                                       const ObjectIndex& index, rsec::ResourceType shownType)
{
    if (op == rsec::Operation::Import) return deny(op, shownType, IdText(topicId).view());   // opBit(Import) is 0: IMPORT is allowsImport
    if (t.allows(topicId, rsec::opBit(op))) return std::nullopt;
    if (index.at(topicId) == nullptr && t.allowsCluster(rsec::opBit(rsec::Operation::Describe))) return std::nullopt;   // D22
    return deny(op, shownType, IdText(topicId).view());
}

std::optional<Denial> authorizeViewSource(const PermissionTable& t, int32_t topicId)
{
    if (t.allows(topicId, rsec::opBit(rsec::Operation::Read))) return std::nullopt;
    return deny(rsec::Operation::Read, rsec::ResourceType::Topic, IdText(topicId).view());
}

std::optional<Denial> authorizeName(const PermissionTable& t, rsec::ResourceType type, std::string_view name, rsec::Operation op,
                                  uint32_t nsId)
{
    if (op == rsec::Operation::Import || foreignNs(t, nsId)) return deny(op, type, name);   // #640; IMPORT: see authorizeTopicId
    if (t.allowsName(type, name, rsec::opBit(op))) return std::nullopt;
    return deny(op, type, name);
}

std::optional<Denial> authorizeTopicOrViewName(const PermissionTable& t, std::string_view name, rsec::Operation op,
                                               const ObjectIndex& index, uint32_t nsId)
{
    if (op == rsec::Operation::Import || foreignNs(t, nsId)) return deny(op, rsec::ResourceType::Topic, name);   // #640; IMPORT: see authorizeTopicId
    if (const auto id = index.byName(nsId, name)) {   // #128: only the object's own kind's grants count
        const ObjectEntry* e = index.at(*id);
        if (t.allowsName(e->type, name, rsec::opBit(op))) return std::nullopt;
        return deny(op, rsec::ResourceType::Topic, name);
    }
    if (t.allowsName(rsec::ResourceType::Topic, name, rsec::opBit(op)) ||
        t.allowsName(rsec::ResourceType::View, name, rsec::opBit(op)))
        return std::nullopt;
    return deny(op, rsec::ResourceType::Topic, name);
}

std::optional<Denial> authorizeCluster(const PermissionTable& t, rsec::Operation op)
{
    if (op == rsec::Operation::Import) return deny(op, rsec::ResourceType::Cluster, {});   // see authorizeTopicId
    if (t.allowsCluster(rsec::opBit(op))) return std::nullopt;
    return deny(op, rsec::ResourceType::Cluster, {});
}

std::optional<Denial> authorizeAnyGrant(const PermissionTable& t, rsec::ResourceType type, rsec::Operation op)
{
    if (op != rsec::Operation::Import && t.hasAnyGrant(type, op)) return std::nullopt;   // see authorizeTopicId
    Denial d;
    d.append("not authorized: ");
    d.append(opName(op));
    d.append(" on any ");
    d.append(typeName(type));
    return d;
}

} // namespace rune::gateway
